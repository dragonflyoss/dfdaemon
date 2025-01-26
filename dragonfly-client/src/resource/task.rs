/*
 *     Copyright 2024 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use crate::grpc::{scheduler::SchedulerClient, REQUEST_TIMEOUT};
use crate::metrics::{
    collect_backend_request_failure_metrics, collect_backend_request_finished_metrics,
    collect_backend_request_started_metrics,
};
use dragonfly_api::common::v2::{
    Download, Hdfs, ObjectStorage, Peer, Piece, Range, Task as CommonTask, TrafficType,
};
use dragonfly_api::dfdaemon::{
    self,
    v2::{download_task_response, DownloadTaskResponse},
};
use dragonfly_api::errordetails::v2::{Backend, Unknown};
use dragonfly_api::scheduler::v2::{
    announce_peer_request, announce_peer_response, download_piece_back_to_source_failed_request,
    AnnouncePeerRequest, DeleteTaskRequest, DownloadPeerBackToSourceFailedRequest,
    DownloadPeerBackToSourceFinishedRequest, DownloadPeerBackToSourceStartedRequest,
    DownloadPeerFailedRequest, DownloadPeerFinishedRequest, DownloadPeerStartedRequest,
    DownloadPieceBackToSourceFailedRequest, DownloadPieceBackToSourceFinishedRequest,
    DownloadPieceFailedRequest, DownloadPieceFinishedRequest, RegisterPeerRequest,
    ReschedulePeerRequest, StatTaskRequest,
};
use dragonfly_client_backend::{BackendFactory, HeadRequest};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{BackendError, DownloadFromParentFailed, ErrorType, OrErr},
    Error, Result as ClientResult,
};
use dragonfly_client_storage::{metadata, Storage};
use dragonfly_client_util::{
    http::{hashmap_to_headermap, headermap_to_hashmap},
    id_generator::IDGenerator,
};
use reqwest::header::HeaderMap;
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Instant;
use tokio::io::AsyncReadExt;
use tokio::sync::{
    mpsc::{self, Sender},
    Semaphore,
};
use tokio::task::JoinSet;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::{Request, Status};
use tracing::{debug, error, info, instrument, Instrument};

use super::*;

/// Task represents a task manager.
pub struct Task {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// id_generator is the id generator.
    pub id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// scheduler_client is the grpc client of the scheduler.
    pub scheduler_client: Arc<SchedulerClient>,

    /// backend_factory is the backend factory.
    pub backend_factory: Arc<BackendFactory>,

    /// piece is the piece manager.
    pub piece: Arc<piece::Piece>,
}

/// Task implements the task manager.
impl Task {
    /// new returns a new Task.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
        backend_factory: Arc<BackendFactory>,
    ) -> ClientResult<Self> {
        let piece = piece::Piece::new(
            config.clone(),
            id_generator.clone(),
            storage.clone(),
            backend_factory.clone(),
        )?;
        let piece = Arc::new(piece);

        Ok(Self {
            config,
            id_generator,
            storage: storage.clone(),
            scheduler_client: scheduler_client.clone(),
            backend_factory: backend_factory.clone(),
            piece: piece.clone(),
        })
    }

    /// get gets the metadata of the task.
    pub async fn get(&self, id: &str) -> ClientResult<Option<metadata::Task>> {
        self.storage.get_task(id)
    }

    /// download_started updates the metadata of the task when the task downloads started.
    #[instrument(skip_all)]
    pub async fn download_started(
        &self,
        id: &str,
        request: Download,
    ) -> ClientResult<metadata::Task> {
        let task = self.storage.download_task_started(id, None, None, None)?;
        if task.content_length.is_some() && task.piece_length.is_some() {
            return Ok(task);
        }

        // Handle the request header.
        let mut request_header =
            hashmap_to_headermap(&request.request_header).inspect_err(|err| {
                error!("convert header: {}", err);
            })?;

        // Remove the range header to prevent the server from
        // returning a 206 partial content and returning
        // a 200 full content.
        request_header.remove(reqwest::header::RANGE);

        // Head the url to get the content length.
        let backend = self.backend_factory.build(request.url.as_str())?;

        // Record the start time.
        let start_time = Instant::now();

        // Collect the backend request started metrics.
        collect_backend_request_started_metrics(
            backend.scheme().as_str(),
            http::Method::HEAD.as_str(),
        );
        let response = backend
            .head(HeadRequest {
                task_id: id.to_string(),
                url: request.url,
                http_header: Some(request_header),
                timeout: self.config.download.piece_timeout,
                client_cert: None,
                object_storage: request.object_storage,
                hdfs: request.hdfs,
            })
            .await
            .inspect_err(|_err| {
                // Collect the backend request failure metrics.
                collect_backend_request_failure_metrics(
                    backend.scheme().as_str(),
                    http::Method::HEAD.as_str(),
                );
            })?;

        // Check if the status code is success.
        if !response.success {
            // Collect the backend request failure metrics.
            collect_backend_request_failure_metrics(
                backend.scheme().as_str(),
                http::Method::HEAD.as_str(),
            );

            return Err(Error::BackendError(Box::new(BackendError {
                message: response.error_message.unwrap_or_default(),
                status_code: Some(response.http_status_code.unwrap_or_default()),
                header: Some(response.http_header.unwrap_or_default()),
            })));
        }

        // Collect the backend request finished metrics.
        collect_backend_request_finished_metrics(
            backend.scheme().as_str(),
            http::Method::HEAD.as_str(),
            start_time.elapsed(),
        );

        let content_length = match response.content_length {
            Some(content_length) => content_length,
            None => return Err(Error::InvalidContentLength),
        };

        let piece_length = self.piece.calculate_piece_length(
            piece::PieceLengthStrategy::OptimizeByFileLength,
            content_length,
        );

        self.storage.download_task_started(
            id,
            Some(piece_length),
            Some(content_length),
            response.http_header,
        )
    }

    /// download_finished updates the metadata of the task when the task downloads finished.
    #[instrument(skip_all)]
    pub fn download_finished(&self, id: &str) -> ClientResult<metadata::Task> {
        self.storage.download_task_finished(id)
    }

    /// download_failed updates the metadata of the task when the task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_failed(&self, id: &str) -> ClientResult<()> {
        self.storage.download_task_failed(id).await.map(|_| ())
    }

    /// prefetch_task_started updates the metadata of the task when the task prefetch started.
    #[instrument(skip_all)]
    pub async fn prefetch_task_started(&self, id: &str) -> ClientResult<metadata::Task> {
        self.storage.prefetch_task_started(id).await
    }

    /// prefetch_task_failed updates the metadata of the task when the task prefetch failed.
    #[instrument(skip_all)]
    pub async fn prefetch_task_failed(&self, id: &str) -> ClientResult<metadata::Task> {
        self.storage.prefetch_task_failed(id).await
    }

    /// hard_link_or_copy hard links or copies the task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_or_copy(
        &self,
        task: &metadata::Task,
        to: &Path,
        range: Option<Range>,
    ) -> ClientResult<()> {
        self.storage.hard_link_or_copy_task(task, to, range).await
    }

    /// download downloads a task.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub async fn download(
        &self,
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        mut request: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<()> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Get the content length from the task.
        let Some(content_length) = task.content_length() else {
            error!("content length not found");
            return Err(Error::InvalidContentLength);
        };

        // Get the piece length from the task.
        let Some(piece_length) = task.piece_length() else {
            error!("piece length not found");
            return Err(Error::InvalidPieceLength);
        };

        // Add the piece length to the request for register task.
        request.piece_length = Some(piece_length);

        // Calculate the interested pieces to download.
        let interested_pieces =
            match self
                .piece
                .calculate_interested(piece_length, content_length, request.range)
            {
                Ok(interested_pieces) => interested_pieces,
                Err(err) => {
                    error!("calculate interested pieces error: {:?}", err);
                    return Err(err);
                }
            };
        debug!(
            "interested pieces: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Construct the pieces for the download task started response.
        let mut pieces = Vec::new();
        for interested_piece in interested_pieces.clone() {
            pieces.push(Piece {
                number: interested_piece.number,
                parent_id: interested_piece.parent_id.clone(),
                offset: interested_piece.offset,
                length: interested_piece.length,
                digest: interested_piece.digest.clone(),
                content: None,
                traffic_type: None,
                cost: interested_piece.prost_cost(),
                created_at: Some(prost_wkt_types::Timestamp::from(
                    interested_piece.created_at,
                )),
            });
        }

        // Send the download task started request.
        download_progress_tx
            .send_timeout(
                Ok(DownloadTaskResponse {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    response: Some(
                        download_task_response::Response::DownloadTaskStartedResponse(
                            dfdaemon::v2::DownloadTaskStartedResponse {
                                content_length,
                                range: request.range,
                                response_header: task.response_header.clone(),
                                pieces,
                            },
                        ),
                    ),
                }),
                REQUEST_TIMEOUT,
            )
            .await
            .inspect_err(|err| {
                error!("send DownloadTaskStartedResponse failed: {:?}", err);
            })?;

        // Download the pieces from the local.
        debug!("download the pieces from local");
        let finished_pieces = match self
            .download_partial_from_local(
                task,
                host_id,
                peer_id,
                request.need_piece_content,
                request.load_to_cache,
                interested_pieces.clone(),
                download_progress_tx.clone(),
            )
            .await
        {
            Ok(finished_pieces) => finished_pieces,
            Err(err) => {
                error!("download from local error: {:?}", err);
                return Err(err);
            }
        };

        // Remove the finished pieces from the pieces.
        let interested_pieces = self
            .piece
            .remove_finished_from_interested(finished_pieces, interested_pieces);
        info!(
            "interested pieces after removing the finished piece: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Check if all pieces are downloaded.
        if interested_pieces.is_empty() {
            info!("all pieces are downloaded from local");
            return Ok(());
        };
        debug!("download the pieces with scheduler");

        // Download the pieces with scheduler.
        let finished_pieces = match self
            .download_partial_with_scheduler(
                task,
                host_id,
                peer_id,
                interested_pieces.clone(),
                content_length,
                request.clone(),
                download_progress_tx.clone(),
            )
            .await
        {
            Ok(finished_pieces) => finished_pieces,
            Err(err) => {
                error!("download with scheduler error: {:?}", err);

                // If disable back-to-source is true, return an error directly.
                if request.disable_back_to_source {
                    error!(
                        "download back-to-source is disabled, download with scheduler error: {:?}",
                        err
                    );
                    return Err(Error::Unknown("download failed".to_string()));
                };

                // Download the pieces from the source.
                if let Err(err) = self
                    .download_partial_from_source(
                        task,
                        host_id,
                        peer_id,
                        interested_pieces.clone(),
                        request.clone(),
                        download_progress_tx.clone(),
                    )
                    .await
                {
                    error!("download from source error: {:?}", err);
                    return Err(err);
                }

                info!("all pieces are downloaded from source");
                return Ok(());
            }
        };

        // Remove the finished pieces from the pieces.
        let interested_pieces = self
            .piece
            .remove_finished_from_interested(finished_pieces, interested_pieces);
        info!(
            "interested pieces after removing the finished piece: {:?}",
            interested_pieces
                .iter()
                .map(|p| p.number)
                .collect::<Vec<u32>>()
        );

        // Check if all pieces are downloaded.
        if interested_pieces.is_empty() {
            info!("all pieces are downloaded with scheduler");
            return Ok(());
        };

        // If disable back-to-source is true, return an error directly.
        if request.disable_back_to_source {
            error!("download back-to-source is disabled");
            return Err(Error::Unknown("download failed".to_string()));
        };

        // Download the pieces from the source.
        if let Err(err) = self
            .download_partial_from_source(
                task,
                host_id,
                peer_id,
                interested_pieces.clone(),
                request.clone(),
                download_progress_tx.clone(),
            )
            .await
        {
            error!("download from source error: {:?}", err);
            return Err(err);
        }

        info!("all pieces are downloaded from source");
        Ok(())
    }

    /// download_partial_with_scheduler downloads a partial task with scheduler.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_with_scheduler(
        &self,
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        content_length: u64,
        request: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Initialize the schedule count.
        let mut schedule_count = 0;

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Initialize stream channel.
        let (in_stream_tx, in_stream_rx) = mpsc::channel(10 * 1024);

        // Send the register peer request.
        in_stream_tx
            .send_timeout(
                AnnouncePeerRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    peer_id: peer_id.to_string(),
                    request: Some(announce_peer_request::Request::RegisterPeerRequest(
                        RegisterPeerRequest {
                            download: Some(request.clone()),
                        },
                    )),
                },
                REQUEST_TIMEOUT,
            )
            .await
            .inspect_err(|err| {
                error!("send RegisterPeerRequest failed: {:?}", err);
            })?;
        info!("sent RegisterPeerRequest");

        // Initialize the stream.
        let in_stream = ReceiverStream::new(in_stream_rx);
        let request_stream = Request::new(in_stream);
        let response = self
            .scheduler_client
            .announce_peer(task_id, peer_id, request_stream)
            .await
            .inspect_err(|err| {
                error!("announce peer failed: {:?}", err);
            })?;
        info!("announced peer has been connected");

        let out_stream = response
            .into_inner()
            .timeout(self.config.scheduler.schedule_timeout);
        tokio::pin!(out_stream);

        while let Some(message) = out_stream.try_next().await? {
            // Check if the schedule count is exceeded.
            schedule_count += 1;
            if schedule_count >= self.config.scheduler.max_schedule_count {
                in_stream_tx
                    .send_timeout(
                        AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_peer_request::Request::DownloadPeerFailedRequest(
                                    DownloadPeerFailedRequest {
                                        description: Some(
                                            "max schedule count exceeded".to_string(),
                                        ),
                                    },
                                ),
                            ),
                        },
                        REQUEST_TIMEOUT,
                    )
                    .await
                    .unwrap_or_else(|err| {
                        error!("send DownloadPeerFailedRequest failed: {:?}", err)
                    });
                info!("sent DownloadPeerFailedRequest");

                // Wait for the latest message to be sent.
                in_stream_tx.closed().await;
                return Ok(finished_pieces);
            }

            let response = message?.response.ok_or(Error::UnexpectedResponse)?;
            match response {
                announce_peer_response::Response::EmptyTaskResponse(response) => {
                    // If the task is empty, return an empty vector.
                    info!("empty task response: {:?}", response);

                    // Send the download peer started request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPeerStartedRequest(
                                        DownloadPeerStartedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("send DownloadPeerStartedRequest failed: {:?}", err);
                        })?;
                    info!("sent DownloadPeerStartedRequest");

                    // Send the download peer finished request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPeerFinishedRequest(
                                        DownloadPeerFinishedRequest {
                                            content_length: 0,
                                            piece_count: 0,
                                        },
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("send DownloadPeerFinishedRequest failed: {:?}", err);
                        })?;
                    info!("sent DownloadPeerFinishedRequest");

                    // Wait for the latest message to be sent.
                    in_stream_tx.closed().await;
                    return Ok(Vec::new());
                }
                announce_peer_response::Response::NormalTaskResponse(response) => {
                    // If the task is normal, download the pieces from the parent.
                    info!(
                        "normal task response: {:?}",
                        response
                            .candidate_parents
                            .iter()
                            .map(|p| p.id.clone())
                            .collect::<Vec<String>>()
                    );

                    // Send the download peer started request.
                    match in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPeerStartedRequest(
                                        DownloadPeerStartedRequest {},
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                    {
                        Ok(_) => info!("sent DownloadPeerStartedRequest"),
                        Err(err) => {
                            error!("send DownloadPeerStartedRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };

                    // Remove the finished pieces from the pieces.
                    let remaining_interested_pieces = self.piece.remove_finished_from_interested(
                        finished_pieces.clone(),
                        interested_pieces.clone(),
                    );

                    // Download the pieces from the parent.
                    let partial_finished_pieces = match self
                        .download_partial_with_scheduler_from_parent(
                            task,
                            host_id,
                            peer_id,
                            response.candidate_parents.clone(),
                            remaining_interested_pieces.clone(),
                            request.is_prefetch,
                            request.need_piece_content,
                            request.load_to_cache,
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await
                    {
                        Ok(partial_finished_pieces) => {
                            info!(
                                "schedule {} finished {} pieces from parent",
                                schedule_count,
                                partial_finished_pieces.len()
                            );

                            partial_finished_pieces
                        }
                        Err(err) => {
                            error!("download from parent error: {:?}", err);
                            Vec::new()
                        }
                    };

                    // Merge the finished pieces.
                    finished_pieces = self.piece.merge_finished_pieces(
                        finished_pieces.clone(),
                        partial_finished_pieces.clone(),
                    );

                    // Check if all pieces are downloaded.
                    if finished_pieces.len() == interested_pieces.len() {
                        // Send the download peer finished request.
                        match in_stream_tx
                            .send_timeout(
                                AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_peer_request::Request::DownloadPeerFinishedRequest(
                                            DownloadPeerFinishedRequest {
                                                content_length,
                                                piece_count: interested_pieces.len() as u32,
                                            },
                                        ),
                                    ),
                                },
                                REQUEST_TIMEOUT,
                            )
                            .await
                        {
                            Ok(_) => info!("sent DownloadPeerFinishedRequest"),
                            Err(err) => {
                                error!("send DownloadPeerFinishedRequest failed: {:?}", err);
                            }
                        }

                        // Wait for the latest message to be sent.
                        in_stream_tx.closed().await;
                        return Ok(finished_pieces);
                    }

                    // If not all pieces are downloaded, send the reschedule request.
                    match in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::ReschedulePeerRequest(
                                        ReschedulePeerRequest {
                                            candidate_parents: response.candidate_parents,
                                            description: Some(
                                                "not all pieces are downloaded from parent"
                                                    .to_string(),
                                            ),
                                        },
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                    {
                        Ok(_) => info!("sent ReschedulePeerRequest"),
                        Err(err) => {
                            error!("send ReschedulePeerRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };
                }
                announce_peer_response::Response::NeedBackToSourceResponse(response) => {
                    // If the task need back to source, download the pieces from the source.
                    info!("need back to source response: {:?}", response);

                    // Send the download peer back-to-source request.
                    match in_stream_tx
                        .send_timeout(AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_peer_request::Request::DownloadPeerBackToSourceStartedRequest(
                                    DownloadPeerBackToSourceStartedRequest {
                                        description: None,
                                    },
                                ),
                            ),
                        }, REQUEST_TIMEOUT)
                    .await {
                        Ok(_) => info!("sent DownloadPeerBackToSourceStartedRequest"),
                        Err(err) => {
                            error!("send DownloadPeerBackToSourceStartedRequest failed: {:?}", err);
                            return Ok(finished_pieces);
                        }
                    };

                    // Remove the finished pieces from the pieces.
                    let remaining_interested_pieces = self.piece.remove_finished_from_interested(
                        finished_pieces.clone(),
                        interested_pieces.clone(),
                    );

                    // Download the pieces from the source.
                    let partial_finished_pieces = match self
                        .download_partial_with_scheduler_from_source(
                            task,
                            host_id,
                            peer_id,
                            remaining_interested_pieces.clone(),
                            request.clone(),
                            download_progress_tx.clone(),
                            in_stream_tx.clone(),
                        )
                        .await
                    {
                        Ok(finished_pieces) => finished_pieces,
                        Err(err) => {
                            in_stream_tx
                                .send_timeout(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_peer_request::Request::DownloadPeerBackToSourceFailedRequest(
                                            DownloadPeerBackToSourceFailedRequest {
                                                description: Some(err.to_string()),
                                            },
                                        ),
                                    ),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| {
                                    error!("send DownloadPeerBackToSourceFailedRequest failed: {:?}", err)
                                });
                            info!("sent DownloadPeerBackToSourceFailedRequest");

                            // Wait for the latest message to be sent.
                            in_stream_tx.closed().await;
                            return Ok(finished_pieces);
                        }
                    };

                    // Merge the finished pieces.
                    finished_pieces = self.piece.merge_finished_pieces(
                        finished_pieces.clone(),
                        partial_finished_pieces.clone(),
                    );

                    if finished_pieces.len() == remaining_interested_pieces.len() {
                        // Send the download peer finished request.
                        match in_stream_tx
                            .send_timeout(
                                AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(
                                        announce_peer_request::Request::DownloadPeerBackToSourceFinishedRequest(
                                            DownloadPeerBackToSourceFinishedRequest {
                                                content_length,
                                                piece_count: interested_pieces.len() as u32,
                                            },
                                        ),
                                    ),
                                },
                                REQUEST_TIMEOUT,
                            )
                            .await
                        {
                            Ok(_) => info!("sent DownloadPeerBackToSourceFinishedRequest"),
                            Err(err) => {
                                error!("send DownloadPeerBackToSourceFinishedRequest failed: {:?}", err);
                            }
                        }

                        // Wait for the latest message to be sent.
                        in_stream_tx.closed().await;
                        return Ok(finished_pieces);
                    }

                    match in_stream_tx
                        .send_timeout(AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_peer_request::Request::DownloadPeerBackToSourceFailedRequest(
                                    DownloadPeerBackToSourceFailedRequest {
                                        description: Some("not all pieces are downloaded from source".to_string()),
                                    },
                                ),
                            ),
                        }, REQUEST_TIMEOUT)
                    .await {
                        Ok(_) => info!("sent DownloadPeerBackToSourceFailedRequest"),
                        Err(err) => {
                            error!("send DownloadPeerBackToSourceFailedRequest failed: {:?}", err);
                        }
                    }

                    // Wait for the latest message to be sent.
                    in_stream_tx.closed().await;
                    return Ok(finished_pieces);
                }
            }
        }

        // If the stream is finished abnormally, return an error.
        error!("stream is finished abnormally");
        Ok(finished_pieces)
    }

    /// download_partial_with_scheduler_from_parent downloads a partial task with scheduler from a parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_with_scheduler_from_parent(
        &self,
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        parents: Vec<Peer>,
        interested_pieces: Vec<metadata::Piece>,
        is_prefetch: bool,
        need_piece_content: bool,
        load_to_cache: bool,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Initialize the piece collector.
        let piece_collector = piece_collector::PieceCollector::new(
            self.config.clone(),
            host_id,
            task_id,
            interested_pieces.clone(),
            parents
                .into_iter()
                .map(|peer| piece_collector::CollectedParent {
                    id: peer.id,
                    host: peer.host,
                })
                .collect(),
        );
        let mut piece_collector_rx = piece_collector.run().await;

        // Initialize the interrupt. If download from parent failed with scheduler or download
        // progress, interrupt the collector and return the finished pieces.
        let interrupt = Arc::new(AtomicBool::new(false));

        // Initialize the finished pieces.
        let finished_pieces = Arc::new(Mutex::new(Vec::new()));

        // Initialize the join set.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));

        // Download the pieces from the parents.
        while let Some(collect_piece) = piece_collector_rx.recv().await {
            if interrupt.load(Ordering::SeqCst) {
                // If the interrupt is true, break the collector loop.
                debug!("interrupt the piece collector");
                drop(piece_collector_rx);
                break;
            }

            async fn download_from_parent(
                task_id: String,
                host_id: String,
                peer_id: String,
                number: u32,
                length: u64,
                parent: piece_collector::CollectedParent,
                piece_manager: Arc<piece::Piece>,
                semaphore: Arc<Semaphore>,
                download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
                in_stream_tx: Sender<AnnouncePeerRequest>,
                interrupt: Arc<AtomicBool>,
                finished_pieces: Arc<Mutex<Vec<metadata::Piece>>>,
                is_prefetch: bool,
                need_piece_content: bool,
                load_to_cache: bool,
            ) -> ClientResult<metadata::Piece> {
                // Limit the concurrent piece count.
                let _permit = semaphore.acquire().await.unwrap();

                let piece_id = piece_manager.id(task_id.as_str(), number);
                info!(
                    "start to download piece {} from parent {:?}",
                    piece_id,
                    parent.id.clone()
                );

                let metadata = piece_manager
                    .download_from_parent(
                        piece_id.as_str(),
                        host_id.as_str(),
                        task_id.as_str(),
                        number,
                        length,
                        parent.clone(),
                        is_prefetch,
                        load_to_cache,
                    )
                    .await
                    .map_err(|err| {
                        error!(
                            "download piece {} from parent {:?} error: {:?}",
                            piece_id,
                            parent.id.clone(),
                            err
                        );
                        Error::DownloadFromParentFailed(DownloadFromParentFailed {
                            piece_number: number,
                            parent_id: parent.id.clone(),
                        })
                    })?;

                // Construct the piece.
                let mut piece = Piece {
                    number: metadata.number,
                    parent_id: metadata.parent_id.clone(),
                    offset: metadata.offset,
                    length: metadata.length,
                    digest: metadata.digest.clone(),
                    content: None,
                    traffic_type: Some(TrafficType::RemotePeer as i32),
                    cost: metadata.prost_cost(),
                    created_at: Some(prost_wkt_types::Timestamp::from(metadata.created_at)),
                };

                // If need_piece_content is true, read the piece content from the local.
                if need_piece_content {
                    let mut reader = piece_manager
                        .download_from_local_into_async_read(
                            piece_id.as_str(),
                            task_id.as_str(),
                            metadata.length,
                            None,
                            true,
                            false,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("read piece {} failed: {:?}", piece_id, err);
                            interrupt.store(true, Ordering::SeqCst);
                        })?;

                    let mut content = vec![0; metadata.length as usize];
                    reader.read_exact(&mut content).await.inspect_err(|err| {
                        error!("read piece {} failed: {:?}", piece_id, err);
                        interrupt.store(true, Ordering::SeqCst);
                    })?;

                    piece.content = Some(content);
                }

                // Send the download piece finished request.
                in_stream_tx
                    .send_timeout(
                        AnnouncePeerRequest {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            request: Some(
                                announce_peer_request::Request::DownloadPieceFinishedRequest(
                                    DownloadPieceFinishedRequest {
                                        piece: Some(piece.clone()),
                                    },
                                ),
                            ),
                        },
                        REQUEST_TIMEOUT,
                    )
                    .await
                    .inspect_err(|err| {
                        error!(
                            "send DownloadPieceFinishedRequest for piece {} failed: {:?}",
                            piece_id, err
                        );
                        interrupt.store(true, Ordering::SeqCst);
                    })?;

                // Send the download progress.
                download_progress_tx
                    .send_timeout(
                        Ok(DownloadTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_task_response::Response::DownloadPieceFinishedResponse(
                                    dfdaemon::v2::DownloadPieceFinishedResponse {
                                        piece: Some(piece.clone()),
                                    },
                                ),
                            ),
                        }),
                        REQUEST_TIMEOUT,
                    )
                    .await
                    .inspect_err(|err| {
                        error!(
                            "send DownloadPieceFinishedResponse for piece {} failed: {:?}",
                            piece_id, err
                        );
                        interrupt.store(true, Ordering::SeqCst);
                    })?;

                info!(
                    "finished piece {} from parent {:?}",
                    piece_id, metadata.parent_id
                );

                let mut finished_pieces = finished_pieces.lock().unwrap();
                finished_pieces.push(metadata.clone());

                Ok(metadata)
            }

            join_set.spawn(
                download_from_parent(
                    task_id.to_string(),
                    host_id.to_string(),
                    peer_id.to_string(),
                    collect_piece.number,
                    collect_piece.length,
                    collect_piece.parent.clone(),
                    self.piece.clone(),
                    semaphore.clone(),
                    download_progress_tx.clone(),
                    in_stream_tx.clone(),
                    interrupt.clone(),
                    finished_pieces.clone(),
                    is_prefetch,
                    need_piece_content,
                    load_to_cache,
                )
                .in_current_span(),
            );
        }

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set
            .join_next()
            .await
            .transpose()
            .or_err(ErrorType::AsyncRuntimeError)?
        {
            match message {
                Ok(_) => {}
                Err(Error::DownloadFromParentFailed(err)) => {
                    let (piece_number, parent_id) = (err.piece_number, err.parent_id);

                    // Send the download piece failed request.
                    in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPieceFailedRequest(
                                        DownloadPieceFailedRequest {
                                            piece_number: Some(err.piece_number),
                                            parent_id,
                                            temporary: true,
                                        },
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await
                        .unwrap_or_else(|err| {
                            error!(
                                "send DownloadPieceFailedRequest for piece {} failed: {:?}",
                                self.piece.id(task_id, piece_number),
                                err
                            )
                        });

                    // If the download failed from the parent, continue to download the next
                    // piece and ignore the error.
                    continue;
                }
                Err(Error::SendTimeout) => {
                    join_set.detach_all();

                    // If the send timeout with scheduler or download progress, return the finished pieces.
                    // It will stop the download from the parent with scheduler
                    // and download from the source directly from middle.
                    let finished_pieces = finished_pieces.lock().unwrap().clone();
                    return Ok(finished_pieces);
                }
                Err(err) => {
                    error!("download from parent error: {:?}", err);

                    // If the unknown error occurred, continue to download the next piece and
                    // ignore the error.
                    continue;
                }
            }
        }

        let finished_pieces = finished_pieces.lock().unwrap().clone();
        Ok(finished_pieces)
    }

    /// download_partial_with_scheduler_from_source downloads a partial task with scheduler from the source.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_with_scheduler_from_source(
        &self,
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        request: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
        in_stream_tx: Sender<AnnouncePeerRequest>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Convert the header.
        let request_header: HeaderMap = (&request.request_header)
            .try_into()
            .or_err(ErrorType::ParseError)?;

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the piece from the local.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));
        for interested_piece in interested_pieces {
            async fn download_from_source(
                task_id: String,
                host_id: String,
                peer_id: String,
                number: u32,
                url: String,
                offset: u64,
                length: u64,
                request_header: HeaderMap,
                is_prefetch: bool,
                need_piece_content: bool,
                load_to_cache: bool,
                piece_manager: Arc<piece::Piece>,
                semaphore: Arc<Semaphore>,
                download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
                in_stream_tx: Sender<AnnouncePeerRequest>,
                object_storage: Option<ObjectStorage>,
                hdfs: Option<Hdfs>,
            ) -> ClientResult<metadata::Piece> {
                // Limit the concurrent download count.
                let _permit = semaphore.acquire().await.unwrap();

                let piece_id = piece_manager.id(task_id.as_str(), number);
                info!("start to download piece {} from source", piece_id);

                let metadata = piece_manager
                    .download_from_source(
                        piece_id.as_str(),
                        task_id.as_str(),
                        number,
                        url.as_str(),
                        offset,
                        length,
                        request_header,
                        is_prefetch,
                        load_to_cache,
                        object_storage,
                        hdfs,
                    )
                    .await?;

                // Construct the piece.
                let mut piece = Piece {
                    number: metadata.number,
                    parent_id: metadata.parent_id.clone(),
                    offset: metadata.offset,
                    length: metadata.length,
                    digest: metadata.digest.clone(),
                    content: None,
                    traffic_type: Some(TrafficType::BackToSource as i32),
                    cost: metadata.prost_cost(),
                    created_at: Some(prost_wkt_types::Timestamp::from(metadata.created_at)),
                };

                // If need_piece_content is true, read the piece content from the local.
                if need_piece_content {
                    let mut reader = piece_manager
                        .download_from_local_into_async_read(
                            piece_id.as_str(),
                            task_id.as_str(),
                            metadata.length,
                            None,
                            true,
                            false,
                        )
                        .await
                        .inspect_err(|err| {
                            error!("read piece {} failed: {:?}", piece_id, err);
                        })?;

                    let mut content = vec![0; metadata.length as usize];
                    reader.read_exact(&mut content).await.inspect_err(|err| {
                        error!("read piece {} failed: {:?}", piece_id, err);
                    })?;

                    piece.content = Some(content);
                }

                // Send the download piece finished request.
                in_stream_tx
                        .send_timeout(
                            AnnouncePeerRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                request: Some(
                                    announce_peer_request::Request::DownloadPieceBackToSourceFinishedRequest(
                                        DownloadPieceBackToSourceFinishedRequest {
                                            piece: Some(piece.clone()),
                                        },
                                    ),
                                ),
                            },
                            REQUEST_TIMEOUT,
                        )
                        .await.inspect_err(|err| {
                            error!("send DownloadPieceBackToSourceFinishedRequest for piece {} failed: {:?}", piece_id, err);
                        })?;

                // Send the download progress.
                download_progress_tx
                    .send_timeout(
                        Ok(DownloadTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_task_response::Response::DownloadPieceFinishedResponse(
                                    dfdaemon::v2::DownloadPieceFinishedResponse {
                                        piece: Some(piece.clone()),
                                    },
                                ),
                            ),
                        }),
                        REQUEST_TIMEOUT,
                    )
                    .await
                    .inspect_err(|err| {
                        error!(
                            "send DownloadPieceFinishedResponse for piece {} failed: {:?}",
                            piece_id, err
                        );
                    })?;

                info!("finished piece {} from source", piece_id);
                Ok(metadata)
            }

            join_set.spawn(
                download_from_source(
                    task_id.to_string(),
                    host_id.to_string(),
                    peer_id.to_string(),
                    interested_piece.number,
                    request.url.clone(),
                    interested_piece.offset,
                    interested_piece.length,
                    request_header.clone(),
                    request.is_prefetch,
                    request.need_piece_content,
                    request.load_to_cache,
                    self.piece.clone(),
                    semaphore.clone(),
                    download_progress_tx.clone(),
                    in_stream_tx.clone(),
                    request.object_storage.clone(),
                    request.hdfs.clone(),
                )
                .in_current_span(),
            );
        }

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set
            .join_next()
            .await
            .transpose()
            .or_err(ErrorType::AsyncRuntimeError)?
        {
            match message {
                Ok(metadata) => {
                    // Store the finished piece.
                    finished_pieces.push(metadata.clone());
                }
                Err(Error::BackendError(err)) => {
                    join_set.detach_all();

                    // Send the download piece http failed request.
                    in_stream_tx.send_timeout(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: None,
                                                response: Some(download_piece_back_to_source_failed_request::Response::Backend(
                                                        Backend{
                                                            message: err.message.clone(),
                                                            header: headermap_to_hashmap(&err.header.clone().unwrap_or_default()),
                                                            status_code: err.status_code.map(|code| code.as_u16() as i32),
                                                        }
                                                )),
                                            }
                                    )),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| error!("send DownloadPieceBackToSourceFailedRequest error: {:?}", err));

                    // If the backend error with source, return the error.
                    // It will stop the download from the source with scheduler
                    // and download from the source directly from beginning.
                    return Err(Error::BackendError(err));
                }
                Err(Error::SendTimeout) => {
                    join_set.detach_all();

                    // Send the download piece failed request.
                    in_stream_tx.send_timeout(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: None,
                                                response: Some(download_piece_back_to_source_failed_request::Response::Unknown(
                                                        Unknown{
                                                            message: Some("send timeout".to_string()),
                                                        }
                                                )),
                                            }
                                    )),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| error!("send DownloadPieceBackToSourceFailedRequest error: {:?}", err));

                    // If the send timeout with scheduler or download progress, return
                    // the finished pieces. It will stop the download from the source with
                    // scheduler and download from the source directly from middle.
                    return Ok(finished_pieces);
                }
                Err(err) => {
                    join_set.detach_all();

                    // Send the download piece failed request.
                    in_stream_tx.send_timeout(AnnouncePeerRequest {
                                    host_id: host_id.to_string(),
                                    task_id: task_id.to_string(),
                                    peer_id: peer_id.to_string(),
                                    request: Some(announce_peer_request::Request::DownloadPieceBackToSourceFailedRequest(
                                            DownloadPieceBackToSourceFailedRequest{
                                                piece_number: None,
                                                response: Some(download_piece_back_to_source_failed_request::Response::Unknown(
                                                        Unknown{
                                                            message: Some(err.to_string()),
                                                        }
                                                )),
                                            }
                                    )),
                                }, REQUEST_TIMEOUT)
                                .await
                                .unwrap_or_else(|err| error!("send DownloadPieceBackToSourceFailedRequest error: {:?}", err));

                    // If the unknown error, return the error.
                    // It will stop the download from the source with scheduler
                    // and download from the source directly from beginning.
                    return Err(err);
                }
            }
        }

        Ok(finished_pieces)
    }

    /// download_partial_from_local downloads a partial task from a local.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_from_local(
        &self,
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        need_piece_content: bool,
        load_to_cache: bool,
        interested_pieces: Vec<metadata::Piece>,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the piece from the local.
        for interested_piece in interested_pieces {
            let piece_id = self.piece.id(task_id, interested_piece.number);

            // Get the piece metadata from the local storage.
            let piece = match self.piece.get(piece_id.as_str()) {
                Ok(Some(piece)) => piece,
                Ok(None) => {
                    debug!("piece {} not found in local storage", piece_id);
                    continue;
                }
                Err(err) => {
                    error!("get piece {} from local storage error: {:?}", piece_id, err);
                    continue;
                }
            };

            // Fake the download from the local.
            self.piece.download_from_local(task_id, piece.length);
            info!("finished piece {} from local", piece_id,);

            // Construct the piece.
            let mut piece = Piece {
                number: piece.number,
                parent_id: None,
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest.clone(),
                content: None,
                traffic_type: Some(TrafficType::LocalPeer as i32),
                cost: piece.prost_cost(),
                created_at: Some(prost_wkt_types::Timestamp::from(piece.created_at)),
            };

            // If need_piece_content or load_to_cache is true, read the piece content from the local.
            if need_piece_content || load_to_cache {
                let mut reader = self
                    .piece
                    .download_from_local_into_async_read(
                        piece_id.as_str(),
                        task_id,
                        piece.length,
                        None,
                        true,
                        false,
                    )
                    .await
                    .inspect_err(|err| {
                        error!("read piece {} failed: {:?}", piece_id, err);
                    })?;

                let mut content = vec![0; piece.length as usize];
                reader.read_exact(&mut content).await.inspect_err(|err| {
                    error!("read piece {} failed: {:?}", piece_id, err);
                })?;

                if need_piece_content {
                    piece.content = Some(content.clone());
                }

                // Load piece content to cache.
                if load_to_cache {
                    self.storage
                        .load_piece_to_cache(
                            piece_id.as_str(),
                            &mut content.clone().as_slice(),
                            piece.length,
                        )
                        .await;
                }
            }

            // Send the download progress.
            download_progress_tx
                .send_timeout(
                    Ok(DownloadTaskResponse {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        peer_id: peer_id.to_string(),
                        response: Some(
                            download_task_response::Response::DownloadPieceFinishedResponse(
                                dfdaemon::v2::DownloadPieceFinishedResponse {
                                    piece: Some(piece.clone()),
                                },
                            ),
                        ),
                    }),
                    REQUEST_TIMEOUT,
                )
                .await
                .inspect_err(|err| {
                    error!(
                        "send DownloadPieceFinishedResponse for piece {} failed: {:?}",
                        piece_id, err
                    );
                })?;

            // Store the finished piece.
            finished_pieces.push(interested_piece.clone());
        }

        Ok(finished_pieces)
    }

    /// download_partial_from_source downloads a partial task from the source.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn download_partial_from_source(
        &self,
        task: &metadata::Task,
        host_id: &str,
        peer_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        request: Download,
        download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
    ) -> ClientResult<Vec<metadata::Piece>> {
        // Get the id of the task.
        let task_id = task.id.as_str();

        // Convert the header.
        let request_header: HeaderMap = (&request.request_header)
            .try_into()
            .or_err(ErrorType::ParseError)?;

        // Initialize the finished pieces.
        let mut finished_pieces: Vec<metadata::Piece> = Vec::new();

        // Download the pieces.
        let mut join_set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(
            self.config.download.concurrent_piece_count as usize,
        ));

        for interested_piece in &interested_pieces {
            async fn download_from_source(
                task_id: String,
                host_id: String,
                peer_id: String,
                number: u32,
                url: String,
                offset: u64,
                length: u64,
                request_header: HeaderMap,
                is_prefetch: bool,
                load_to_cache: bool,
                piece_manager: Arc<piece::Piece>,
                semaphore: Arc<Semaphore>,
                download_progress_tx: Sender<Result<DownloadTaskResponse, Status>>,
                object_storage: Option<ObjectStorage>,
                hdfs: Option<Hdfs>,
            ) -> ClientResult<metadata::Piece> {
                // Limit the concurrent download count.
                let _permit = semaphore.acquire().await.unwrap();

                let piece_id = piece_manager.id(task_id.as_str(), number);
                info!("start to download piece {} from source", piece_id);

                let metadata = piece_manager
                    .download_from_source(
                        piece_id.as_str(),
                        task_id.as_str(),
                        number,
                        url.as_str(),
                        offset,
                        length,
                        request_header,
                        is_prefetch,
                        load_to_cache,
                        object_storage,
                        hdfs,
                    )
                    .await?;

                // Construct the piece.
                let piece = Piece {
                    number: metadata.number,
                    parent_id: None,
                    offset: metadata.offset,
                    length: metadata.length,
                    digest: metadata.digest.clone(),
                    content: None,
                    traffic_type: Some(TrafficType::BackToSource as i32),
                    cost: metadata.prost_cost(),
                    created_at: Some(prost_wkt_types::Timestamp::from(metadata.created_at)),
                };

                // Send the download progress.
                download_progress_tx
                    .send_timeout(
                        Ok(DownloadTaskResponse {
                            host_id: host_id.to_string(),
                            task_id: task_id.to_string(),
                            peer_id: peer_id.to_string(),
                            response: Some(
                                download_task_response::Response::DownloadPieceFinishedResponse(
                                    dfdaemon::v2::DownloadPieceFinishedResponse {
                                        piece: Some(piece.clone()),
                                    },
                                ),
                            ),
                        }),
                        REQUEST_TIMEOUT,
                    )
                    .await
                    .inspect_err(|err| {
                        error!(
                            "send DownloadPieceFinishedResponse for piece {} failed: {:?}",
                            piece_id, err
                        );
                    })?;

                info!("finished piece {} from source", piece_id);
                Ok(metadata)
            }

            join_set.spawn(
                download_from_source(
                    task_id.to_string(),
                    host_id.to_string(),
                    peer_id.to_string(),
                    interested_piece.number,
                    request.url.clone(),
                    interested_piece.offset,
                    interested_piece.length,
                    request_header.clone(),
                    request.is_prefetch,
                    request.load_to_cache,
                    self.piece.clone(),
                    semaphore.clone(),
                    download_progress_tx.clone(),
                    request.object_storage.clone(),
                    request.hdfs.clone(),
                )
                .in_current_span(),
            );
        }

        // Wait for the pieces to be downloaded.
        while let Some(message) = join_set
            .join_next()
            .await
            .transpose()
            .or_err(ErrorType::AsyncRuntimeError)?
        {
            match message {
                Ok(metadata) => {
                    // Store the finished piece.
                    finished_pieces.push(metadata.clone());
                }
                Err(err) => {
                    join_set.detach_all();

                    // If the download failed from the source, return the error.
                    // It will stop the download from the source.
                    return Err(err);
                }
            }
        }

        // Check if all pieces are downloaded.
        if finished_pieces.len() != interested_pieces.len() {
            // If not all pieces are downloaded, return an error.
            return Err(Error::Unknown(
                "not all pieces are downloaded from source".to_string(),
            ));
        }

        return Ok(finished_pieces);
    }

    /// stat_task returns the task metadata.
    #[instrument(skip_all)]
    pub async fn stat(&self, task_id: &str, host_id: &str) -> ClientResult<CommonTask> {
        let task = self
            .scheduler_client
            .stat_task(StatTaskRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
            })
            .await
            .inspect_err(|err| {
                error!("stat task failed: {}", err);
            })?;

        Ok(task)
    }

    /// Delete a task and reclaim local storage.
    #[instrument(skip_all)]
    pub async fn delete(&self, task_id: &str, host_id: &str) -> ClientResult<()> {
        let task = self.storage.get_task(task_id).inspect_err(|err| {
            error!("get task {} from local storage error: {:?}", task_id, err);
        })?;

        match task {
            Some(task) => {
                self.storage.delete_task(task.id.as_str()).await;

                self.scheduler_client
                    .delete_task(DeleteTaskRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                    })
                    .await
                    .inspect_err(|err| {
                        error!("delete task {} failed from scheduler: {:?}", task_id, err);
                    })?;

                info!("delete task {} from local storage", task.id);
                Ok(())
            }
            None => {
                error!("delete_task task {} not found", task_id);
                Err(Error::TaskNotFound(task_id.to_owned()))
            }
        }
    }
}
