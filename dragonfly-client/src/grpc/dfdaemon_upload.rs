/*
 *     Copyright 2023 The Dragonfly Authors
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

use super::interceptor::TracingInterceptor;
use crate::metrics::{
    collect_delete_task_failure_metrics, collect_delete_task_started_metrics,
    collect_download_task_failure_metrics, collect_download_task_finished_metrics,
    collect_download_task_started_metrics, collect_stat_task_failure_metrics,
    collect_stat_task_started_metrics, collect_upload_piece_failure_metrics,
    collect_upload_piece_finished_metrics, collect_upload_piece_started_metrics,
};
use crate::resource::{persistent_cache_task, task};
use crate::shutdown;
use bytesize::ByteSize;
use dragonfly_api::common::v2::{
    Host, Network, PersistentCacheTask, Piece, Priority, Task, TaskType,
};
use dragonfly_api::dfdaemon::v2::{
    dfdaemon_upload_client::DfdaemonUploadClient as DfdaemonUploadGRPCClient,
    dfdaemon_upload_server::{DfdaemonUpload, DfdaemonUploadServer as DfdaemonUploadGRPCServer},
    DeletePersistentCacheTaskRequest, DeleteTaskRequest, DownloadPersistentCacheTaskRequest,
    DownloadPersistentCacheTaskResponse, DownloadPieceRequest, DownloadPieceResponse,
    DownloadTaskRequest, DownloadTaskResponse, StatPersistentCacheTaskRequest, StatTaskRequest,
    SyncHostRequest, SyncPiecesRequest, SyncPiecesResponse,
};
use dragonfly_api::errordetails::v2::Backend;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error as ClientError, Result as ClientResult,
};
use dragonfly_client_util::http::{get_range, hashmap_to_headermap, headermap_to_hashmap};
use pnet::datalink;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use sysinfo::Networks;
use tokio::io::AsyncReadExt;
use tokio::sync::mpsc;
use tokio::sync::Barrier;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::interceptor::InterceptedService;
use tonic::{
    transport::{Channel, Server},
    Code, Request, Response, Status,
};
use tracing::{debug, error, info, instrument, Instrument, Span};
use url::Url;

/// DfdaemonUploadServer is the grpc server of the upload.
pub struct DfdaemonUploadServer {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// addr is the address of the grpc server.
    addr: SocketAddr,

    /// service is the grpc service of the dfdaemon upload.
    service: DfdaemonUploadGRPCServer<DfdaemonUploadServerHandler>,

    /// shutdown is used to shutdown the grpc server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the grpc server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// DfdaemonUploadServer implements the grpc server of the upload.
impl DfdaemonUploadServer {
    /// new creates a new DfdaemonUploadServer.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        addr: SocketAddr,
        task: Arc<task::Task>,
        persistent_cache_task: Arc<persistent_cache_task::PersistentCacheTask>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        // Initialize the grpc service.
        let service = DfdaemonUploadGRPCServer::new(DfdaemonUploadServerHandler {
            socket_path: config.download.server.socket_path.clone(),
            task,
            persistent_cache_task,
        })
        .max_decoding_message_size(usize::MAX)
        .max_encoding_message_size(usize::MAX);

        Self {
            config,
            addr,
            service,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// run starts the upload server.
    #[instrument(skip_all)]
    pub async fn run(&mut self, grpc_server_started_barrier: Arc<Barrier>) -> ClientResult<()> {
        // Register the reflection service.
        let reflection = tonic_reflection::server::Builder::configure()
            .register_encoded_file_descriptor_set(dragonfly_api::FILE_DESCRIPTOR_SET)
            .build_v1()?;

        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Initialize health reporter.
        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

        // Set the serving status of the upload grpc server.
        health_reporter
            .set_serving::<DfdaemonUploadGRPCServer<DfdaemonUploadServerHandler>>()
            .await;

        // Start upload grpc server.
        let mut server_builder = Server::builder();
        if let Ok(Some(server_tls_config)) =
            self.config.upload.server.load_server_tls_config().await
        {
            server_builder = server_builder.tls_config(server_tls_config)?;
        }

        let server = server_builder
            .tcp_nodelay(true)
            .max_frame_size(super::MAX_FRAME_SIZE)
            .initial_connection_window_size(super::INITIAL_WINDOW_SIZE)
            .initial_stream_window_size(super::INITIAL_WINDOW_SIZE)
            .add_service(reflection.clone())
            .add_service(health_service)
            .add_service(self.service.clone())
            .serve_with_shutdown(self.addr, async move {
                // When the grpc server is started, notify the barrier. If the shutdown signal is received
                // before barrier is waited successfully, the server will shutdown immediately.
                tokio::select! {
                    // Notify the upload grpc server is started.
                    _ = grpc_server_started_barrier.wait() => {
                        info!("upload server is ready");
                    }
                    // Wait for shutdown signal.
                    _ = shutdown.recv() => {
                        info!("upload grpc server stop to wait");
                    }
                }

                // Wait for the shutdown signal to shutdown the upload grpc server,
                // when server is started.
                let _ = shutdown.recv().await;
                info!("upload grpc server shutting down");
            });

        // Wait for the upload grpc server to shutdown.
        info!("upload server listening on {}", self.addr);
        Ok(server.await?)
    }
}

/// DfdaemonUploadServerHandler is the handler of the dfdaemon upload grpc service.
pub struct DfdaemonUploadServerHandler {
    /// socket_path is the path of the unix domain socket.
    socket_path: PathBuf,

    /// task is the task manager.
    task: Arc<task::Task>,

    /// persistent_cache_task is the persistent cache task manager.
    persistent_cache_task: Arc<persistent_cache_task::PersistentCacheTask>,
}

/// DfdaemonUploadServerHandler implements the dfdaemon upload grpc service.
#[tonic::async_trait]
impl DfdaemonUpload for DfdaemonUploadServerHandler {
    /// DownloadTaskStream is the stream of the download task response.
    type DownloadTaskStream = ReceiverStream<Result<DownloadTaskResponse, Status>>;

    /// download_task downloads the task.
    #[instrument(skip_all, fields(host_id, task_id, peer_id))]
    async fn download_task(
        &self,
        request: Request<DownloadTaskRequest>,
    ) -> Result<Response<Self::DownloadTaskStream>, Status> {
        debug!("download task in upload server");

        // Record the start time.
        let start_time = Instant::now();

        // Clone the request.
        let request = request.into_inner();

        // Check whether the download is empty.
        let mut download = request.download.ok_or_else(|| {
            error!("missing download");
            Status::invalid_argument("missing download")
        })?;

        // Generate the task id.
        let task_id = self
            .task
            .id_generator
            .task_id(
                download.url.as_str(),
                download.tag.as_deref(),
                download.application.as_deref(),
                download.filtered_query_params.clone(),
            )
            .map_err(|e| {
                error!("generate task id: {}", e);
                Status::invalid_argument(e.to_string())
            })?;

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Generate the peer id.
        let peer_id = self.task.id_generator.peer_id();

        // Span record the host id, task id and peer id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("peer_id", peer_id.as_str());

        // Download task started.
        info!("download task started: {:?}", download);
        let task = match self
            .task
            .download_started(task_id.as_str(), download.clone())
            .await
        {
            Err(ClientError::BackendError(err)) => {
                error!("download started failed by error: {}", err);
                self.task
                    .download_failed(task_id.as_str())
                    .await
                    .unwrap_or_else(|err| error!("download task failed: {}", err));

                match serde_json::to_vec::<Backend>(&Backend {
                    message: err.message.clone(),
                    header: headermap_to_hashmap(&err.header.clone().unwrap_or_default()),
                    status_code: err.status_code.map(|code| code.as_u16() as i32),
                }) {
                    Ok(json) => {
                        return Err(Status::with_details(
                            Code::Internal,
                            err.to_string(),
                            json.into(),
                        ));
                    }
                    Err(err) => {
                        error!("serialize error: {}", err);
                        return Err(Status::internal(err.to_string()));
                    }
                }
            }
            Err(err) => {
                error!("download started failed: {}", err);
                return Err(Status::internal(err.to_string()));
            }
            Ok(task) => {
                // Collect download task started metrics.
                collect_download_task_started_metrics(
                    download.r#type,
                    download.tag.clone().unwrap_or_default().as_str(),
                    download.application.clone().unwrap_or_default().as_str(),
                    download.priority.to_string().as_str(),
                );

                task
            }
        };

        // Clone the task.
        let task_manager = self.task.clone();

        // Check whether the content length is empty.
        let Some(content_length) = task.content_length() else {
            // Download task failed.
            task_manager
                .download_failed(task_id.as_str())
                .await
                .unwrap_or_else(|err| error!("download task failed: {}", err));

            // Collect download task failure metrics.
            collect_download_task_failure_metrics(
                download.r#type,
                download.tag.clone().unwrap_or_default().as_str(),
                download.application.clone().unwrap_or_default().as_str(),
                download.priority.to_string().as_str(),
            );

            error!("missing content length in the response");
            return Err(Status::internal("missing content length in the response"));
        };

        info!(
            "content length {}, piece length {}",
            content_length,
            task.piece_length().unwrap_or_default()
        );

        // Download's range priority is higher than the request header's range.
        // If download protocol is http, use the range of the request header.
        // If download protocol is not http, use the range of the download.
        if download.range.is_none() {
            // Convert the header.
            let request_header = match hashmap_to_headermap(&download.request_header) {
                Ok(header) => header,
                Err(e) => {
                    // Download task failed.
                    task_manager
                        .download_failed(task_id.as_str())
                        .await
                        .unwrap_or_else(|err| error!("download task failed: {}", err));

                    // Collect download task failure metrics.
                    collect_download_task_failure_metrics(
                        download.r#type,
                        download.tag.clone().unwrap_or_default().as_str(),
                        download.application.clone().unwrap_or_default().as_str(),
                        download.priority.to_string().as_str(),
                    );

                    error!("convert header: {}", e);
                    return Err(Status::invalid_argument(e.to_string()));
                }
            };

            download.range = match get_range(&request_header, content_length) {
                Ok(range) => range,
                Err(e) => {
                    // Download task failed.
                    task_manager
                        .download_failed(task_id.as_str())
                        .await
                        .unwrap_or_else(|err| error!("download task failed: {}", err));

                    // Collect download task failure metrics.
                    collect_download_task_failure_metrics(
                        download.r#type,
                        download.tag.clone().unwrap_or_default().as_str(),
                        download.application.clone().unwrap_or_default().as_str(),
                        download.priority.to_string().as_str(),
                    );

                    error!("get range failed: {}", e);
                    return Err(Status::failed_precondition(e.to_string()));
                }
            };
        }

        // Initialize stream channel.
        let download_clone = download.clone();
        let task_manager_clone = task_manager.clone();
        let task_clone = task.clone();
        let (out_stream_tx, out_stream_rx) = mpsc::channel(10 * 1024);
        tokio::spawn(
            async move {
                match task_manager_clone
                    .download(
                        &task_clone,
                        host_id.as_str(),
                        peer_id.as_str(),
                        download_clone.clone(),
                        out_stream_tx.clone(),
                    )
                    .await
                {
                    Ok(_) => {
                        // Collect download task finished metrics.
                        collect_download_task_finished_metrics(
                            download_clone.r#type,
                            download_clone.tag.clone().unwrap_or_default().as_str(),
                            download_clone
                                .application
                                .clone()
                                .unwrap_or_default()
                                .as_str(),
                            download_clone.priority.to_string().as_str(),
                            task_clone.content_length().unwrap_or_default(),
                            download_clone.range,
                            start_time.elapsed(),
                        );

                        // Download task succeeded.
                        info!("download task succeeded");
                        if download_clone.range.is_none() {
                            if let Err(err) =
                                task_manager_clone.download_finished(task_clone.id.as_str())
                            {
                                error!("download task finished: {}", err);
                            }
                        }

                        // Check whether the output path is empty. If output path is empty,
                        // should not hard link or copy the task content to the destination.
                        if let Some(output_path) = download_clone.output_path.clone() {
                            // Hard link or copy the task content to the destination.
                            if let Err(err) = task_manager_clone
                                .hard_link_or_copy(
                                    &task_clone,
                                    Path::new(output_path.as_str()),
                                    download_clone.range,
                                )
                                .await
                            {
                                error!("hard link or copy task: {}", err);
                                out_stream_tx
                                    .send(Err(Status::internal(err.to_string())))
                                    .await
                                    .unwrap_or_else(|err| {
                                        error!("send download progress error: {:?}", err)
                                    });
                            };
                        }
                    }
                    Err(ClientError::BackendError(err)) => {
                        error!("download failed by error: {}", err);

                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            download_clone.r#type,
                            download_clone.tag.clone().unwrap_or_default().as_str(),
                            download_clone
                                .application
                                .clone()
                                .unwrap_or_default()
                                .as_str(),
                            download_clone.priority.to_string().as_str(),
                        );

                        task_manager_clone
                            .download_failed(task_clone.id.as_str())
                            .await
                            .unwrap_or_else(|err| error!("download task failed: {}", err));

                        match serde_json::to_vec::<Backend>(&Backend {
                            message: err.message.clone(),
                            header: headermap_to_hashmap(&err.header.clone().unwrap_or_default()),
                            status_code: err.status_code.map(|code| code.as_u16() as i32),
                        }) {
                            Ok(json) => {
                                out_stream_tx
                                    .send(Err(Status::with_details(
                                        Code::Internal,
                                        err.to_string(),
                                        json.into(),
                                    )))
                                    .await
                                    .unwrap_or_else(|err| {
                                        error!("send download progress error: {:?}", err)
                                    });
                            }
                            Err(err) => {
                                out_stream_tx
                                    .send(Err(Status::internal(err.to_string())))
                                    .await
                                    .unwrap_or_else(|err| {
                                        error!("send download progress error: {:?}", err)
                                    });
                                error!("serialize error: {}", err);
                            }
                        }
                    }
                    Err(err) => {
                        error!("download failed: {}", err);

                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            download_clone.r#type,
                            download_clone.tag.clone().unwrap_or_default().as_str(),
                            download_clone
                                .application
                                .clone()
                                .unwrap_or_default()
                                .as_str(),
                            download_clone.priority.to_string().as_str(),
                        );

                        // Download task failed.
                        task_manager_clone
                            .download_failed(task_clone.id.as_str())
                            .await
                            .unwrap_or_else(|err| error!("download task failed: {}", err));

                        out_stream_tx
                            .send(Err(Status::internal(err.to_string())))
                            .await
                            .unwrap_or_else(|err| {
                                error!("send download progress error: {:?}", err)
                            });
                    }
                }

                drop(out_stream_tx);
            }
            .in_current_span(),
        );

        // If prefetch flag is true, prefetch the full task.
        if download.prefetch {
            info!("try to prefetch task");
            match task_manager.prefetch_task_started(task_id.as_str()).await {
                Ok(_) => {
                    info!("prefetch task started");
                    let socket_path = self.socket_path.clone();
                    let task_manager_clone = task_manager.clone();
                    tokio::spawn(
                        async move {
                            if let Err(err) = super::prefetch_task(
                                socket_path.clone(),
                                Request::new(DownloadTaskRequest {
                                    download: Some(download.clone()),
                                }),
                            )
                            .await
                            {
                                match task_manager_clone
                                    .prefetch_task_failed(task_id.clone().as_str())
                                    .await
                                {
                                    Ok(_) => {
                                        error!("prefetch task failed: {}", err);
                                    }
                                    Err(err) => {
                                        error!(
                                            "prefetch succeeded, but failed to update metadata: {}",
                                            err
                                        );
                                    }
                                }
                            }
                        }
                        .in_current_span(),
                    );
                }
                // If the task is already prefetched, ignore the error.
                Err(ClientError::InvalidState(_)) => info!("task is already prefetched"),
                Err(err) => {
                    error!("prefetch task started: {}", err);
                }
            }
        }

        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    /// stat_task stats the task.
    #[instrument(skip_all, fields(host_id, task_id))]
    async fn stat_task(&self, request: Request<StatTaskRequest>) -> Result<Response<Task>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());

        // Collect the stat task metrics.
        collect_stat_task_started_metrics(TaskType::Standard as i32);

        // Get the task from the scheduler.
        let task = self
            .task
            .stat(task_id.as_str(), host_id.as_str())
            .await
            .map_err(|err| {
                // Collect the stat task failure metrics.
                collect_stat_task_failure_metrics(TaskType::Standard as i32);

                error!("stat task: {}", err);
                Status::internal(err.to_string())
            })?;

        Ok(Response::new(task))
    }

    /// delete_task deletes the task.
    #[instrument(skip_all, fields(host_id, task_id))]
    async fn delete_task(
        &self,
        request: Request<DeleteTaskRequest>,
    ) -> Result<Response<()>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());

        // Collect the delete task started metrics.
        collect_delete_task_started_metrics(TaskType::Standard as i32);

        // Delete the task from the scheduler.
        self.task
            .delete(task_id.as_str(), host_id.as_str())
            .await
            .map_err(|err| {
                // Collect the delete task failure metrics.
                collect_delete_task_failure_metrics(TaskType::Standard as i32);

                error!("delete task: {}", err);
                Status::internal(err.to_string())
            })?;

        Ok(Response::new(()))
    }

    /// SyncPiecesStream is the stream of the sync pieces response.
    type SyncPiecesStream = ReceiverStream<Result<SyncPiecesResponse, Status>>;

    /// sync_pieces provides the piece metadata for parent.
    #[instrument(skip_all, fields(host_id, remote_host_id, task_id))]
    async fn sync_pieces(
        &self,
        request: Request<SyncPiecesRequest>,
    ) -> Result<Response<Self::SyncPiecesStream>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the remote host id from the request.
        let remote_host_id = request.host_id;

        // Get the task id from tae request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.clone());
        Span::current().record("remote_host_id", remote_host_id.as_str());
        Span::current().record("task_id", task_id.clone());

        // Get the interested piece numbers from the request.
        let mut interested_piece_numbers = request.interested_piece_numbers.clone();

        // Clone the task.
        let task_manager = self.task.clone();

        // Initialize stream channel.
        let (out_stream_tx, out_stream_rx) = mpsc::channel(10 * 1024);
        tokio::spawn(
            async move {
                loop {
                    let mut has_started_piece = false;
                    let mut finished_piece_numbers = Vec::new();
                    for interested_piece_number in interested_piece_numbers.iter() {
                        let piece = match task_manager.piece.get(
                            task_manager
                                .piece
                                .id(task_id.as_str(), *interested_piece_number)
                                .as_str(),
                        ) {
                            Ok(Some(piece)) => piece,
                            Ok(None) => continue,
                            Err(err) => {
                                error!(
                                    "send piece metadata {}-{}: {}",
                                    task_id, interested_piece_number, err
                                );
                                out_stream_tx
                                    .send(Err(Status::internal(err.to_string())))
                                    .await
                                    .unwrap_or_else(|err| {
                                        error!(
                                            "send piece metadata {}-{} to stream: {}",
                                            task_id, interested_piece_number, err
                                        );
                                    });

                                drop(out_stream_tx);
                                return;
                            }
                        };

                        // Send the piece metadata to the stream.
                        if piece.is_finished() {
                            match out_stream_tx
                                .send(Ok(SyncPiecesResponse {
                                    number: piece.number,
                                    offset: piece.offset,
                                    length: piece.length,
                                }))
                                .await
                            {
                                Ok(_) => {
                                    info!("send piece metadata {}-{}", task_id, piece.number);
                                }
                                Err(err) => {
                                    error!(
                                        "send piece metadata {}-{} to stream: {}",
                                        task_id, interested_piece_number, err
                                    );

                                    drop(out_stream_tx);
                                    return;
                                }
                            }

                            // Add the finished piece number to the finished piece numbers.
                            finished_piece_numbers.push(piece.number);
                            continue;
                        }

                        // Check whether the piece is started.
                        if piece.is_started() {
                            has_started_piece = true;
                        }
                    }

                    // Remove the finished piece numbers from the interested piece numbers.
                    interested_piece_numbers
                        .retain(|number| !finished_piece_numbers.contains(number));

                    // If all the interested pieces are finished, return.
                    if interested_piece_numbers.is_empty() {
                        info!("all the interested pieces are finished");
                        drop(out_stream_tx);
                        return;
                    }

                    // If there is no started piece, return.
                    if !has_started_piece {
                        info!("there is no started piece");
                        drop(out_stream_tx);
                        return;
                    }

                    // Wait for the piece to be finished.
                    tokio::time::sleep(
                        dragonfly_client_storage::DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL,
                    )
                    .await;
                }
            }
            .in_current_span(),
        );

        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    /// download_piece provides the piece content for parent.
    #[instrument(skip_all, fields(host_id, remote_host_id, task_id, piece_id))]
    async fn download_piece(
        &self,
        request: Request<DownloadPieceRequest>,
    ) -> Result<Response<DownloadPieceResponse>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the remote host id from the request.
        let remote_host_id = request.host_id;

        // Get the task id from the request.
        let task_id = request.task_id;

        // Get the interested piece number from the request.
        let piece_number = request.piece_number;

        // Generate the piece id.
        let piece_id = self.task.piece.id(task_id.as_str(), piece_number);

        // Span record the host id, task id and piece number.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("remote_host_id", remote_host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("piece_id", piece_id.as_str());

        // Get the piece metadata from the local storage.
        let piece = self
            .task
            .piece
            .get(piece_id.as_str())
            .map_err(|err| {
                error!("upload piece metadata from local storage: {}", err);
                Status::internal(err.to_string())
            })?
            .ok_or_else(|| {
                error!("upload piece metadata not found");
                Status::not_found("piece metadata not found")
            })?;

        // Collect upload piece started metrics.
        collect_upload_piece_started_metrics();
        info!("start upload piece content");

        // Get the piece content from the local storage.
        let mut reader = self
            .task
            .piece
            .upload_from_local_into_async_read(
                piece_id.as_str(),
                task_id.as_str(),
                piece.length,
                None,
                false,
            )
            .await
            .map_err(|err| {
                // Collect upload piece failure metrics.
                collect_upload_piece_failure_metrics();

                error!("upload piece content from local storage: {}", err);
                Status::internal(err.to_string())
            })?;

        // Read the content of the piece.
        let mut content = vec![0; piece.length as usize];
        reader.read_exact(&mut content).await.map_err(|err| {
            // Collect upload piece failure metrics.
            collect_upload_piece_failure_metrics();

            error!("upload piece content failed: {}", err);
            Status::internal(err.to_string())
        })?;

        // Collect upload piece finished metrics.
        collect_upload_piece_finished_metrics();
        info!("finished upload piece content");

        // Return the piece.
        Ok(Response::new(DownloadPieceResponse {
            piece: Some(Piece {
                number: piece.number,
                parent_id: piece.parent_id,
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest,
                content: Some(content),
                traffic_type: None,
                cost: None,
                created_at: None,
            }),
        }))
    }

    /// SyncHostStream is the stream of the sync host response.
    type SyncHostStream = ReceiverStream<Result<Host, Status>>;

    /// sync_host syncs the host information.
    #[instrument(skip_all)]
    async fn sync_host(
        &self,
        request: Request<SyncHostRequest>,
    ) -> Result<Response<Self::SyncHostStream>, Status> {
        /// DEFAULT_INTERFACE_SPEED is the default speed for interfaces.
        const DEFAULT_INTERFACE_SPEED: ByteSize = ByteSize::mb(10000);
        /// FIRST_REFRESH_INTERVAL is the interval between the first refresh of the network.
        const FIRST_REFRESH_INTERVAL: Duration = Duration::from_millis(100);
        /// DEFAULT_REFRESH_INTERVAL is the default interval for refreshing the network.
        const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

        // Get request ip.
        let request_ip = request.remote_addr();

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the remote host id from the request.
        let remote_host_id = request.host_id;

        // Get the remote peer id from the request;
        let remote_peer_id = request.peer_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.clone());
        match request_ip {
            None => Span::current().record("request_ip", "None"),
            Some(ip) => Span::current().record("request_ip", ip.to_string().as_str()),
        };

        Span::current().record("remote_host_id", remote_host_id.as_str());
        Span::current().record("remote_peer_id", remote_peer_id.as_str());

        // Get request interface and speed.
        let mut request_interface = None;
        let mut request_interface_speed = DEFAULT_INTERFACE_SPEED.as_u64();
        if let Some(request_ip) = request_ip {
            // Get interface of this ip.
            let interfaces = datalink::interfaces();
            for net in interfaces.iter() {
                if net.ips.iter().any(|ip| ip.contains(request_ip.ip())) {
                    request_interface = Some(net.name.clone());
                    break;
                }
            }

            // Get speed of interface.
            if let Some(interface) = request_interface.clone() {
                let speed_path = format!("/sys/class/net/{}/speed", interface);
                let content = fs::read_to_string(speed_path).unwrap_or_default();
                if let Ok(speed) = content.trim().parse::<u64>() {
                    // Convert Mib/Sec to bit/Sec.
                    debug!(
                        "interface {} bandwidth is {}",
                        &interface,
                        ByteSize::mb(speed)
                    );
                    request_interface_speed = ByteSize::mb(speed).as_u64();
                }
            }
        }

        // Initialize stream channel.
        let (out_stream_tx, out_stream_rx) = mpsc::channel(1);
        // Start the host info update loop.
        let mut networks = Networks::new_with_refreshed_list();
        let mut last_refresh_time = SystemTime::now();
        tokio::time::sleep(FIRST_REFRESH_INTERVAL).await;

        info!("start sending host info to host {}", remote_host_id);
        loop {
            let mut host = Host::default();

            // Get network information by request ip
            let mut network = Network::default();

            // Refresh network information.
            networks.refresh();
            let new_time = SystemTime::now();
            let interval = new_time
                .duration_since(last_refresh_time)
                .unwrap()
                .as_millis() as u64;
            last_refresh_time = new_time;

            // Get interface available bandwidth.
            if let Some(request_interface) = request_interface.clone() {
                for (interface, data) in &networks {
                    if *interface == request_interface {
                        network.upload_rate =
                            (request_interface_speed - data.transmitted()) * 1000 / interval;
                        debug!(
                            "refresh interface {} available bandwidth to {}",
                            interface,
                            ByteSize(network.upload_rate)
                        );
                    }
                }
            }
            host.network = Some(network);

            match out_stream_tx.send(Ok(host.clone())).await {
                Ok(_) => {
                    debug!("sync host info to remote host {}", remote_host_id.as_str());
                }
                Err(err) => {
                    info!(
                        "connection broken from remote host {}, err: {}",
                        remote_host_id, err
                    );
                    drop(out_stream_tx);
                    break;
                }
            };

            // Wait for the piece to be finished.
            tokio::time::sleep(DEFAULT_REFRESH_INTERVAL).await;
        }
        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    /// DownloadPersistentCacheTaskStream is the stream of the download persistent cache task response.
    type DownloadPersistentCacheTaskStream =
        ReceiverStream<Result<DownloadPersistentCacheTaskResponse, Status>>;

    /// download_persistent_cache_task downloads the persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id, peer_id))]
    async fn download_persistent_cache_task(
        &self,
        request: Request<DownloadPersistentCacheTaskRequest>,
    ) -> Result<Response<Self::DownloadPersistentCacheTaskStream>, Status> {
        info!("download persistent cache task in download server");

        // Record the start time.
        let start_time = Instant::now();

        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Clone the task id.
        let task_id = request.task_id.clone();

        // Generate the peer id.
        let peer_id = self.task.id_generator.peer_id();

        // Generate the task type and task priority.
        let task_type = TaskType::PersistentCache as i32;
        let task_priority = Priority::Level0 as i32;

        // Span record the host id, task id and peer id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());
        Span::current().record("peer_id", peer_id.as_str());

        // Download task started.
        info!("download persistent cache task started: {:?}", request);
        let task = match self
            .persistent_cache_task
            .download_started(task_id.as_str(), host_id.as_str(), request.clone())
            .await
        {
            Err(ClientError::BackendError(err)) => {
                error!("download cache started failed by error: {}", err);
                self.persistent_cache_task
                    .download_failed(task_id.as_str())
                    .await
                    .unwrap_or_else(|err| error!("download persistent cache task failed: {}", err));

                match serde_json::to_vec::<Backend>(&Backend {
                    message: err.message.clone(),
                    header: headermap_to_hashmap(&err.header.clone().unwrap_or_default()),
                    status_code: err.status_code.map(|code| code.as_u16() as i32),
                }) {
                    Ok(json) => {
                        return Err(Status::with_details(
                            Code::Internal,
                            err.to_string(),
                            json.into(),
                        ));
                    }
                    Err(e) => {
                        error!("serialize error: {}", e);
                        return Err(Status::internal(e.to_string()));
                    }
                }
            }
            Err(err) => {
                error!("download started failed: {}", err);
                return Err(Status::internal(err.to_string()));
            }
            Ok(task) => {
                // Collect download persistent cache task started metrics.
                collect_download_task_started_metrics(
                    task_type,
                    request.tag.clone().unwrap_or_default().as_str(),
                    request.application.clone().unwrap_or_default().as_str(),
                    task_priority.to_string().as_str(),
                );

                task
            }
        };
        info!(
            "content length {}, piece length {}",
            task.content_length(),
            task.piece_length()
        );

        // Clone the persistent cache task.
        let task_manager = self.persistent_cache_task.clone();

        // Initialize stream channel.
        let request_clone = request.clone();
        let task_manager_clone = task_manager.clone();
        let task_clone = task.clone();
        let (out_stream_tx, out_stream_rx) = mpsc::channel(10 * 1024);
        tokio::spawn(
            async move {
                match task_manager_clone
                    .download(
                        &task_clone,
                        host_id.as_str(),
                        peer_id.as_str(),
                        request_clone.clone(),
                        out_stream_tx.clone(),
                    )
                    .await
                {
                    Ok(_) => {
                        // Collect download task finished metrics.
                        collect_download_task_finished_metrics(
                            task_type,
                            request_clone.tag.clone().unwrap_or_default().as_str(),
                            request_clone
                                .application
                                .clone()
                                .unwrap_or_default()
                                .as_str(),
                            task_priority.to_string().as_str(),
                            task_clone.content_length(),
                            None,
                            start_time.elapsed(),
                        );

                        // Download persistent cache task succeeded.
                        info!("download persistent cache task succeeded");

                        // Hard link or copy the persistent cache task content to the destination.
                        if let Some(output_path) = request_clone.output_path {
                            if let Err(err) = task_manager_clone
                                .hard_link_or_copy(&task_clone, Path::new(output_path.as_str()))
                                .await
                            {
                                error!("hard link or copy persistent cache task: {}", err);
                                out_stream_tx
                                    .send(Err(Status::internal(err.to_string())))
                                    .await
                                    .unwrap_or_else(|err| {
                                        error!("send download progress error: {:?}", err)
                                    });
                            };
                        }
                    }
                    Err(e) => {
                        // Download task failed.
                        task_manager_clone
                            .download_failed(task_clone.id.as_str())
                            .await
                            .unwrap_or_else(|err| {
                                error!("download persistent cache task failed: {}", err)
                            });

                        // Collect download task failure metrics.
                        collect_download_task_failure_metrics(
                            task_type,
                            request.tag.clone().unwrap_or_default().as_str(),
                            request.application.clone().unwrap_or_default().as_str(),
                            task_priority.to_string().as_str(),
                        );

                        error!("download failed: {}", e);
                    }
                }

                drop(out_stream_tx);
            }
            .in_current_span(),
        );

        Ok(Response::new(ReceiverStream::new(out_stream_rx)))
    }

    /// stat_persistent_cache_task stats the persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id))]
    async fn stat_persistent_cache_task(
        &self,
        request: Request<StatPersistentCacheTaskRequest>,
    ) -> Result<Response<PersistentCacheTask>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());

        // Collect the stat task started metrics.
        collect_stat_task_started_metrics(TaskType::PersistentCache as i32);

        let task = self
            .persistent_cache_task
            .stat(task_id.as_str(), host_id.as_str())
            .await
            .map_err(|err| {
                // Collect the stat task failure metrics.
                collect_stat_task_failure_metrics(TaskType::PersistentCache as i32);

                error!("stat persistent cache task: {}", err);
                Status::internal(err.to_string())
            })?;

        Ok(Response::new(task))
    }

    /// delete_persistent_cache_task deletes the persistent cache task.
    #[instrument(skip_all, fields(host_id, task_id))]
    async fn delete_persistent_cache_task(
        &self,
        request: Request<DeletePersistentCacheTaskRequest>,
    ) -> Result<Response<()>, Status> {
        // Clone the request.
        let request = request.into_inner();

        // Generate the host id.
        let host_id = self.task.id_generator.host_id();

        // Get the task id from the request.
        let task_id = request.task_id;

        // Span record the host id and task id.
        Span::current().record("host_id", host_id.as_str());
        Span::current().record("task_id", task_id.as_str());

        // Collect the delete task started metrics.
        collect_delete_task_started_metrics(TaskType::PersistentCache as i32);
        self.persistent_cache_task.delete(task_id.as_str()).await;
        Ok(Response::new(()))
    }
}

/// DfdaemonUploadClient is a wrapper of DfdaemonUploadGRPCClient.
#[derive(Clone)]
pub struct DfdaemonUploadClient {
    /// client is the grpc client of the dfdaemon upload.
    pub client: DfdaemonUploadGRPCClient<InterceptedService<Channel, TracingInterceptor>>,
}

/// DfdaemonUploadClient implements the dfdaemon upload grpc client.
impl DfdaemonUploadClient {
    /// new creates a new DfdaemonUploadClient.
    #[instrument(skip_all)]
    pub async fn new(config: Arc<Config>, addr: String) -> ClientResult<Self> {
        let domain_name = Url::parse(addr.as_str())?
            .host_str()
            .ok_or_else(|| {
                error!("invalid address: {}", addr);
                ClientError::InvalidParameter
            })?
            .to_string();

        let channel = match config
            .upload
            .client
            .load_client_tls_config(domain_name.as_str())
            .await?
        {
            Some(client_tls_config) => {
                Channel::from_static(Box::leak(addr.clone().into_boxed_str()))
                    .tls_config(client_tls_config)?
                    .tcp_nodelay(true)
                    .buffer_size(super::BUFFER_SIZE)
                    .connect_timeout(super::CONNECT_TIMEOUT)
                    .timeout(super::REQUEST_TIMEOUT)
                    .connect()
                    .await
                    .inspect_err(|err| {
                        error!("connect to {} failed: {}", addr, err);
                    })
                    .or_err(ErrorType::ConnectError)?
            }
            None => Channel::from_static(Box::leak(addr.clone().into_boxed_str()))
                .tcp_nodelay(true)
                .buffer_size(super::BUFFER_SIZE)
                .connect_timeout(super::CONNECT_TIMEOUT)
                .timeout(super::REQUEST_TIMEOUT)
                .connect()
                .await
                .inspect_err(|err| {
                    error!("connect to {} failed: {}", addr, err);
                })
                .or_err(ErrorType::ConnectError)?,
        };

        let client = DfdaemonUploadGRPCClient::with_interceptor(channel, TracingInterceptor)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    /// download_task downloads the task.
    #[instrument(skip_all)]
    pub async fn download_task(
        &self,
        request: DownloadTaskRequest,
    ) -> ClientResult<tonic::Response<tonic::codec::Streaming<DownloadTaskResponse>>> {
        // Get the download from the request.
        let download = request.clone().download.ok_or_else(|| {
            tonic::Status::invalid_argument("missing download in download task request")
        })?;

        // Initialize the request.
        let mut request = tonic::Request::new(request);

        // Set the timeout to the request.
        if let Some(timeout) = download.timeout {
            request.set_timeout(
                Duration::try_from(timeout)
                    .map_err(|_| tonic::Status::invalid_argument("invalid timeout"))?,
            );
        }

        let response = self.client.clone().download_task(request).await?;
        Ok(response)
    }

    /// sync_pieces provides the piece metadata for parent.
    #[instrument(skip_all)]
    pub async fn sync_pieces(
        &self,
        request: SyncPiecesRequest,
    ) -> ClientResult<tonic::Response<tonic::codec::Streaming<SyncPiecesResponse>>> {
        let request = Self::make_request(request);
        let response = self.client.clone().sync_pieces(request).await?;
        Ok(response)
    }

    /// download_piece provides the piece content for parent.
    #[instrument(skip_all)]
    pub async fn download_piece(
        &self,
        request: DownloadPieceRequest,
        timeout: Duration,
    ) -> ClientResult<DownloadPieceResponse> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(timeout);

        let response = self.client.clone().download_piece(request).await?;
        Ok(response.into_inner())
    }

    /// download_persistent_cache_task downloads the persistent cache task.
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_task(
        &self,
        request: DownloadPersistentCacheTaskRequest,
    ) -> ClientResult<tonic::Response<tonic::codec::Streaming<DownloadPersistentCacheTaskResponse>>>
    {
        // Clone the request.
        let request_clone = request.clone();

        // Initialize the request.
        let mut request = tonic::Request::new(request);

        // Set the timeout to the request.
        if let Some(timeout) = request_clone.timeout {
            request.set_timeout(
                Duration::try_from(timeout)
                    .map_err(|_| tonic::Status::invalid_argument("invalid timeout"))?,
            );
        }

        let response = self
            .client
            .clone()
            .download_persistent_cache_task(request)
            .await?;
        Ok(response)
    }

    /// stat_persistent_cache_task stats the persistent cache task.
    #[instrument(skip_all)]
    pub async fn stat_persistent_cache_task(
        &self,
        request: StatPersistentCacheTaskRequest,
    ) -> ClientResult<PersistentCacheTask> {
        let request = Self::make_request(request);
        let response = self
            .client
            .clone()
            .stat_persistent_cache_task(request)
            .await?;
        Ok(response.into_inner())
    }

    /// delete_persistent_cache_task deletes the persistent cache task.
    #[instrument(skip_all)]
    pub async fn delete_persistent_cache_task(
        &self,
        request: DeletePersistentCacheTaskRequest,
    ) -> ClientResult<()> {
        let request = Self::make_request(request);
        let _response = self
            .client
            .clone()
            .delete_persistent_cache_task(request)
            .await?;
        Ok(())
    }

    /// make_request creates a new request with timeout.
    #[instrument(skip_all)]
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
