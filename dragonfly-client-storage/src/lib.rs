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

use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::digest::{Algorithm, Digest};
use reqwest::header::HeaderMap;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tracing::info;
use tracing::{debug, error, instrument};

pub mod cache;
pub mod content;
pub mod metadata;
pub mod storage_engine;

/// DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL is the default interval for waiting for the piece to be finished.
pub const DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL: Duration = Duration::from_millis(100);

/// Storage is the storage of the task.
pub struct Storage {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// metadata implements the metadata storage.
    metadata: metadata::Metadata,

    /// content implements the content storage.
    content: content::Content,

    /// cache implements the cache for preheat task.
    cache: cache::Cache,
}

/// Storage implements the storage.
impl Storage {
    /// new returns a new storage.
    #[instrument(skip_all)]
    pub async fn new(config: Arc<Config>, dir: &Path, log_dir: PathBuf) -> Result<Self> {
        let metadata = metadata::Metadata::new(config.clone(), dir, &log_dir)?;
        let content = content::Content::new(config.clone(), dir).await?;
        let cache = cache::Cache::new(config.clone())?;

        Ok(Storage {
            config,
            metadata,
            content,
            cache,
        })
    }

    /// hard_link_or_copy_task hard links or copies the task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_or_copy_task(
        &self,
        task: &metadata::Task,
        to: &Path,
        range: Option<Range>,
    ) -> Result<()> {
        self.content.hard_link_or_copy_task(task, to, range).await
    }

    /// download_task_started updates the metadata of the task when the task downloads started.
    #[instrument(skip_all)]
    pub fn download_task_started(
        &self,
        id: &str,
        piece_length: Option<u64>,
        content_length: Option<u64>,
        response_header: Option<HeaderMap>,
    ) -> Result<metadata::Task> {
        self.metadata
            .download_task_started(id, piece_length, content_length, response_header)
    }

    /// download_task_finished updates the metadata of the task when the task downloads finished.
    #[instrument(skip_all)]
    pub fn download_task_finished(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.download_task_finished(id)
    }

    /// download_task_failed updates the metadata of the task when the task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_task_failed(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.download_task_failed(id)
    }

    /// prefetch_task_started updates the metadata of the task when the task prefetches started.
    #[instrument(skip_all)]
    pub async fn prefetch_task_started(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.prefetch_task_started(id)
    }

    /// prefetch_task_failed updates the metadata of the task when the task prefetches failed.
    #[instrument(skip_all)]
    pub async fn prefetch_task_failed(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.prefetch_task_failed(id)
    }

    /// upload_task_finished updates the metadata of the task when task uploads finished.
    #[instrument(skip_all)]
    pub fn upload_task_finished(&self, id: &str) -> Result<metadata::Task> {
        self.metadata.upload_task_finished(id)
    }

    /// get_task returns the task metadata.
    #[instrument(skip_all)]
    pub fn get_task(&self, id: &str) -> Result<Option<metadata::Task>> {
        self.metadata.get_task(id)
    }

    /// is_task_exists returns whether the task exists.
    #[instrument(skip_all)]
    pub fn is_task_exists(&self, id: &str) -> Result<bool> {
        self.metadata.is_task_exists(id)
    }

    /// get_tasks returns the task metadatas.
    #[instrument(skip_all)]
    pub fn get_tasks(&self) -> Result<Vec<metadata::Task>> {
        self.metadata.get_tasks()
    }

    /// delete_task deletes the task metadatas, task content and piece metadatas.
    #[instrument(skip_all)]
    pub async fn delete_task(&self, id: &str) {
        self.metadata
            .delete_task(id)
            .unwrap_or_else(|err| error!("delete task metadata failed: {}", err));

        self.metadata.delete_pieces(id).unwrap_or_else(|err| {
            error!("delete piece metadatas failed: {}", err);
        });

        self.content.delete_task(id).await.unwrap_or_else(|err| {
            error!("delete task content failed: {}", err);
        });
    }

    /// hard_link_or_copy_persistent_cache_task hard links or copies the persistent cache task content to the destination.
    #[instrument(skip_all)]
    pub async fn hard_link_or_copy_persistent_cache_task(
        &self,
        task: &metadata::PersistentCacheTask,
        to: &Path,
    ) -> Result<()> {
        self.content
            .hard_link_or_copy_persistent_cache_task(task, to)
            .await
    }

    /// create_persistent_cache_task_started creates a new persistent cache task.
    #[instrument(skip_all)]
    pub async fn create_persistent_cache_task_started(
        &self,
        id: &str,
        ttl: Duration,
        piece_length: u64,
        content_length: u64,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata
            .create_persistent_cache_task_started(id, ttl, piece_length, content_length)
    }

    /// create_persistent_cache_task_finished updates the metadata of the persistent cache task
    /// when the persistent cache task creates finished.
    #[instrument(skip_all)]
    pub async fn create_persistent_cache_task_finished(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.create_persistent_cache_task_finished(id)
    }

    /// create_persistent_cache_task_failed deletes the persistent cache task when
    /// the persistent cache task creates failed.
    #[instrument(skip_all)]
    pub async fn create_persistent_cache_task_failed(&self, id: &str) {
        self.delete_persistent_cache_task(id).await;
    }

    /// download_persistent_cache_task_started updates the metadata of the persistent cache task
    /// when the persistent cache task downloads started.
    #[instrument(skip_all)]
    pub fn download_persistent_cache_task_started(
        &self,
        id: &str,
        ttl: Duration,
        persistent: bool,
        piece_length: u64,
        content_length: u64,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.download_persistent_cache_task_started(
            id,
            ttl,
            persistent,
            piece_length,
            content_length,
        )
    }

    /// download_persistent_cache_task_finished updates the metadata of the persistent cache task when the persistent cache task downloads finished.
    #[instrument(skip_all)]
    pub fn download_persistent_cache_task_finished(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.download_persistent_cache_task_finished(id)
    }

    /// download_persistent_cache_task_failed updates the metadata of the persistent cache task when the persistent cache task downloads failed.
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_task_failed(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.download_persistent_cache_task_failed(id)
    }

    /// upload_persistent_cache_task_finished updates the metadata of the cahce task when persistent cache task uploads finished.
    #[instrument(skip_all)]
    pub fn upload_persistent_cache_task_finished(
        &self,
        id: &str,
    ) -> Result<metadata::PersistentCacheTask> {
        self.metadata.upload_persistent_cache_task_finished(id)
    }

    /// get_persistent_cache_task returns the persistent cache task metadata.
    #[instrument(skip_all)]
    pub fn get_persistent_cache_task(
        &self,
        id: &str,
    ) -> Result<Option<metadata::PersistentCacheTask>> {
        self.metadata.get_persistent_cache_task(id)
    }

    /// is_persistent_cache_task_exists returns whether the persistent cache task exists.
    #[instrument(skip_all)]
    pub fn is_persistent_cache_task_exists(&self, id: &str) -> Result<bool> {
        self.metadata.is_persistent_cache_task_exists(id)
    }

    /// get_tasks returns the task metadatas.
    #[instrument(skip_all)]
    pub fn get_persistent_cache_tasks(&self) -> Result<Vec<metadata::PersistentCacheTask>> {
        self.metadata.get_persistent_cache_tasks()
    }

    /// delete_persistent_cache_task deletes the persistent cache task metadatas, persistent cache task content and piece metadatas.
    #[instrument(skip_all)]
    pub async fn delete_persistent_cache_task(&self, id: &str) {
        self.metadata
            .delete_persistent_cache_task(id)
            .unwrap_or_else(|err| {
                error!("delete persistent cache task metadata failed: {}", err);
            });

        self.metadata.delete_pieces(id).unwrap_or_else(|err| {
            error!("delete persistent cache piece metadatas failed: {}", err);
        });

        self.content
            .delete_persistent_cache_task(id)
            .await
            .unwrap_or_else(|err| {
                error!("delete persistent cache task content failed: {}", err);
            });
    }

    /// create_persistent_cache_piece creates a new persistent cache piece.
    #[instrument(skip_all)]
    pub async fn create_persistent_cache_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        number: u32,
        offset: u64,
        length: u64,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_piece_with_crc32_castagnoli(task_id, offset, reader)
            .await?;
        let digest = Digest::new(Algorithm::Crc32, response.hash);

        self.metadata.create_persistent_cache_piece(
            piece_id,
            number,
            offset,
            length,
            digest.to_string().as_str(),
        )
    }

    /// download_piece_started updates the metadata of the piece and writes
    /// the data of piece to file when the piece downloads started.
    #[instrument(skip_all)]
    pub async fn download_piece_started(
        &self,
        piece_id: &str,
        number: u32,
    ) -> Result<metadata::Piece> {
        // Wait for the piece to be finished.
        match self.wait_for_piece_finished(piece_id).await {
            Ok(piece) => Ok(piece),
            // If piece is not found or wait timeout, create piece metadata.
            Err(_) => self.metadata.download_piece_started(piece_id, number),
        }
    }

    /// download_piece_from_source_finished is used for downloading piece from source.
    #[instrument(skip_all)]
    pub async fn download_piece_from_source_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_piece_with_crc32_castagnoli(task_id, offset, reader)
            .await?;
        let digest = Digest::new(Algorithm::Crc32, response.hash);

        self.metadata.download_piece_finished(
            piece_id,
            offset,
            length,
            digest.to_string().as_str(),
            None,
        )
    }

    /// download_piece_from_parent_finished is used for downloading piece from parent.
    #[instrument(skip_all)]
    pub async fn download_piece_from_parent_finished<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        expected_digest: &str,
        parent_id: &str,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_piece_with_crc32_castagnoli(task_id, offset, reader)
            .await?;

        let length = response.length;
        let digest = Digest::new(Algorithm::Crc32, response.hash);

        // Check the digest of the piece.
        if expected_digest != digest.to_string() {
            return Err(Error::DigestMismatch(
                expected_digest.to_string(),
                digest.to_string(),
            ));
        }

        self.metadata.download_piece_finished(
            piece_id,
            offset,
            length,
            digest.to_string().as_str(),
            Some(parent_id.to_string()),
        )
    }

    /// download_piece_failed updates the metadata of the piece when the piece downloads failed.
    #[instrument(skip_all)]
    pub fn download_piece_failed(&self, piece_id: &str) -> Result<()> {
        self.metadata.download_piece_failed(piece_id)
    }

    /// upload_piece updates the metadata of the piece and
    /// returns the data of the piece.
    #[instrument(skip_all)]
    pub async fn upload_piece(
        &self,
        piece_id: &str,
        task_id: &str,
        range: Option<Range>,
    ) -> Result<Pin<Box<dyn AsyncRead + Send>>> {
        // Wait for the piece to be finished.
        self.wait_for_piece_finished(piece_id).await?;

        // Start uploading the task.
        self.metadata.upload_task_started(task_id)?;

        // Get the piece metadata and return the content of the piece.
        match self.metadata.get_piece(piece_id) {
            Ok(Some(piece)) => {
                // Try to upload piece content form cache.
                if !self.cache.is_empty().await && self.cache.contains_piece(piece_id).await {
                    match self
                        .cache
                        .read_piece(piece_id, piece.offset, piece.length, range)
                        .await
                    {
                        Ok(cache_reader) => {
                            // Finish uploading the task.
                            info!("hit cache: {}", piece_id);
                            self.metadata.upload_task_finished(task_id)?;
                            return Ok(Box::pin(cache_reader));
                        }
                        Err(_err) => {}
                    }
                }

                // Upload piece content from storage when cache entry is not hit or cache is empty.
                match self
                    .content
                    .read_piece(task_id, piece.offset, piece.length, range)
                    .await
                {
                    Ok(reader) => {
                        // Finish uploading the task.
                        self.metadata.upload_task_finished(task_id)?;
                        Ok(Box::pin(reader))
                    }
                    Err(err) => {
                        // Failed uploading the task.
                        self.metadata.upload_task_failed(task_id)?;
                        Err(err)
                    }
                }
            }
            Ok(None) => {
                // Failed uploading the task.
                self.metadata.upload_task_failed(task_id)?;
                Err(Error::PieceNotFound(piece_id.to_string()))
            }
            Err(err) => {
                // Failed uploading the task.
                self.metadata.upload_task_failed(task_id)?;
                Err(err)
            }
        }
    }

    /// upload_piece_with_dual_read returns the dual reader of the piece, one is the range reader, and the other is the
    /// full reader of the piece. It is used for cache the piece content to the proxy cache.
    #[instrument(skip_all)]
    pub async fn upload_piece_with_dual_read(
        &self,
        piece_id: &str,
        task_id: &str,
        range: Option<Range>,
    ) -> Result<(impl AsyncRead, impl AsyncRead)> {
        // Wait for the piece to be finished.
        self.wait_for_piece_finished(piece_id).await?;

        // Start uploading the task.
        self.metadata.upload_task_started(task_id)?;

        // Get the piece metadata and return the content of the piece.
        match self.metadata.get_piece(piece_id) {
            Ok(Some(piece)) => {
                match self
                    .content
                    .read_piece_with_dual_read(task_id, piece.offset, piece.length, range)
                    .await
                {
                    Ok(dual_reader) => {
                        // Finish uploading the task.
                        self.metadata.upload_task_finished(task_id)?;
                        Ok(dual_reader)
                    }
                    Err(err) => {
                        // Failed uploading the task.
                        self.metadata.upload_task_failed(task_id)?;
                        Err(err)
                    }
                }
            }
            Ok(None) => {
                // Failed uploading the task.
                self.metadata.upload_task_failed(task_id)?;
                Err(Error::PieceNotFound(piece_id.to_string()))
            }
            Err(err) => {
                // Failed uploading the task.
                self.metadata.upload_task_failed(task_id)?;
                Err(err)
            }
        }
    }

    /// upload_piece_from_cache uploads the piece content by piece id from cache.
    #[instrument(skip_all)]
    pub async fn upload_piece_from_cache(
        &self,
        piece_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        self.cache.read_piece(piece_id, offset, length, range).await
    }

    /// load_piece_to_cache loads the piece content with piece id to the cache.
    #[instrument(skip_all)]
    pub async fn load_piece_to_cache<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        reader: &mut R,
        length: u64,
    ) {
        // If the piece is already in the cache, return.
        if self.cache.contains_piece(piece_id).await {
            return;
        }

        // Load the piece content to the cache.
        match self.cache.write_piece(piece_id, reader, length).await {
            Ok(_) => {}
            Err(err) => {
                error!("load piece to cache failed: {}", err);
            }
        }
    }

    /// get_piece returns the piece metadata.
    #[instrument(skip_all)]
    pub fn get_piece(&self, piece_id: &str) -> Result<Option<metadata::Piece>> {
        self.metadata.get_piece(piece_id)
    }

    /// is_piece_exists returns whether the piece exists.
    #[instrument(skip_all)]
    pub fn is_piece_exists(&self, piece_id: &str) -> Result<bool> {
        self.metadata.is_piece_exists(piece_id)
    }

    /// get_pieces returns the piece metadatas.
    pub fn get_pieces(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.metadata.get_pieces(task_id)
    }

    /// piece_id returns the piece id.
    #[inline]
    #[instrument(skip_all)]
    pub fn piece_id(&self, task_id: &str, number: u32) -> String {
        self.metadata.piece_id(task_id, number)
    }

    /// download_persistent_cache_piece_started updates the metadata of the persistent cache piece and writes
    /// the data of piece to file when the persistent cache piece downloads started.
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_piece_started(
        &self,
        piece_id: &str,
        number: u32,
    ) -> Result<metadata::Piece> {
        // Wait for the piece to be finished.
        match self
            .wait_for_persistent_cache_piece_finished(piece_id)
            .await
        {
            Ok(piece) => Ok(piece),
            // If piece is not found or wait timeout, create piece metadata.
            Err(_) => self.metadata.download_piece_started(piece_id, number),
        }
    }

    /// download_persistent_cache_piece_from_parent_finished is used for downloading persistent cache piece from parent.
    #[instrument(skip_all)]
    pub async fn download_persistent_cache_piece_from_parent_finished<
        R: AsyncRead + Unpin + ?Sized,
    >(
        &self,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        expected_digest: &str,
        parent_id: &str,
        reader: &mut R,
    ) -> Result<metadata::Piece> {
        let response = self
            .content
            .write_piece_with_crc32_castagnoli(task_id, offset, reader)
            .await?;

        let length = response.length;
        let digest = Digest::new(Algorithm::Crc32, response.hash);

        // Check the digest of the piece.
        if expected_digest != digest.to_string() {
            return Err(Error::DigestMismatch(
                expected_digest.to_string(),
                digest.to_string(),
            ));
        }

        self.metadata.download_piece_finished(
            piece_id,
            offset,
            length,
            digest.to_string().as_str(),
            Some(parent_id.to_string()),
        )
    }

    /// download_persistent_cache_piece_failed updates the metadata of the persistent cache piece when the persistent cache piece downloads failed.
    #[instrument(skip_all)]
    pub fn download_persistent_cache_piece_failed(&self, piece_id: &str) -> Result<()> {
        self.metadata.download_piece_failed(piece_id)
    }

    /// upload_persistent_cache_piece updates the metadata of the piece and_then
    /// returns the data of the piece.
    #[instrument(skip_all)]
    pub async fn upload_persistent_cache_piece(
        &self,
        piece_id: &str,
        task_id: &str,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        // Wait for the persistent cache piece to be finished.
        self.wait_for_persistent_cache_piece_finished(piece_id)
            .await?;

        // Start uploading the persistent cache task.
        self.metadata
            .upload_persistent_cache_task_started(task_id)?;

        // Get the persistent cache piece metadata and return the content of the persistent cache piece.
        match self.metadata.get_piece(piece_id) {
            Ok(Some(piece)) => {
                match self
                    .content
                    .read_piece(task_id, piece.offset, piece.length, range)
                    .await
                {
                    Ok(reader) => {
                        // Finish uploading the persistent cache task.
                        self.metadata
                            .upload_persistent_cache_task_finished(task_id)?;
                        Ok(reader)
                    }
                    Err(err) => {
                        // Failed uploading the persistent cache task.
                        self.metadata.upload_persistent_cache_task_failed(task_id)?;
                        Err(err)
                    }
                }
            }
            Ok(None) => {
                // Failed uploading the persistent cache task.
                self.metadata.upload_persistent_cache_task_failed(task_id)?;
                Err(Error::PieceNotFound(piece_id.to_string()))
            }
            Err(err) => {
                // Failed uploading the persistent cache task.
                self.metadata.upload_persistent_cache_task_failed(task_id)?;
                Err(err)
            }
        }
    }

    /// get_persistent_cache_piece returns the persistent cache piece metadata.
    #[instrument(skip_all)]
    pub fn get_persistent_cache_piece(&self, piece_id: &str) -> Result<Option<metadata::Piece>> {
        self.metadata.get_piece(piece_id)
    }

    /// is_persistent_cache_piece_exists returns whether the persistent cache piece exists.
    #[instrument(skip_all)]
    pub fn is_persistent_cache_piece_exists(&self, piece_id: &str) -> Result<bool> {
        self.metadata.is_piece_exists(piece_id)
    }

    /// get_persistent_cache_pieces returns the persistent cache piece metadatas.
    pub fn get_persistent_cache_pieces(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.metadata.get_pieces(task_id)
    }

    /// persistent_cache_piece_id returns the persistent cache piece id.
    #[inline]
    #[instrument(skip_all)]
    pub fn persistent_cache_piece_id(&self, task_id: &str, number: u32) -> String {
        self.metadata.piece_id(task_id, number)
    }

    /// wait_for_piece_finished waits for the piece to be finished.
    #[instrument(skip_all)]
    async fn wait_for_piece_finished(&self, piece_id: &str) -> Result<metadata::Piece> {
        // Initialize the timeout of piece.
        let piece_timeout = tokio::time::sleep(self.config.download.piece_timeout);
        tokio::pin!(piece_timeout);

        // Initialize the interval of piece.
        let mut wait_for_piece_count = 0;
        let mut interval = tokio::time::interval(DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let piece = self
                        .get_piece(piece_id)?
                        .ok_or_else(|| Error::PieceNotFound(piece_id.to_string()))?;

                    // If the piece is finished, return.
                    if piece.is_finished() {
                        debug!("wait piece finished success");
                        return Ok(piece);
                    }

                    if wait_for_piece_count > 0 {
                        debug!("wait piece finished");
                    }
                    wait_for_piece_count += 1;
                }
                _ = &mut piece_timeout => {
                    self.metadata.wait_for_piece_finished_failed(piece_id).unwrap_or_else(|err| error!("delete piece metadata failed: {}", err));
                    return Err(Error::WaitForPieceFinishedTimeout(piece_id.to_string()));
                }
            }
        }
    }

    /// wait_for_persistent_cache_piece_finished waits for the persistent cache piece to be finished.
    #[instrument(skip_all)]
    async fn wait_for_persistent_cache_piece_finished(
        &self,
        piece_id: &str,
    ) -> Result<metadata::Piece> {
        // Initialize the timeout of piece.
        let piece_timeout = tokio::time::sleep(self.config.download.piece_timeout);
        tokio::pin!(piece_timeout);

        // Initialize the interval of piece.
        let mut wait_for_piece_count = 0;
        let mut interval = tokio::time::interval(DEFAULT_WAIT_FOR_PIECE_FINISHED_INTERVAL);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let piece = self
                        .get_persistent_cache_piece(piece_id)?
                        .ok_or_else(|| Error::PieceNotFound(piece_id.to_string()))?;

                    // If the piece is finished, return.
                    if piece.is_finished() {
                        debug!("wait piece finished success");
                        return Ok(piece);
                    }

                    if wait_for_piece_count > 0 {
                        debug!("wait piece finished");
                    }
                    wait_for_piece_count += 1;
                }
                _ = &mut piece_timeout => {
                    self.metadata.wait_for_piece_finished_failed(piece_id).unwrap_or_else(|err| error!("delete piece metadata failed: {}", err));
                    return Err(Error::WaitForPieceFinishedTimeout(piece_id.to_string()));
                }
            }
        }
    }
}
