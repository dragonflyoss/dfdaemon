/*
 *     Copyright 2025 The Dragonfly Authors
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

use bytes::Bytes;
use dragonfly_api::common::v2::Range;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use lru::LruCache;
use std::cmp::{max, min};
use std::io::Cursor;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, BufReader};
use tokio::sync::Mutex;

/// Cache is the cache for storing piece content by LRU algorithm.
///
/// Cache storage:
/// 1. Users can create preheating jobs and preheat tasks to memory and disk by setting `load_to_cache` to `true`.
///    For more details, refer to https://github.com/dragonflyoss/api/blob/main/proto/common.proto#L443.
/// 2. If the download hits the memory cache, it will be faster than reading from the disk, because there is no
///    page cache for the first read.
/// ```
///
///     1.Preheat
///         |
///         |
/// +--------------------------------------------------+
/// |       |              Peer                        |
/// |       |                   +-----------+          |
/// |       |     -- Partial -->|   Cache   |          |
/// |       |     |             +-----------+          |
/// |       v     |                |    |              |
/// |   Download  |              Miss   |              |             
/// |     Task -->|                |    --- Hit ------>|<-- 2.Download
/// |             |                |               ^   |              
/// |             |                v               |   |
/// |             |          +-----------+         |   |
/// |             -- Full -->|   Disk    |----------   |
/// |                        +-----------+             |
/// |                                                  |
/// +--------------------------------------------------+
/// ```
#[derive(Clone)]
pub struct Cache {
    /// pieces stores the pieces with their piece id and content.
    pieces: Arc<Mutex<LruCache<String, bytes::Bytes>>>,
}

/// Cache implements the cache for storing piece content by LRU algorithm.
impl Cache {
    /// new creates a new cache with the specified capacity.
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let capacity =
            NonZeroUsize::new(config.storage.cache_capacity).ok_or(Error::InvalidParameter)?;
        let pieces = Arc::new(Mutex::new(LruCache::new(capacity)));

        Ok(Cache { pieces })
    }

    /// read_piece reads the piece from the cache.
    pub async fn read_piece(
        &self,
        piece_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        // Try to get the piece content from the cache.
        let mut pieces = self.pieces.lock().await;
        let Some(piece_content) = pieces.get(piece_id).cloned() else {
            return Err(Error::PieceNotFound(piece_id.to_string()));
        };

        // Calculate the range of bytes to return based on the range provided.
        let (target_offset, target_length) = if let Some(range) = range {
            let target_offset = max(offset, range.start) - offset;
            let target_length =
                min(offset + length - 1, range.start + range.length - 1) - target_offset - offset
                    + 1;
            (target_offset as usize, target_length as usize)
        } else {
            (0, length as usize)
        };

        // Check if the target range is valid.
        let begin = target_offset;
        let end = target_offset + target_length;
        if begin >= piece_content.len() || end > piece_content.len() {
            return Err(Error::InvalidParameter);
        }

        Ok(BufReader::new(Cursor::new(piece_content.slice(begin..end))))
    }

    /// write_piece writes the piece content to the cache.
    pub async fn write_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        piece_id: &str,
        reader: &mut R,
        length: u64,
    ) -> Result<()> {
        let mut pieces = self.pieces.lock().await;
        if pieces.contains(piece_id) {
            // The piece already exists in the cache.
            return Err(Error::Unknown(format!(
                "overwrite existing piece {}",
                piece_id
            )));
        }

        let mut buffer = Vec::with_capacity(length as usize);
        reader
            .read_to_end(&mut buffer)
            .await
            .map_err(|_err| Error::Unknown(piece_id.to_string()))?;

        pieces.put(piece_id.to_string(), Bytes::from(buffer));
        Ok(())
    }

    /// is_empty checks if the cache is empty.
    pub async fn is_empty(&self) -> bool {
        let pieces = self.pieces.lock().await;
        pieces.is_empty()
    }

    /// contains_piece checks whether the piece exists in the cache.
    pub async fn contains_piece(&self, id: &str) -> bool {
        let pieces = self.pieces.lock().await;
        pieces.contains(id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_client_config::dfdaemon::{Config, Storage};
    use std::{io::Cursor, sync::Arc};
    use tokio::io::AsyncReadExt;

    const PIECE_LENGTH: u64 = 4 * 1024 * 1024;

    fn create_test_config() -> Arc<Config> {
        Arc::new(Config {
            storage: Storage {
                cache_capacity: 10,
                ..Default::default()
            },
            ..Default::default()
        })
    }

    #[tokio::test]
    async fn test_cache_initialization() {
        let config = create_test_config();
        let cache = Cache::new(config.clone());
        assert!(cache.is_ok(), "Cache should initialize successfully");
    }

    #[tokio::test]
    async fn test_basic_read_write_operations() {
        let config = create_test_config();
        let cache = Cache::new(config).unwrap();

        // Test data setup
        let data = b"Hello Dragonfly";
        let data_len = data.len();
        let mut writer = Cursor::new(data.to_vec());

        // Write operation
        cache
            .write_piece("test_piece", &mut writer, data_len as u64)
            .await
            .expect("Writing piece should succeed");

        // Read operation verification
        let mut reader = cache
            .read_piece("test_piece", 0, data_len as u64, None)
            .await
            .expect("Reading piece should succeed");
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, data, "Read data should match written content");
    }

    #[tokio::test]
    async fn test_empty_state_detection() {
        let config = create_test_config();
        let cache = Cache::new(config).unwrap();

        assert!(cache.is_empty().await, "New cache should be empty");

        let data = b"content";
        let mut writer = Cursor::new(data.to_vec());
        cache
            .write_piece("temp", &mut writer, data.len() as u64)
            .await
            .unwrap();

        assert!(
            !cache.is_empty().await,
            "Cache should not be empty after write"
        );
    }

    #[tokio::test]
    async fn test_piece_existence_check() {
        let config = create_test_config();
        let cache = Cache::new(config).unwrap();

        assert!(
            !cache.contains_piece("missing").await,
            "Non-existent piece should not be reported"
        );

        let data = b"test";
        let mut writer = Cursor::new(data.to_vec());
        cache
            .write_piece("present", &mut writer, data.len() as u64)
            .await
            .unwrap();

        assert!(
            cache.contains_piece("present").await,
            "Existing piece should be detected"
        );
    }

    #[tokio::test]
    async fn test_range_reading() {
        let config = create_test_config();
        let cache = Cache::new(config).unwrap();
        let full_data = b"Range Test Content";
        let data_len = full_data.len() as u64;

        // Write full content
        let mut writer = Cursor::new(full_data.to_vec());
        cache
            .write_piece("range_test", &mut writer, data_len)
            .await
            .unwrap();

        // Test range read (bytes 6-10: "Test")
        let range = Range {
            start: 6,
            length: 4,
        };
        let mut reader = cache
            .read_piece("range_test", 0, data_len, Some(range))
            .await
            .expect("Valid range read should succeed");

        let mut buffer = String::new();
        reader.read_to_string(&mut buffer).await.unwrap();
        assert_eq!(buffer, "Test", "Partial content should match");
    }

    #[tokio::test]
    async fn test_missing_piece_handling() {
        let config = create_test_config();
        let cache = Cache::new(config).unwrap();

        let result = cache.read_piece("ghost", 0, 100, None).await;
        assert!(
            matches!(result, Err(Error::PieceNotFound(_))),
            "Should return PieceNotFound for missing content"
        );
    }

    #[tokio::test]
    async fn test_duplicate_write_prevention() {
        let config = create_test_config();
        let cache = Cache::new(config).unwrap();
        let data = b"Original";
        let data_len = data.len() as u64;

        // Initial write
        let mut writer = Cursor::new(data.to_vec());
        cache
            .write_piece("dupe_test", &mut writer, data_len)
            .await
            .unwrap();

        // Attempt overwrite
        let new_data = b"New Data";
        let mut new_writer = Cursor::new(new_data.to_vec());
        let result = cache
            .write_piece("dupe_test", &mut new_writer, new_data.len() as u64)
            .await;

        assert!(
            matches!(result, Err(Error::Unknown(s)) if s == "overwrite existing piece dupe_test"),
            "Should prevent overwriting existing pieces"
        );
    }

    #[tokio::test]
    async fn test_capacity_management() {
        let config = create_test_config();
        let cache = Cache::new(config).unwrap();
        let piece_length = PIECE_LENGTH;

        // Fill cache
        for i in 0..10 {
            let key = format!("item-{}", i);
            let data = vec![0u8; piece_length as usize];
            let mut writer = Cursor::new(data);
            cache
                .write_piece(&key, &mut writer, piece_length)
                .await
                .unwrap();
        }

        // Add one more piece to trigger eviction
        let new_data = vec![1u8; piece_length as usize];
        let mut new_writer = Cursor::new(new_data.clone());
        cache
            .write_piece("new_item", &mut new_writer, piece_length)
            .await
            .unwrap();

        // Verify oldest item was evicted
        assert!(
            !cache.contains_piece("item-0").await,
            "Oldest item should be evicted"
        );

        // Verify new item exists
        assert!(
            cache.contains_piece("new_item").await,
            "New item should be present"
        );
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        let config = create_test_config();
        let cache = Arc::new(Cache::new(config).unwrap());
        let mut handles = vec![];
        let piece_length = PIECE_LENGTH;

        // Spawn 10 concurrent writers
        for i in 0..10 {
            let cache = cache.clone();
            handles.push(tokio::spawn(async move {
                let key = format!("concurrent-{}", i);
                let data = vec![i as u8; piece_length as usize];
                let mut writer = Cursor::new(data.clone());
                cache
                    .write_piece(&key, &mut writer, piece_length)
                    .await
                    .unwrap();

                let mut reader = cache.read_piece(&key, 0, piece_length, None).await.unwrap();
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();
                assert_eq!(buffer, data, "Data integrity check failed");
            }));
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.expect("Task failed");
        }
    }
}
