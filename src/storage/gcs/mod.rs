//! **yrs-gcs** is a persistence layer allowing to store [Yrs](https://docs.rs/yrs/latest/yrs/index.html)
//! documents and providing convenient utility functions to work with them, using Google Cloud Storage for
//! persistent backend.

pub use super::kv as store;
use super::kv::{DocOps, KVEntry, KVStore};
use google_cloud_storage::{
    client::{Client, ClientConfig},
    http::objects::delete::DeleteObjectRequest,
    http::objects::download::Range,
    http::objects::get::GetObjectRequest,
    http::objects::list::ListObjectsRequest,
    http::objects::upload::{Media, UploadObjectRequest, UploadType},
    http::objects::Object,
};
use thiserror::Error;
use tracing::{debug, info};

#[derive(Debug, Error)]
pub enum GcsError {
    #[error("Google API error: {0}")]
    GoogleApi(#[from] google_cloud_storage::http::Error),

    #[error("Other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Type wrapper around GCS Client struct. Used to extend GCS with [DocOps]
/// methods used for convenience when working with Yrs documents.
pub struct GcsStore {
    #[allow(dead_code)]
    client: Client,
    bucket: String,
}

impl std::fmt::Debug for GcsStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcsStore")
            .field("bucket", &self.bucket)
            .finish_non_exhaustive()
    }
}

impl GcsStore {
    pub async fn new(bucket: String) -> Result<Self, GcsError> {
        let config = ClientConfig::default();
        let client = Client::new(config);
        Ok(Self { client, bucket })
    }

    pub async fn with_client(client: Client, bucket: String) -> Self {
        Self { client, bucket }
    }
}

impl<'db> DocOps<'db> for GcsStore {}

impl KVStore for GcsStore {
    type Error = GcsError;
    type Cursor = GcsRange;
    type Entry = GcsEntry;
    type Return = Vec<u8>;

    async fn get(&self, key: &[u8]) -> Result<Option<Self::Return>, Self::Error> {
        let key = String::from_utf8_lossy(key);
        let request = GetObjectRequest {
            bucket: self.bucket.clone(),
            object: key.to_string(),
            ..Default::default()
        };

        match self
            .client
            .download_object(&request, &Range::default())
            .await
        {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.to_string().contains("not found") => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let key = String::from_utf8_lossy(key);
        let upload_type = UploadType::Simple(Media::new(key.to_string()));
        let request = UploadObjectRequest {
            bucket: self.bucket.clone(),
            ..Default::default()
        };

        info!("Writing to GCS storage - key: {:?}", key);
        debug!("Value length: {} bytes", value.len());

        self.client
            .upload_object(&request, value.to_vec(), &upload_type)
            .await
            .map_err(GcsError::from)?;
        Ok(())
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Self::Error> {
        let key = String::from_utf8_lossy(key);
        let request = DeleteObjectRequest {
            bucket: self.bucket.clone(),
            object: key.to_string(),
            ..Default::default()
        };

        match self.client.delete_object(&request).await {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("not found") => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error> {
        let from = String::from_utf8_lossy(from);
        let request = ListObjectsRequest {
            bucket: self.bucket.clone(),
            prefix: Some(from.to_string()),
            ..Default::default()
        };

        let objects = self
            .client
            .list_objects(&request)
            .await
            .map_err(GcsError::from)?
            .items
            .unwrap_or_default();

        let to = String::from_utf8_lossy(to);

        for obj in objects {
            if obj.name.as_str() <= to.as_ref() {
                let delete_request = DeleteObjectRequest {
                    bucket: self.bucket.clone(),
                    object: obj.name,
                    ..Default::default()
                };
                self.client
                    .delete_object(&delete_request)
                    .await
                    .map_err(GcsError::from)?;
            }
        }

        Ok(())
    }

    async fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error> {
        let from = String::from_utf8_lossy(from);
        let request = ListObjectsRequest {
            bucket: self.bucket.clone(),
            prefix: Some(from.to_string()),
            ..Default::default()
        };

        let response = self
            .client
            .list_objects(&request)
            .await
            .map_err(GcsError::from)?;
        let to = String::from_utf8_lossy(to);

        let objects = response
            .items
            .unwrap_or_default()
            .into_iter()
            .filter(|obj| obj.name.as_str() <= to.as_ref())
            .collect();

        Ok(GcsRange {
            objects,
            current: 0,
        })
    }

    async fn peek_back(&self, key: &[u8]) -> Result<Option<Self::Entry>, Self::Error> {
        let key = String::from_utf8_lossy(key);
        let request = ListObjectsRequest {
            bucket: self.bucket.clone(),
            prefix: Some(key.to_string()),
            ..Default::default()
        };

        let mut objects: Vec<_> = self
            .client
            .list_objects(&request)
            .await
            .map_err(GcsError::from)?
            .items
            .unwrap_or_default()
            .into_iter()
            .filter(|obj| obj.name.as_str() < key.as_ref())
            .collect();

        if let Some(obj) = objects.pop() {
            let get_request = GetObjectRequest {
                bucket: self.bucket.clone(),
                object: obj.name.clone(),
                ..Default::default()
            };

            let value = self
                .client
                .download_object(&get_request, &Range::default())
                .await
                .map_err(GcsError::from)?;

            Ok(Some(GcsEntry {
                key: obj.name.into_bytes(),
                value,
            }))
        } else {
            Ok(None)
        }
    }
}

pub struct GcsRange {
    objects: Vec<Object>,
    current: usize,
}

impl Iterator for GcsRange {
    type Item = GcsEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.objects.len() {
            return None;
        }
        let obj = &self.objects[self.current];
        self.current += 1;

        Some(GcsEntry {
            key: obj.name.clone().into_bytes(),
            value: vec![], // Need to download actual value
        })
    }
}

pub struct GcsEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KVEntry for GcsEntry {
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn value(&self) -> &[u8] {
        &self.value
    }
}
