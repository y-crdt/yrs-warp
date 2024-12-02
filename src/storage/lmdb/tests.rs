#[cfg(test)]
mod tests {
    use super::*;
    use tempdir::TempDir;
    use yrs::{Doc, GetString, Text, Transact};

    #[tokio::test]
    async fn test_basic_operations() {
        let dir = TempDir::new("test-lmdb").unwrap();
        let store = LmdbStore::new(dir.path()).unwrap();

        // Test insert
        let key = b"test-key";
        let value = b"test-value";
        store.upsert(key, value).await.unwrap();

        // Test get
        let result = store.get(key).await.unwrap();
        assert_eq!(result.as_deref(), Some(value));

        // Test remove
        store.remove(key).await.unwrap();
        let result = store.get(key).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_range_operations() {
        let dir = TempDir::new("test-lmdb").unwrap();
        let store = LmdbStore::new(dir.path()).unwrap();

        // Insert test data
        for i in 0..5 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            store.upsert(&key, &value).await.unwrap();
        }

        // Test range iteration
        let range = store.iter_range(b"key1", b"key3").await.unwrap();
        let entries: Vec<_> = range.into_iter().collect();
        assert_eq!(entries.len(), 3);

        // Test range removal
        store.remove_range(b"key1", b"key3").await.unwrap();
        let range = store.iter_range(b"key1", b"key3").await.unwrap();
        let entries: Vec<_> = range.into_iter().collect();
        assert_eq!(entries.len(), 0);
    }
}
