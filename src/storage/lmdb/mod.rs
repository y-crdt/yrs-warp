//! **yrs-lmdb** is a persistence layer allowing to store [Yrs](https://docs.rs/yrs/latest/yrs/index.html)
//! documents and providing convenient utility functions to work with them, using LMDB for persistent
//! backend.
//!
//! # Example
//!
//! ```rust
//! use std::sync::Arc;
//! use lmdb_rs::core::DbCreate;
//! use lmdb_rs::Environment;
//! use yrs::{Doc, Text, Transact};
//! use super::kv::DocOps;
//! use super::kv::LmdbStore;
//!
//! let env = Arc::new(Environment::new()
//!     .autocreate_dir(true)
//!     .max_dbs(4)
//!     .open("my-lmdb-dir", 0o777)
//!     .unwrap());
//! let h = Arc::new(env.create_db("yrs", DbCreate).unwrap());
//!
//! let doc = Doc::new();
//! let text = doc.get_or_insert_text("text");
//!
//! // restore document state from DB
//! {
//!   let db_txn = env.get_reader().unwrap();
//!   let db = LmdbStore::from(db_txn.bind(&h));
//!   db.load_doc("my-doc-name", &mut doc.transact_mut()).unwrap();
//! }
//!
//! // another options is to flush document state right away, but
//! // this requires a read-write transaction
//! {
//!   let db_txn = env.new_transaction().unwrap();
//!   let db = LmdbStore::from(db_txn.bind(&h));
//!   let doc = db.flush_doc_with("my-doc-name", yrs::Options::default()).unwrap();
//!   db_txn.commit().unwrap(); // flush may modify internal store state
//! }
//!
//! // configure document to persist every update and
//! // occassionaly compact them into document state
//! let sub = {
//!   let env = env.clone();
//!   let h = h.clone();
//!   let options = doc.options().clone();
//!   doc.observe_update_v1(move |_,e| {
//!       let db_txn = env.new_transaction().unwrap();
//!       let db = LmdbStore::from(db_txn.bind(&h));
//!       let seq_nr = db.push_update("my-doc-name", &e.update).unwrap();
//!       if seq_nr % 64 == 0 {
//!           // occassinally merge updates into the document state
//!           db.flush_doc_with("my-doc-name", options.clone()).unwrap();
//!       }
//!       db_txn.commit().unwrap();
//!   })
//! };
//!
//! text.insert(&mut doc.transact_mut(), 0, "a");
//! text.insert(&mut doc.transact_mut(), 1, "b");
//! text.insert(&mut doc.transact_mut(), 2, "c");
//! ```

use lmdb_rs::core::{CursorIterator, MdbResult};
use lmdb_rs::{CursorKeyRangeIter, Database, MdbError, ReadonlyTransaction};
use std::ops::Deref;
use log::{debug, info};

pub use super::kv as store;
use super::kv::error::Error;
use super::kv::keys::Key;
use super::kv::{DocOps, KVEntry, KVStore};

trait OptionalNotFound {
    type Return;
    type Error;

    fn optional(self) -> Result<Option<Self::Return>, Self::Error>;
}

impl<T> OptionalNotFound for MdbResult<T> {
    type Return = T;
    type Error = MdbError;

    /// Changes [MdbError::NotFound] onto [None] case.
    fn optional(self) -> Result<Option<Self::Return>, Self::Error> {
        match self {
            Ok(value) => Ok(Some(value)),
            Err(MdbError::NotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }
}

/// Type wrapper around LMDB's [Database] struct. Used to extend LMDB transactions with [DocOps]
/// methods used for convenience when working with Yrs documents.
#[repr(transparent)]
#[derive(Debug)]
pub struct LmdbStore<'db>(Database<'db>);

impl<'db> From<Database<'db>> for LmdbStore<'db> {
    #[inline(always)]
    fn from(db: Database<'db>) -> Self {
        LmdbStore(db)
    }
}

impl<'db> Into<Database<'db>> for LmdbStore<'db> {
    #[inline(always)]
    fn into(self) -> Database<'db> {
        self.0
    }
}

impl<'db> Deref for LmdbStore<'db> {
    type Target = Database<'db>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'db> DocOps<'db> for LmdbStore<'db> {}

impl<'db> KVStore<'db> for LmdbStore<'db> {
    type Error = MdbError;
    type Cursor = LmdbRange<'db>;
    type Entry = LmdbEntry<'db>;
    type Return = &'db [u8];

    async fn get(&self, key: &[u8]) -> Result<Option<Self::Return>, Self::Error> {
        let value = self.0.get(&key).optional()?;
        Ok(value)
    }

    async fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.0.set(&key, &value)?;
        Ok(())
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Self::Error> {
        let prev: Option<&[u8]> = self.0.get(&key).optional()?;
        if prev.is_some() {
            self.0.del(&key)?;
        }
        Ok(())
    }

    async fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error> {
        let mut c = self.0.new_cursor()?;
        if c.to_gte_key(&from).optional()?.is_some() {
            while c.get_key::<&[u8]>()? <= to {
                c.del()?;
                if c.to_next_key().optional()?.is_none() {
                    break;
                }
            }
        }
        Ok(())
    }

    async fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error> {
        let from = from.to_vec();
        let to = to.to_vec();
        let cursor = unsafe { std::mem::transmute(self.0.keyrange(&from, &to)?) };
        Ok(LmdbRange { from, to, cursor })
    }

    async fn peek_back(&self, key: &[u8]) -> Result<Option<Self::Entry>, Self::Error> {
        let mut cursor = self.0.new_cursor()?;
        cursor.to_gte_key(&key).optional()?;
        if cursor.to_prev_key().optional()?.is_none() {
            return Ok(None);
        }
        let key = cursor.get_key()?;
        let value = cursor.get_value()?;
        Ok(Some(LmdbEntry::new(key, value)))
    }
}

pub struct LmdbRange<'a> {
    from: Vec<u8>,
    to: Vec<u8>,
    cursor: CursorIterator<'a, CursorKeyRangeIter<'a>>,
}

impl<'a> Iterator for LmdbRange<'a> {
    type Item = LmdbEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let (key, value) = self.cursor.next()?.get();
        Some(LmdbEntry::new(key, value))
    }
}

pub struct LmdbEntry<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> LmdbEntry<'a> {
    fn new(key: &'a [u8], value: &'a [u8]) -> Self {
        LmdbEntry { key, value }
    }
}

impl<'a> KVEntry for LmdbEntry<'a> {
    fn key(&self) -> &[u8] {
        self.key
    }
    fn value(&self) -> &[u8] {
        self.value
    }
}

pub(crate) struct OwnedCursorRange<'a> {
    txn: ReadonlyTransaction<'a>,
    db: Database<'a>,
    cursor: CursorIterator<'a, CursorKeyRangeIter<'a>>,
    start: Vec<u8>,
    end: Vec<u8>,
}

impl<'a> OwnedCursorRange<'a> {
    pub(crate) fn new<const N: usize>(
        txn: ReadonlyTransaction<'a>,
        db: Database<'a>,
        start: Key<N>,
        end: Key<N>,
    ) -> Result<Self, Error> {
        let start = start.into();
        let end = end.into();
        let cursor = unsafe { std::mem::transmute(db.keyrange(&start, &end)?) };

        Ok(OwnedCursorRange {
            txn,
            db,
            cursor,
            start,
            end,
        })
    }

    pub(crate) fn db(&self) -> &Database {
        &self.db
    }
}

impl<'a> Iterator for OwnedCursorRange<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        let v = self.cursor.next()?;
        Some(v.get())
    }
}

#[cfg(test)]
mod test {
    use super::{DocOps, LmdbStore};
    use lmdb_rs::core::DbCreate;
    use lmdb_rs::Environment;
    use std::path::Path;
    use std::sync::Arc;
    use tempdir::TempDir;
    use tokio::{runtime::Runtime, sync::futures};
    use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact};

    fn init_env<P: AsRef<Path>>(dir: P) -> Environment {
        Environment::new()
            .autocreate_dir(true)
            .max_dbs(4)
            .open(dir, 0o777)
            .unwrap()
    }

    #[test]
    fn create_get_remove() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let dir = TempDir::new("lmdb-create_get_remove").unwrap();
            let env = init_env(&dir);
            let h = env.create_db("yrs", DbCreate).unwrap();

            // insert document
            {
                let doc = Doc::new();
                let text = doc.get_or_insert_text("text");
                let mut txn = doc.transact_mut();
                text.insert(&mut txn, 0, "hello");

                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                db.insert_doc("doc", &txn).await.unwrap();
                db_txn.commit().unwrap();
            }

            // retrieve document
            {
                let doc = Doc::new();
                let text = doc.get_or_insert_text("text");
                let mut txn = doc.transact_mut();
                let db_txn = env.get_reader().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                db.load_doc("doc", &mut txn).await.unwrap();

                assert_eq!(text.get_string(&txn), "hello");

                let (sv, completed) = db.get_state_vector("doc").await.unwrap();
                assert_eq!(sv, Some(txn.state_vector()));
                assert!(completed);
            }

            // remove document
            {
                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));

                db.clear_doc("doc").await.unwrap();

                let doc = Doc::new();
                let text = doc.get_or_insert_text("text");
                let mut txn = doc.transact_mut();
                db.load_doc("doc", &mut txn).await.unwrap();

                assert_eq!(text.get_string(&txn), "");

                let (sv, completed) = db.get_state_vector("doc").await.unwrap();
                assert!(sv.is_none());
                assert!(completed);
            }
        });
    }

    #[test]
    fn incremental_updates() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            const DOC_NAME: &str = "doc";
            let dir = TempDir::new("lmdb-incremental_updates").unwrap();
            let env = init_env(&dir);
            let h = env.create_db("yrs", DbCreate).unwrap();
            let env = Arc::new(env);
            let h = Arc::new(h);

            // store document updates
            {
                let doc = Doc::new();
                let text = doc.get_or_insert_text("text");
                let env = env.clone();
                let h = h.clone();

                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

                let _sub = doc.observe_update_v1(move |_, u| {
                    let update = u.update.to_vec();
                    let _ = tx.send(update);
                });

                // Execute document modifications
                text.push(&mut doc.transact_mut(), "a");
                text.push(&mut doc.transact_mut(), "b");
                text.push(&mut doc.transact_mut(), "c");

                // Process updates
                while let Some(update) = rx.recv().await {
                    let db_txn = env.new_transaction().unwrap();
                    let db = LmdbStore::from(db_txn.bind(&h));
                    db.push_update(DOC_NAME, &update).await.unwrap();
                    db_txn.commit().unwrap();
                }
            }

            // load document
            {
                let doc = Doc::new();
                let text = doc.get_or_insert_text("text");
                let mut txn = doc.transact_mut();

                let db_txn = env.get_reader().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                db.load_doc(DOC_NAME, &mut txn).await.unwrap();

                assert_eq!(text.get_string(&txn), "abc");
            }

            // flush document
            {
                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                let doc = db.flush_doc(DOC_NAME).await.unwrap().unwrap();
                db_txn.commit().unwrap();

                let text = doc.get_or_insert_text("text");
                assert_eq!(text.get_string(&doc.transact()), "abc");
            }
        });
    }

    #[test]
    fn metadata() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let dir = TempDir::new("lmdb-metadata").unwrap();
            let env = init_env(&dir);
            let h = env.create_db("yrs", DbCreate).unwrap();

            // insert metadata
            {
                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                db.insert_meta("doc", "key1", b"value1").await.unwrap();
                db.insert_meta("doc", "key2", b"value2").await.unwrap();
                db_txn.commit().unwrap();
            }

            // get metadata
            {
                let db_txn = env.get_reader().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                let value = db.get_meta("doc", "key1").await.unwrap();
                assert_eq!(value.map(|v| v.as_ref()), Some(b"value1".as_ref()));
            }

            // iterate metadata
            {
                let db_txn = env.get_reader().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                let mut iter = db.iter_meta("doc").await.unwrap();
                let mut entries = Vec::new();
                while let Some((key, value)) = iter.next() {
                    entries.push((key.to_vec(), value.to_vec()));
                }
                entries.sort_by(|a, b| a.0.cmp(&b.0));
                assert_eq!(entries.len(), 2);
                assert_eq!(&entries[0].0, b"key1");
                assert_eq!(&entries[0].1, b"value1");
                assert_eq!(&entries[1].0, b"key2");
                assert_eq!(&entries[1].1, b"value2");
            }

            // remove metadata
            {
                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                db.remove_meta("doc", "key1").await.unwrap();
                db_txn.commit().unwrap();
            }

            {
                let db_txn = env.get_reader().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                let value = db.get_meta("doc", "key1").await.unwrap();
                assert_eq!(value, None);
            }
        });
    }

    #[test]
    fn document_iteration() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let dir = TempDir::new("lmdb-doc_iteration").unwrap();
            let env = init_env(&dir);
            let h = env.create_db("yrs", DbCreate).unwrap();

            // insert documents
            {
                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));

                let doc = Doc::new();
                let text = doc.get_or_insert_text("text");
                let mut txn = doc.transact_mut();
                text.insert(&mut txn, 0, "A");
                db.insert_doc("doc1", &txn).await.unwrap();

                let doc = Doc::new();
                let text = doc.get_or_insert_text("text");
                let mut txn = doc.transact_mut();
                text.insert(&mut txn, 0, "B");
                db.insert_doc("doc2", &txn).await.unwrap();

                db_txn.commit().unwrap();
            }

            // iterate documents
            {
                let db_txn = env.get_reader().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                let mut iter = db.iter_docs().await.unwrap();
                let mut docs = Vec::new();
                while let Some(doc_name) = iter.next() {
                    docs.push(doc_name.to_vec());
                }
                docs.sort();
                assert_eq!(docs.len(), 2);
                assert_eq!(&docs[0], b"doc1");
                assert_eq!(&docs[1], b"doc2");
            }
        });
    }

    #[test]
    fn state_vector() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let dir = TempDir::new("lmdb-state_vector").unwrap();
            let env = init_env(&dir);
            let h = env.create_db("yrs", DbCreate).unwrap();

            // insert document
            {
                let doc = Doc::new();
                let text = doc.get_or_insert_text("text");
                let mut txn = doc.transact_mut();
                text.insert(&mut txn, 0, "hello");

                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                db.insert_doc("doc", &txn).await.unwrap();
                db_txn.commit().unwrap();
            }

            // check state vector
            {
                let db_txn = env.get_reader().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                let (sv, completed) = db.get_state_vector("doc").await.unwrap();
                assert!(sv.is_some());
                assert!(completed);
            }

            // get diff
            {
                let db_txn = env.get_reader().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                let diff = db.get_diff("doc", &StateVector::default()).await.unwrap();
                assert!(diff.is_some());
            }
        });
    }

    #[test]
    fn non_existent_doc() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let dir = TempDir::new("lmdb-non_existent").unwrap();
            let env = init_env(&dir);
            let h = env.create_db("yrs", DbCreate).unwrap();

            let db_txn = env.get_reader().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));

            // load non-existent document
            {
                let doc = Doc::new();
                let mut txn = doc.transact_mut();
                let loaded = db.load_doc("non-existent", &mut txn).await.unwrap();
                assert!(!loaded);
            }

            // get state vector of non-existent document
            {
                let (sv, completed) = db.get_state_vector("non-existent").await.unwrap();
                assert!(sv.is_none());
                assert!(completed);
            }

            // get diff of non-existent document
            {
                let diff = db
                    .get_diff("non-existent", &StateVector::default())
                    .await
                    .unwrap();
                assert!(diff.is_none());
            }

            // get metadata of non-existent document
            {
                let meta = db.get_meta("non-existent", "key").await.unwrap();
                assert!(meta.is_none());
            }
        });
    }
}
