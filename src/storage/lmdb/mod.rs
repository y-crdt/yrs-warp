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

    fn get(&self, key: &[u8]) -> Result<Option<Self::Return>, Self::Error> {
        let value = self.0.get(&key).optional()?;
        Ok(value)
    }

    fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.0.set(&key, &value)?;
        Ok(())
    }

    fn remove(&self, key: &[u8]) -> Result<(), Self::Error> {
        let prev: Option<&[u8]> = self.0.get(&key).optional()?;
        if prev.is_some() {
            self.0.del(&key)?;
        }
        Ok(())
    }

    fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error> {
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

    fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error> {
        let from = from.to_vec();
        let to = to.to_vec();
        let cursor = unsafe { std::mem::transmute(self.0.keyrange(&from, &to)?) };
        Ok(LmdbRange { from, to, cursor })
    }

    fn peek_back(&self, key: &[u8]) -> Result<Option<Self::Entry>, Self::Error> {
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
    use yrs::{Doc, GetString, ReadTxn, Text, Transact};

    fn init_env<P: AsRef<Path>>(dir: P) -> Environment {
        let env = Environment::new()
            .autocreate_dir(true)
            .max_dbs(4)
            .open(dir, 0o777)
            .unwrap();
        env
    }

    #[test]
    fn create_get_remove() {
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
            db.insert_doc("doc", &txn).unwrap();
            db_txn.commit().unwrap();
        }

        // retrieve document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            let db_txn = env.get_reader().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));
            db.load_doc("doc", &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "hello");

            let (sv, completed) = db.get_state_vector("doc").unwrap();
            assert_eq!(sv, Some(txn.state_vector()));
            assert!(completed);
        }

        // remove document
        {
            let db_txn = env.new_transaction().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));

            db.clear_doc("doc").unwrap();

            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            db.load_doc("doc", &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "");

            let (sv, completed) = db.get_state_vector("doc").unwrap();
            assert!(sv.is_none());
            assert!(completed);
        }
    }
    #[test]
    fn multi_insert() {
        let dir = TempDir::new("lmdb-multi_insert").unwrap();
        let env = init_env(&dir);
        let h = env.create_db("yrs", DbCreate).unwrap();

        // insert document twice
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, "hello");

            let db_txn = env.new_transaction().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));

            db.insert_doc("doc", &txn).unwrap();

            text.push(&mut txn, " world");

            db.insert_doc("doc", &txn).unwrap();

            db_txn.commit().unwrap();
        }

        // retrieve document
        {
            let db_txn = env.get_reader().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));

            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            db.load_doc("doc", &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "hello world");
        }
    }

    #[test]
    fn incremental_updates() {
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
            let _sub = doc.observe_update_v1(move |_, u| {
                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                db.push_update(DOC_NAME, &u.update).unwrap();
                db_txn.commit().unwrap();
            });
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            text.push(&mut doc.transact_mut(), "c");
        }

        // load document
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();

            let db_txn = env.get_reader().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));
            db.load_doc(DOC_NAME, &mut txn).unwrap();

            assert_eq!(text.get_string(&txn), "abc");
        }

        // flush document
        {
            let db_txn = env.new_transaction().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));
            let doc = db.flush_doc(DOC_NAME).unwrap().unwrap();
            db_txn.commit().unwrap();

            let text = doc.get_or_insert_text("text");

            assert_eq!(text.get_string(&doc.transact()), "abc");
        }
    }

    #[test]
    fn state_vector_updates_only() {
        const DOC_NAME: &str = "doc";
        let dir = TempDir::new("lmdb-state_vector_updates_only").unwrap();
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
            let _sub = doc.observe_update_v1(move |_, u| {
                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                db.push_update(DOC_NAME, &u.update).unwrap();
                db_txn.commit().unwrap();
            });
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            text.push(&mut doc.transact_mut(), "c");

            let sv = doc.transact().state_vector();
            sv
        };

        let db_txn = env.get_reader().unwrap();
        let db = LmdbStore::from(db_txn.bind(&h));
        let (sv, completed) = db.get_state_vector(DOC_NAME).unwrap();
        assert!(sv.is_none());
        assert!(!completed); // since it's not completed, we should recalculate state vector from doc state
    }

    #[test]
    fn state_diff_from_updates() {
        const DOC_NAME: &str = "doc";
        let dir = TempDir::new("lmdb-state_diff_from_updates").unwrap();
        let env = init_env(&dir);
        let h = env.create_db("yrs", DbCreate).unwrap();
        let env = Arc::new(env);
        let h = Arc::new(h);

        let (sv, expected) = {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");

            let env = env.clone();
            let h = h.clone();
            let _sub = doc.observe_update_v1(move |_, u| {
                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                db.push_update(DOC_NAME, &u.update).unwrap();
                db_txn.commit().unwrap();
            });

            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            let sv = doc.transact().state_vector();
            text.push(&mut doc.transact_mut(), "c");
            let update = doc.transact().encode_diff_v1(&sv);
            (sv, update)
        };

        let db_txn = env.get_reader().unwrap();
        let db = LmdbStore::from(db_txn.bind(&h));
        let actual = db.get_diff(DOC_NAME, &sv).unwrap();
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn state_diff_from_doc() {
        const DOC_NAME: &str = "doc";
        let dir = TempDir::new("lmdb-state_diff_from_doc").unwrap();
        let env = init_env(&dir);
        let h = env.create_db("yrs", DbCreate).unwrap();

        let (sv, expected) = {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            // generate 3 updates
            text.push(&mut doc.transact_mut(), "a");
            text.push(&mut doc.transact_mut(), "b");
            let sv = doc.transact().state_vector();
            text.push(&mut doc.transact_mut(), "c");
            let update = doc.transact().encode_diff_v1(&sv);

            let db_txn = env.new_transaction().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));
            db.insert_doc(DOC_NAME, &doc.transact()).unwrap();
            db_txn.commit().unwrap();

            (sv, update)
        };

        let db_txn = env.get_reader().unwrap();
        let db = LmdbStore::from(db_txn.bind(&h));
        let actual = db.get_diff(DOC_NAME, &sv).unwrap();
        assert_eq!(actual, Some(expected));
    }

    #[test]
    fn doc_meta() {
        const DOC_NAME: &str = "doc";
        let dir = TempDir::new("lmdb-doc_meta").unwrap();
        let env = init_env(&dir);
        let h = env.create_db("yrs", DbCreate).unwrap();

        let db_txn = env.new_transaction().unwrap();
        let db = LmdbStore::from(db_txn.bind(&h));
        let value = db.get_meta(DOC_NAME, "key").unwrap();
        assert!(value.is_none());
        db.insert_meta(DOC_NAME, "key", "value1".as_bytes())
            .unwrap();
        db_txn.commit().unwrap();

        let db_txn = env.new_transaction().unwrap();
        let db = LmdbStore::from(db_txn.bind(&h));
        let prev = db.get_meta(DOC_NAME, "key").unwrap().map(Vec::from);
        db.insert_meta(DOC_NAME, "key", "value2".as_bytes())
            .unwrap();
        db_txn.commit().unwrap();
        assert_eq!(prev.as_deref(), Some("value1".as_bytes()));

        let db_txn = env.new_transaction().unwrap();
        let db = LmdbStore::from(db_txn.bind(&h));
        let prev = db.get_meta(DOC_NAME, "key").unwrap().map(Vec::from);
        db.remove_meta(DOC_NAME, "key").unwrap();
        assert_eq!(prev.as_deref(), Some("value2".as_bytes()));
        let value = db.get_meta(DOC_NAME, "key").unwrap();
        assert!(value.is_none());
    }

    #[test]
    fn doc_meta_iter() {
        let dir = TempDir::new("lmdb-doc_meta_iter").unwrap();
        let env = init_env(&dir);
        let h = env.create_db("yrs", DbCreate).unwrap();
        let db_txn = env.new_transaction().unwrap();
        let db = LmdbStore::from(db_txn.bind(&h));

        db.insert_meta("A", "key1", [1].as_ref()).unwrap();
        db.insert_meta("B", "key2", [2].as_ref()).unwrap();
        db.insert_meta("B", "key3", [3].as_ref()).unwrap();
        db.insert_meta("C", "key4", [4].as_ref()).unwrap();

        let mut i = db.iter_meta("B").unwrap();
        assert_eq!(i.next(), Some(("key2".as_bytes().into(), [2].into())));
        assert_eq!(i.next(), Some(("key3".as_bytes().into(), [3].into())));
        assert!(i.next().is_none());
    }

    #[test]
    fn doc_iter() {
        let dir = TempDir::new("lmdb-doc_iter").unwrap();
        let env = init_env(&dir);
        let h = env.create_db("yrs", DbCreate).unwrap();
        let env = Arc::new(env);
        let h = Arc::new(h);

        // insert metadata
        {
            let db_txn = env.new_transaction().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));
            db.insert_meta("A", "key1", [1].as_ref()).unwrap();
            db_txn.commit().unwrap();
        }

        // insert full doc state
        {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, "hello world");
            let db_txn = env.new_transaction().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));
            db.insert_doc("B", &txn).unwrap();
            db_txn.commit().unwrap();
        }

        // insert update
        {
            let doc = Doc::new();
            let env = env.clone();
            let h = h.clone();
            let _sub = doc.observe_update_v1(move |_, u| {
                let db_txn = env.new_transaction().unwrap();
                let db = LmdbStore::from(db_txn.bind(&h));
                db.push_update("C", &u.update).unwrap();
                db_txn.commit().unwrap();
            });
            let text = doc.get_or_insert_text("text");
            let mut txn = doc.transact_mut();
            text.push(&mut txn, "hello world");
        }

        {
            let db_txn = env.get_reader().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));
            let mut i = db.iter_docs().unwrap();
            assert_eq!(i.next(), Some("A".as_bytes().into()));
            assert_eq!(i.next(), Some("B".as_bytes().into()));
            assert_eq!(i.next(), Some("C".as_bytes().into()));
            assert!(i.next().is_none());
        }

        // clear doc
        {
            let db_txn = env.new_transaction().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));
            db.clear_doc("B").unwrap();
            db_txn.commit().unwrap();
        }

        {
            let db_txn = env.get_reader().unwrap();
            let db = LmdbStore::from(db_txn.bind(&h));
            let mut i = db.iter_docs().unwrap();
            assert_eq!(i.next(), Some("A".as_bytes().into()));
            assert_eq!(i.next(), Some("C".as_bytes().into()));
            assert!(i.next().is_none());
        }
    }
}
