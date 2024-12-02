//! yrs-kvstore is a generic library that allows to quickly implement
//! [Yrs](https://docs.rs/yrs/latest/yrs/index.html) specific document operations over any kind
//! of persistent key-value store i.e. LMDB or RocksDB.
//!
//! In order to do so, persistent unit of transaction should define set of basic operations via
//! [KVStore] trait implementation. Once this is done it can implement [DocOps]. Latter offers a
//! set of useful operations like document metadata management options, document and update merging
//! etc. They are implemented automatically as long struct has correctly implemented [KVStore].
//!
//! ## Internal representation
//!
//! yrs-kvstore operates around few key spaces. All keys inserted via [DocOps] are prefixed with
//! [V1] constant. Later on the key space is further divided into:
//!
//! - [KEYSPACE_OID] used for object ID (OID) index mapping. Whenever the new document is being
//!   inserted, a new OID number is generated for it. While document names can be any kind of strings
//!   OIDs are guaranteed to have constant size. Internally all of the document contents are referred
//!   to via their OID identifiers.
//! - [KEYSPACE_DOC] used to store [document state](crate::keys::SUB_DOC), its
//!   [state vector](crate::keys::SUB_STATE_VEC), corresponding series of
//!   [updates](crate::keys::SUB_UPDATE) and [metadata](crate::keys::SUB_META). Document state and
//!   state vector may not represent full system knowledge about the document, as they don't reflect
//!   information inside document updates. Updates can be stored separately to avoid big document
//!   binary read/parse/merge/store cycles of every update. It's a good idea to insert updates as they
//!   come and every once in a while call [DocOps::flush_doc] or [DocOps::flush_doc_with] to merge
//!   them into document state itself.
//!
//! The variants and schemas of byte keys in use could be summarized as:
//!
//! ```nocompile
//! 00{doc_name:N}0      - OID key pattern
//! 01{oid:4}0           - document key pattern
//! 01{oid:4}1           - state vector key pattern
//! 01{oid:4}2{seqNr:4}0 - document update key pattern
//! 01{oid:4}3{name:M}0  - document meta key pattern
//! ```

pub mod error;
pub mod keys;

use error::Error;
use keys::{
    doc_oid_name, key_doc, key_doc_end, key_doc_start, key_meta, key_meta_end, key_meta_start,
    key_oid, key_state_vector, key_update, Key, KEYSPACE_DOC, KEYSPACE_OID, OID, V1,
};
use std::convert::TryInto;
use tracing::{debug, info};
use yrs::updates::decoder::Decode;
use yrs::updates::encoder::Encode;
use yrs::{Doc, ReadTxn, StateVector, Transact, TransactionMut, Update};

/// A trait to be implemented by the specific key-value store transaction equivalent in order to
/// auto-implement features provided by [DocOps] trait.
pub trait KVStore: Send + Sync {
    /// Error type returned from the implementation.
    type Error: std::error::Error + Send + Sync + 'static;
    /// Cursor type used to iterate over the ordered range of key-value entries.
    type Cursor: Iterator<Item = Self::Entry>;
    /// Entry type returned by cursor.
    type Entry: KVEntry;
    /// Type returned from the implementation.
    type Return: AsRef<[u8]>;

    /// Return a value stored under given `key` or `None` if key was not found.
    async fn get(&self, key: &[u8]) -> Result<Option<Self::Return>, Self::Error>;

    /// Insert a new `value` under given `key` or replace an existing value with new one if
    /// entry with that `key` already existed.
    async fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error>;

    /// Return a value stored under the given `key` if it exists.
    async fn remove(&self, key: &[u8]) -> Result<(), Self::Error>;

    /// Remove all keys between `from`..=`to` range of keys.
    async fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error>;

    /// Return an iterator over all entries between `from`..=`to` range of keys.
    async fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error>;

    /// Looks into the last entry value prior to a given key. The provided key parameter may not
    /// exist and it's used only to establish cursor position in ordered key collection.
    ///
    /// In example: in a key collection of `{1,2,5,7}`, this method with the key parameter of `4`
    /// should return value of `2`.
    async fn peek_back(&self, key: &[u8]) -> Result<Option<Self::Entry>, Self::Error>;
}

/// Trait used by [KVStore] to define key-value entry tuples returned by cursor iterators.
pub trait KVEntry {
    /// Returns a key of current entry.
    fn key(&self) -> &[u8];
    /// Returns a value of current entry.
    fn value(&self) -> &[u8];
}

/// Trait used to automatically implement core operations over the Yrs document.
pub trait DocOps<'a>: KVStore + Sized
where
    Error: From<<Self as KVStore>::Error>,
{
    /// Inserts or updates a document given it's read transaction and name. lib0 v1 encoding is
    /// used for storing the document.
    async fn insert_doc<K: AsRef<[u8]> + ?Sized, T: ReadTxn>(
        &self,
        name: &K,
        txn: &T,
    ) -> Result<(), Error> {
        let doc_state = txn.encode_diff_v1(&StateVector::default());
        let state_vector = txn.state_vector().encode_v1();
        self.insert_doc_raw_v1(name.as_ref(), &doc_state, &state_vector)
            .await
    }

    /// Inserts or updates a document given it's binary update and state vector. lib0 v1 encoding is
    /// assumed as a format for storing the document.
    ///
    /// This is useful when you i.e. want to pre-serialize big document prior to acquiring
    /// a database transaction.
    ///
    /// This feature requires a write capabilities from the database transaction.
    async fn insert_doc_raw_v1(
        &self,
        name: &[u8],
        doc_state_v1: &[u8],
        doc_sv_v1: &[u8],
    ) -> Result<(), Error> {
        let oid = get_or_create_oid(self, name).await?;
        insert_inner_v1(self, oid, doc_state_v1, doc_sv_v1).await?;
        Ok(())
    }

    /// Loads the document state stored in current database under given document `name` into
    /// in-memory Yrs document using provided [TransactionMut]. This includes potential update
    /// entries that may not have been merged with the main document state yet.
    ///
    /// This feature requires only a read capabilities from the database transaction.
    async fn load_doc<'doc, K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
        txn: &mut TransactionMut<'doc>,
    ) -> Result<bool, Error> {
        if let Some(oid) = get_oid(self, name.as_ref()).await? {
            let loaded = load_doc(self, oid, txn).await?;
            Ok(loaded != 0)
        } else {
            Ok(false)
        }
    }

    /// Merges all updates stored via [Self::push_update] that were detached from the main document
    /// state, updates the document and its state vector and finally prunes the updates that have
    /// been integrated this way. Returns the [Doc] with the most recent state produced this way.
    ///
    /// This feature requires a write capabilities from the database transaction.
    async fn flush_doc<K: AsRef<[u8]> + ?Sized>(&self, name: &K) -> Result<Option<Doc>, Error> {
        self.flush_doc_with(name, yrs::Options::default()).await
    }

    /// Merges all updates stored via [Self::push_update] that were detached from the main document
    /// state, updates the document and its state vector and finally prunes the updates that have
    /// been integrated this way. `options` are used to drive the details of integration process.
    /// Returns the [Doc] with the most recent state produced this way, initialized using
    /// `options` parameter.
    ///
    /// This feature requires a write capabilities from the database transaction.
    async fn flush_doc_with<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
        options: yrs::Options,
    ) -> Result<Option<Doc>, Error> {
        if let Some(oid) = get_oid(self, name.as_ref()).await? {
            let doc = flush_doc(self, oid, options).await?;
            Ok(doc)
        } else {
            Ok(None)
        }
    }

    /// Returns the [StateVector] stored directly for the document with a given `name`.
    /// Returns `None` if the state vector was not stored.
    ///
    /// Keep in mind that this method only returns a state vector that's stored directly. A second
    /// tuple parameter boolean informs if returned value is up to date. If that's not the case, it
    /// means that state vector exists but must be recalculated from the collection of persisted
    /// updates using either [Self::load_doc] (read-only) or [Self::flush_doc] (read-write).
    ///
    /// This feature requires only the read capabilities from the database transaction.
    async fn get_state_vector<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
    ) -> Result<(Option<StateVector>, bool), Error> {
        if let Some(oid) = get_oid(self, name.as_ref()).await? {
            let key = key_state_vector(oid);
            let data = self.get(&key).await?;
            let sv = if let Some(data) = data {
                let state_vector = StateVector::decode_v1(data.as_ref())?;
                Some(state_vector)
            } else {
                None
            };
            let update_range_start = key_update(oid, 0);
            let update_range_end = key_update(oid, u32::MAX);
            let mut iter = self
                .iter_range(&update_range_start, &update_range_end)
                .await?;
            let up_to_date = iter.next().is_none();
            Ok((sv, up_to_date))
        } else {
            Ok((None, true))
        }
    }

    /// Appends new update without integrating it directly into document store (which is faster
    /// than persisting full document state on every update). Updates are assumed to be serialized
    /// using lib0 v1 encoding.
    ///
    /// Returns a sequence number of a stored update. Once updates are integrated into document and
    /// pruned (using [Self::flush_doc] method), sequence number is reset.
    ///
    /// This feature requires a write capabilities from the database transaction.
    async fn push_update<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
        update: &[u8],
    ) -> Result<u32, Error> {
        let oid = get_or_create_oid(self, name.as_ref()).await?;
        let last_clock = {
            let end = key_update(oid, u32::MAX);
            if let Some(e) = self.peek_back(&end).await? {
                let last_key = e.key();
                let len = last_key.len();
                let last_clock = &last_key[(len - 5)..(len - 1)]; // update key scheme: 01{name:n}1{clock:4}0
                u32::from_be_bytes(last_clock.try_into().unwrap())
            } else {
                0
            }
        };
        let clock = last_clock + 1;
        let update_key = key_update(oid, clock);
        self.upsert(&update_key, &update).await?;
        Ok(clock)
    }

    /// Returns an update (encoded using lib0 v1 encoding) which contains all new changes that
    /// happened since provided state vector for a given document.
    ///
    /// This feature requires only the read capabilities from the database transaction.
    async fn get_diff<K: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K,
        sv: &StateVector,
    ) -> Result<Option<Vec<u8>>, Error> {
        let doc = Doc::new();
        let found = {
            let mut txn = doc.transact_mut();
            self.load_doc(name, &mut txn).await?
        };
        if found {
            Ok(Some(doc.transact().encode_diff_v1(sv)))
        } else {
            Ok(None)
        }
    }

    /// Removes all data associated with the current document (including its updates and metadata).
    ///
    /// This feature requires a write capabilities from the database transaction.
    async fn clear_doc<K: AsRef<[u8]> + ?Sized>(&self, name: &K) -> Result<(), Error> {
        let oid_key = key_oid(name.as_ref());
        if let Some(oid) = self.get(&oid_key).await? {
            // all document related elements are stored within bounds [0,1,..oid,0]..[0,1,..oid,255]
            let oid: [u8; 4] = oid.as_ref().try_into().unwrap();
            let oid = OID::from_be_bytes(oid);
            self.remove(&oid_key).await?;
            let start = key_doc_start(oid);
            let end = key_doc_end(oid);
            for v in self.iter_range(&start, &end).await? {
                let key: &[u8] = v.key();
                if key > end.as_ref() {
                    break; //TODO: for some reason key range doesn't always work
                }
                self.remove(&key).await?;
            }
        }
        Ok(())
    }

    /// Returns a metadata value stored under its metadata `key` for a document with given `name`.
    ///
    /// This feature requires only the read capabilities from the database transaction.
    async fn get_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K1,
        meta_key: &K2,
    ) -> Result<Option<Self::Return>, Error> {
        if let Some(oid) = get_oid(self, name.as_ref()).await? {
            let key = key_meta(oid, meta_key.as_ref());
            Ok(self.get(&key).await?)
        } else {
            Ok(None)
        }
    }

    /// Inserts or updates new `meta` value stored under its metadata `key` for a document with
    /// given `name`.
    ///
    /// This feature requires write capabilities from the database transaction.
    async fn insert_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K1,
        meta_key: &K2,
        meta: &[u8],
    ) -> Result<(), Error> {
        let oid = get_or_create_oid(self, name.as_ref()).await?;
        let key = key_meta(oid, meta_key.as_ref());
        self.upsert(&key, meta).await?;
        Ok(())
    }

    /// Removes an metadata entry stored under given metadata `key` for a document with provided `name`.
    ///
    /// This feature requires write capabilities from the database transaction.
    async fn remove_meta<K1: AsRef<[u8]> + ?Sized, K2: AsRef<[u8]> + ?Sized>(
        &self,
        name: &K1,
        meta_key: &K2,
    ) -> Result<(), Error> {
        if let Some(oid) = get_oid(self, name.as_ref()).await? {
            let key = key_meta(oid, meta_key.as_ref());
            self.remove(&key).await?;
        }
        Ok(())
    }

    /// Returns an iterator over all document names stored in current database.
    async fn iter_docs(&self) -> Result<DocsNameIter<Self::Cursor, Self::Entry>, Error> {
        let start = Key::from_const([V1, KEYSPACE_OID]);
        let end = Key::from_const([V1, KEYSPACE_DOC]);
        let cursor = self.iter_range(&start, &end).await?;
        Ok(DocsNameIter { cursor })
    }

    /// Returns an iterator over all metadata entries stored for a given document.
    async fn iter_meta<K: AsRef<[u8]> + ?Sized>(
        &self,
        doc_name: &K,
    ) -> Result<MetadataIter<Self::Cursor, Self::Entry>, Error> {
        if let Some(oid) = get_oid(self, doc_name.as_ref()).await? {
            let start = key_meta_start(oid).to_vec();
            let end = key_meta_end(oid).to_vec();
            let cursor = self.iter_range(&start, &end).await?;
            Ok(MetadataIter(Some((cursor, start, end))))
        } else {
            Ok(MetadataIter(None))
        }
    }
}

async fn get_oid<'a, DB: DocOps<'a> + ?Sized>(db: &DB, name: &[u8]) -> Result<Option<OID>, Error>
where
    Error: From<<DB as KVStore>::Error>,
{
    let key = key_oid(name);
    let value = db.get(&key).await?;
    if let Some(value) = value {
        let bytes: [u8; 4] = value.as_ref().try_into().unwrap();
        let oid = OID::from_be_bytes(bytes);
        Ok(Some(oid))
    } else {
        Ok(None)
    }
}

async fn get_or_create_oid<'a, DB: DocOps<'a> + ?Sized>(db: &DB, name: &[u8]) -> Result<OID, Error>
where
    Error: From<<DB as KVStore>::Error>,
{
    if let Some(oid) = get_oid(db, name).await? {
        Ok(oid)
    } else {
        let last_oid = if let Some(e) = db.peek_back([V1, KEYSPACE_DOC].as_ref()).await? {
            let value = e.value();
            let last_value = OID::from_be_bytes(value.try_into().unwrap());
            last_value
        } else {
            0
        };
        let new_oid = last_oid + 1;
        let key = key_oid(name);
        db.upsert(&key, new_oid.to_be_bytes().as_ref()).await?;
        Ok(new_oid)
    }
}

async fn load_doc<'doc, 'a, DB: DocOps<'a> + ?Sized>(
    db: &DB,
    oid: OID,
    txn: &mut TransactionMut<'doc>,
) -> Result<u32, Error>
where
    Error: From<<DB as KVStore>::Error>,
{
    let mut found = false;
    {
        let doc_key = key_doc(oid);
        if let Some(doc_state) = db.get(&doc_key).await? {
            let update = Update::decode_v1(doc_state.as_ref())?;
            txn.apply_update(update);
            found = true;
        }
    }
    let mut update_count = 0;
    {
        let update_key_start = key_update(oid, 0);
        let update_key_end = key_update(oid, u32::MAX);
        let mut iter = db.iter_range(&update_key_start, &update_key_end).await?;
        while let Some(e) = iter.next() {
            let value = e.value();
            let update = Update::decode_v1(value)?;
            txn.apply_update(update);
            update_count += 1;
        }
    }
    if found {
        update_count |= 1 << 31;
    }
    Ok(update_count)
}

async fn delete_updates<'a, DB: DocOps<'a> + ?Sized>(db: &DB, oid: OID) -> Result<(), Error>
where
    Error: From<<DB as KVStore>::Error>,
{
    let start = key_update(oid, 0);
    let end = key_update(oid, u32::MAX);
    db.remove_range(&start, &end).await?;
    Ok(())
}

async fn flush_doc<'a, DB: DocOps<'a> + ?Sized>(
    db: &DB,
    oid: OID,
    options: yrs::Options,
) -> Result<Option<Doc>, Error>
where
    Error: From<<DB as KVStore>::Error>,
{
    let doc = Doc::with_options(options);
    let found = load_doc(db, oid, &mut doc.transact_mut()).await?;
    if found & !(1 << 31) != 0 {
        let txn = doc.transact();
        let doc_state = txn.encode_state_as_update_v1(&StateVector::default());
        let state_vec = txn.state_vector().encode_v1();
        drop(txn);

        insert_inner_v1(db, oid, &doc_state, &state_vec).await?;
        delete_updates(db, oid).await?;
        Ok(Some(doc))
    } else {
        Ok(None)
    }
}

async fn insert_inner_v1<'a, DB: DocOps<'a> + ?Sized>(
    db: &DB,
    oid: OID,
    doc_state_v1: &[u8],
    doc_sv_v1: &[u8],
) -> Result<(), Error>
where
    error::Error: From<<DB as KVStore>::Error>,
{
    let key_doc = key_doc(oid);
    let key_sv = key_state_vector(oid);
    db.upsert(&key_doc, doc_state_v1).await?;
    db.upsert(&key_sv, doc_sv_v1).await?;
    Ok(())
}

pub struct DocsNameIter<I, E>
where
    I: Iterator<Item = E>,
    E: KVEntry,
{
    cursor: I,
}

impl<I, E> Iterator for DocsNameIter<I, E>
where
    I: Iterator<Item = E>,
    E: KVEntry,
{
    type Item = Box<[u8]>;

    fn next(&mut self) -> Option<Self::Item> {
        let e = self.cursor.next()?;
        Some(doc_oid_name(e.key()).into())
    }
}

pub struct MetadataIter<I, E>(Option<(I, Vec<u8>, Vec<u8>)>)
where
    I: Iterator<Item = E>,
    E: KVEntry;

impl<I, E> Iterator for MetadataIter<I, E>
where
    I: Iterator<Item = E>,
    E: KVEntry,
{
    type Item = (Box<[u8]>, Box<[u8]>);

    fn next(&mut self) -> Option<Self::Item> {
        let (cursor, _, _) = self.0.as_mut()?;
        let v = cursor.next()?;
        let key = v.key();
        let value = v.value();
        let meta_key = &key[7..key.len() - 1];
        Some((meta_key.into(), value.into()))
    }
}
