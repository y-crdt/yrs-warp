use super::kv::{DocOps, KVEntry, KVStore};
use r2d2;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, OpenFlags};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SqliteError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("Pool error: {0}")]
    Pool(#[from] r2d2::Error),

    #[error("Other error: {0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub struct SqliteStore {
    pool: Arc<r2d2::Pool<SqliteConnectionManager>>,
}

impl SqliteStore {
    pub fn new(path: &str) -> Result<Self, SqliteError> {
        let manager = SqliteConnectionManager::file(path).with_flags(
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        );
        let pool = r2d2::Pool::new(manager)?;

        // Initialize the database
        let conn = pool.get()?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS kv_store (
                key BLOB PRIMARY KEY,
                value BLOB
            )",
            [],
        )?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }
}

impl<'db> DocOps<'db> for SqliteStore {}

impl KVStore for SqliteStore {
    type Error = SqliteError;
    type Cursor = SqliteRange;
    type Entry = SqliteEntry;
    type Return = Vec<u8>;

    async fn get(&self, key: &[u8]) -> Result<Option<Self::Return>, Self::Error> {
        let conn = self.pool.get()?;
        let mut stmt = conn.prepare("SELECT value FROM kv_store WHERE key = ?")?;
        let mut rows = stmt.query(params![key])?;

        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    async fn upsert(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        let conn = self.pool.get()?;
        conn.execute(
            "INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)",
            params![key, value],
        )?;
        Ok(())
    }

    async fn remove(&self, key: &[u8]) -> Result<(), Self::Error> {
        let conn = self.pool.get()?;
        conn.execute("DELETE FROM kv_store WHERE key = ?", params![key])?;
        Ok(())
    }

    async fn remove_range(&self, from: &[u8], to: &[u8]) -> Result<(), Self::Error> {
        let conn = self.pool.get()?;
        conn.execute(
            "DELETE FROM kv_store WHERE key >= ? AND key <= ?",
            params![from, to],
        )?;
        Ok(())
    }

    async fn iter_range(&self, from: &[u8], to: &[u8]) -> Result<Self::Cursor, Self::Error> {
        let conn = self.pool.get()?;
        let mut stmt = conn
            .prepare("SELECT key, value FROM kv_store WHERE key >= ? AND key <= ? ORDER BY key")?;
        let rows = stmt.query_map(params![from, to], |row| {
            Ok(SqliteEntry {
                key: row.get(0)?,
                value: row.get(1)?,
            })
        })?;

        Ok(SqliteRange {
            entries: rows.collect::<Result<Vec<_>, _>>()?,
            current: 0,
        })
    }

    async fn peek_back(&self, key: &[u8]) -> Result<Option<Self::Entry>, Self::Error> {
        let conn = self.pool.get()?;
        let mut stmt = conn
            .prepare("SELECT key, value FROM kv_store WHERE key < ? ORDER BY key DESC LIMIT 1")?;
        let mut rows = stmt.query(params![key])?;

        if let Some(row) = rows.next()? {
            Ok(Some(SqliteEntry {
                key: row.get(0)?,
                value: row.get(1)?,
            }))
        } else {
            Ok(None)
        }
    }
}

pub struct SqliteRange {
    entries: Vec<SqliteEntry>,
    current: usize,
}

impl Iterator for SqliteRange {
    type Item = SqliteEntry;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.entries.len() {
            return None;
        }
        let entry = self.entries[self.current].clone();
        self.current += 1;
        Some(entry)
    }
}

#[derive(Clone)]
pub struct SqliteEntry {
    key: Vec<u8>,
    value: Vec<u8>,
}

impl KVEntry for SqliteEntry {
    fn key(&self) -> &[u8] {
        &self.key
    }
    fn value(&self) -> &[u8] {
        &self.value
    }
}
