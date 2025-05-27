// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod scan;

use crate::lmdb::scan::LmdbScanIter;
use crate::{BeginBatch, Key, Persistence, PersistenceBatch, Value};
use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions, RwTxn};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

pub struct Lmdb {
    pub(crate) env: Arc<Env>,
    pub(crate) db: Database<Bytes, Bytes>,
}

impl Lmdb {
    pub fn new(path: &Path) -> crate::Result<Self> {
        let env = unsafe { EnvOpenOptions::new().max_dbs(1).open(path).unwrap() };

        let mut tx = env.write_txn().unwrap();
        let db = env.create_database::<Bytes, Bytes>(&mut tx, None).unwrap();
        tx.commit().unwrap();

        Ok(Self { env: Arc::new(env), db })
    }
}

pub struct LmbdbScanIter {
    pub(crate) env: Arc<Env>,
    pub(crate) db: Database<Bytes, Bytes>,
}

impl LmbdbScanIter {
    pub fn new(env: Arc<Env>, db: Database<Bytes, Bytes>) -> Self {
        Self { env, db }
    }
}

pub struct LmdbBatch<'env> {
    db: Database<Bytes, Bytes>,
    txn: Option<RwTxn<'env>>,
}

impl BeginBatch for Lmdb {
    type Batch<'a>
        = LmdbBatch<'a>
    where
        Self: 'a;

    fn begin_batch(&self) -> crate::Result<Self::Batch<'_>> {
        let txn = self.env.write_txn().unwrap();
        Ok(LmdbBatch { db: self.db, txn: Some(txn) })
    }
}

impl Persistence for Lmdb {
    type ScanIter<'a>
        = LmdbScanIter
    where
        Self: 'a;

    fn get(&self, key: &Key) -> crate::Result<Option<Value>> {
        let txn = self.env.read_txn().unwrap();
        let val = self.db.get(&txn, &key[..]).unwrap();
        Ok(val.map(|v| v.to_vec()))
    }

    fn scan(&self, range: impl RangeBounds<Key> + Clone) -> Self::ScanIter<'_> {
        LmdbScanIter::new(self.env.clone(), self.db.clone(), range, 1000)
    }

    fn set(&mut self, key: &Key, value: Value) -> crate::Result<()> {
        let mut tx = self.env.write_txn().unwrap();
        self.db.put(&mut tx, &key[..], &value).unwrap();
        tx.commit().unwrap();
        Ok(())
    }

    fn remove(&mut self, key: &Key) -> crate::Result<()> {
        let mut tx = self.env.write_txn().unwrap();
        self.db.delete(&mut tx, &key[..]).unwrap();
        tx.commit().unwrap();
        Ok(())
    }

    fn sync(&mut self) -> crate::Result<()> {
        Ok(())
    }
}
impl PersistenceBatch for LmdbBatch<'_> {
    fn set(&mut self, key: &Key, value: Value) -> crate::Result<()> {
        self.db.put(self.txn.as_mut().unwrap(), &key[..], &value).unwrap();
        Ok(())
    }

    fn remove(&mut self, key: &Key) -> crate::Result<()> {
        self.db.delete(self.txn.as_mut().unwrap(), &key[..]).unwrap();
        Ok(())
    }

    fn complete(mut self) -> crate::Result<()> {
        if let Some(txn) = self.txn.take() {
            txn.commit().unwrap();
        }
        Ok(())
    }

    fn abort(mut self) -> crate::Result<()> {
        if let Some(txn) = self.txn.take() {
            drop(txn);
        }
        Ok(())
    }
}
