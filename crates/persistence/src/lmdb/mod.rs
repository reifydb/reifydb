// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod scan;

use crate::{BeginBatch, Key, MemoryScanIter, Persistence, PersistenceBatch, Value};
use heed::types::Bytes;
use heed::{Database, Env, EnvOpenOptions, RwTxn};
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;
use crate::lmdb::scan::LmdbScanIter;

pub struct Lmdb {
    pub(crate) env: Arc<Env>,
    pub(crate) db: Database<Bytes, Bytes>,
}

impl Lmdb {
    pub fn new(path: &Path) -> crate::Result<Self> {
        let env = unsafe { EnvOpenOptions::new().max_dbs(1).open(path).unwrap() };

        // dummy txn just to create DB
        let mut txn = env.write_txn().unwrap();
        let db = env.create_database::<Bytes, Bytes>(&mut txn, None).unwrap();
        txn.commit().unwrap();

        Ok(Self { env: Arc::new(env), db })
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
        = MemoryScanIter<'a>
    where
        Self: 'a;

    fn get(&self, key: &Key) -> crate::Result<Option<Value>> {
        let txn = self.env.read_txn().unwrap();
        let val = self.db.get(&txn, &key[..]).unwrap();
        Ok(val.map(|v| v.to_vec()))
    }

    fn scan(&self, range: impl RangeBounds<Key> + Clone) -> Self::ScanIter<'_> {
        // let txn = self.env.read_txn().expect("txn should succeed");
        // LmdbScanIter::new(*txn, self.db, range)
        todo!()
    }

    fn set(&mut self, key: &Key, value: Value) -> crate::Result<()> {
        unreachable!("use batch methods")
    }

    fn remove(&mut self, key: &Key) -> crate::Result<()> {
        unreachable!("use batch methods")
    }

    fn sync(&mut self) -> crate::Result<()> {
        unreachable!("use batch methods")
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

#[cfg(test)]
mod tests {
    use crate::lmdb::Lmdb;
    use crate::{BeginBatch, Persistence, PersistenceBatch};
    use std::path::Path;
    use std::time::Instant;

    #[test]
    fn test() {
        let path = Path::new("/tmp/test");

        let lmdb = Lmdb::new(path).unwrap();

        let batch_data = vec![
            (b"alpha".to_vec(), b"one".to_vec()),
            (b"beta".to_vec(), b"two".to_vec()),
            (b"gamma".to_vec(), b"three".to_vec()),
        ];

        let start = Instant::now();
        let mut batch = lmdb.begin_batch().unwrap();

        for (key, value) in batch_data {
            batch.set(&key, value).unwrap();
        }

        batch.complete().unwrap();

        println!("Batch inserted and committed.");
        println!("Time: {} ms", start.elapsed().as_millis());

        // Verify values
        let reader = lmdb.env.read_txn().unwrap();
        let val = lmdb.db.get(&reader, b"beta").unwrap().unwrap();
        assert_eq!(val, b"two");
    }
}
