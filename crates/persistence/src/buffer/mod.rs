// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod scan;

pub use crate::buffer::scan::BufferScanIter;
use crate::{BeginBatch, Key, Persistence, PersistenceBatch, Value};
use std::collections::BTreeMap;
use std::ops::RangeBounds;

#[derive(Default)]
pub struct Buffer<P: Persistence + BeginBatch> {
    // data read from underlying persistence
    cache: BTreeMap<Key, Value>,

    // holds unsynced key value pairs
    // None if deleted
    staging: BTreeMap<Key, Option<Value>>,

    // underlying persistence where the buffer writes too and read from
    underlying: P,
}

impl<P: Persistence + BeginBatch> Buffer<P> {
    pub fn new(underlying: P) -> Self {
        // Self { cache: RefCell::new(BTreeMap::new()), staging: BTreeMap::new(), underlying }
        Self { cache: BTreeMap::new(), staging: BTreeMap::new(), underlying }
    }
}

impl<P: Persistence + BeginBatch> Persistence for Buffer<P> {
    type ScanIter<'a>
        = BufferScanIter<'a, P>
    where
        P: 'a;

    fn get(&self, key: &Key) -> crate::Result<Option<Value>> {
        if let Some(entry) = self.staging.get(key) {
            return Ok(entry.clone());
        }

        if let Some(entry) = self.cache.get(key) {
            // if let Some(entry) = self.cache.borrow().get(key) {
            return Ok(Some(entry.clone()));
        }

        let result = self.underlying.get(key);
        // if let Ok(Some(value)) = &result {
        //     self.cache.borrow_mut().insert(key.clone(), value.clone());
        // }

        result
    }

    fn scan(&self, range: impl RangeBounds<Key> + Clone) -> Self::ScanIter<'_> {
        BufferScanIter::new(self, range)
    }

    fn set(&mut self, key: &Key, value: Value) -> crate::Result<()> {
        self.staging.insert(key.to_vec(), Some(value.clone()));
        Ok(())
    }

    fn remove(&mut self, key: &Key) -> crate::Result<()> {
        self.staging.insert(key.to_vec(), None);
        Ok(())
    }

    fn sync(&mut self) -> crate::Result<()> {
        let staging = std::mem::take(&mut self.staging);

        let mut batch = self.underlying.begin_batch().unwrap();
        for (k, v) in staging {
            if let Some(v) = v { batch.set(&k, v).unwrap() } else { batch.remove(&k).unwrap() }
        }
        batch.complete().unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::lmdb::Lmdb;
    use crate::{Buffer, Persistence};
    use std::path::Path;

    #[test]
    fn test() {
        let path = Path::new("/tmp/test");

        let lmdb = Lmdb::new(path).unwrap();

        let mut buffer =
            Buffer { cache: Default::default(), staging: Default::default(), underlying: lmdb };

        buffer.set(&b"alpha".to_vec(), b"one".to_vec()).unwrap();
        buffer.set(&b"beta".to_vec(), b"two".to_vec()).unwrap();
        buffer.set(&b"gamma".to_vec(), b"three".to_vec()).unwrap();

        buffer.sync().unwrap();
    }
}
