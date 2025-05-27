// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{BeginBatch, Key, Persistence, Value};
use crate::{PersistenceBatch, Result};
use std::collections::BTreeMap;
use std::collections::btree_map::Range;
use std::ops::RangeBounds;

/// An in-memory key-value store engine
#[derive(Default)]
pub struct Memory(BTreeMap<Key, Value>);

impl BeginBatch for Memory {
    type Batch<'a>
        = MemoryBatch
    where
        Self: 'a;

    fn begin_batch(&self) -> Result<Self::Batch<'_>> {
        Ok(MemoryBatch {})
    }
}

pub struct MemoryBatch {}

impl PersistenceBatch for MemoryBatch {
    fn set(&mut self, key: &Key, value: Value) -> Result<()> {
        todo!()
    }

    fn remove(&mut self, key: &Key) -> Result<()> {
        todo!()
    }

    fn complete(self) -> Result<()> {
        todo!()
    }

    fn abort(self) -> Result<()> {
        todo!()
    }
}

impl Persistence for Memory {
    type ScanIter<'a> = MemoryScanIter<'a>;

    fn get(&self, key: &Key) -> Result<Option<Value>> {
        Ok(self.0.get(key).cloned())
    }

    fn scan(&self, range: impl RangeBounds<Key>) -> Self::ScanIter<'_> {
        MemoryScanIter(self.0.range(range))
    }

    fn set(&mut self, key: &Key, value: Value) -> Result<()> {
        self.0.insert(key.to_vec(), value);
        Ok(())
    }

    fn remove(&mut self, key: &Key) -> Result<()> {
        self.0.remove(key);
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
}

impl PersistenceBatch for Memory {
    fn set(&mut self, key: &Key, value: Value) -> Result<()> {
        todo!()
    }

    fn remove(&mut self, key: &Key) -> Result<()> {
        todo!()
    }

    fn complete(self) -> Result<()> {
        todo!()
    }

    fn abort(self) -> Result<()> {
        todo!()
    }
}

pub struct MemoryScanIter<'a>(Range<'a, Key, Value>);

impl Iterator for MemoryScanIter<'_> {
    type Item = Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| Ok((k.clone(), v.clone())))
    }
}
