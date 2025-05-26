// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use persistence::{Key, Persistence, Value};
use std::collections::BTreeMap;
use std::collections::btree_map::Range;
use std::ops::RangeBounds;

#[derive(Default)]
pub struct MemStage<P: Persistence> {
    memory: BTreeMap<Key, Value>,
    persistence: P,
}

impl<P: Persistence> MemStage<P> {
    pub fn new(persistence: P) -> Self {
        Self { memory: BTreeMap::new(), persistence }
    }
}

impl<P: Persistence> Persistence for MemStage<P> {
    type ScanIter<'a>
        = MemStageScan<'a>
    where
        P: 'a;

    fn get(&self, key: &Key) -> persistence::Result<Option<Value>> {
        Ok(self.memory.get(key).cloned())
    }

    fn scan(&self, range: impl RangeBounds<Key>) -> Self::ScanIter<'_> {
        MemStageScan(self.memory.range(range))
    }

    fn set(&mut self, key: &Key, value: Value) -> persistence::Result<()> {
        self.memory.insert(key.to_vec(), value);
        Ok(())
    }

    fn remove(&mut self, key: &Key) -> persistence::Result<()> {
        self.memory.remove(key);
        Ok(())
    }

    fn sync(&mut self) -> persistence::Result<()> {
        Ok(())
    }
}

pub struct MemStageScan<'a>(Range<'a, Key, Value>);

impl Iterator for MemStageScan<'_> {
    type Item = persistence::Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| Ok((k.clone(), v.clone())))
    }
}

impl DoubleEndedIterator for MemStageScan<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(|(k, v)| Ok((k.clone(), v.clone())))
    }
}
