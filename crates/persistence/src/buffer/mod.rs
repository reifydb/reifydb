// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod scan;

pub use crate::buffer::scan::BufferScanIter;
use crate::{Key, Persistence, Value};
use std::collections::BTreeMap;
use std::ops::RangeBounds;

#[derive(Default)]
pub struct Buffer<P: Persistence> {
    // data read from underlying persistence
    cache: BTreeMap<Key, Value>,

    // holds unsynced key value pairs
    // None if deleted
    staging: BTreeMap<Key, Option<Value>>,

    // underlying persistence where the buffer writes too and read from
    underlying: P,
}

impl<P: Persistence> Buffer<P> {
    pub fn new(underlying: P) -> Self {
        // Self { cache: RefCell::new(BTreeMap::new()), staging: BTreeMap::new(), underlying }
        Self { cache: BTreeMap::new(), staging: BTreeMap::new(), underlying }
    }
}

impl<P: Persistence> Persistence for Buffer<P> {
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
        Ok(())
    }
}
