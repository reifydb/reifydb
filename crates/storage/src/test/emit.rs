// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::{Engine, EngineMut, Key, Value};
use std::ops::RangeBounds;
use std::sync::mpsc::Sender;

/// Wraps another engine and emits write events to the given channel.
pub struct Emit<E: EngineMut> {
    /// The wrapped engine.
    inner: E,
    /// Sends operation events.
    tx: Sender<crate::test::Operation>,
}

impl<E: EngineMut> crate::test::Emit<E> {
    pub fn new(inner: E, tx: Sender<crate::test::Operation>) -> Self {
        Self { inner, tx }
    }
}

impl<E: EngineMut> Engine for Emit<E> {
    type ScanIter<'a>
        = E::ScanIter<'a>
    where
        E: 'a;

    fn get(&self, key: &Key) -> crate::Result<Option<Value>> {
        self.inner.get(key)
    }

    fn scan(&self, range: impl RangeBounds<Key>) -> Self::ScanIter<'_> {
        self.inner.scan(range)
    }

    // fn status(&mut self) -> Result<Status> {
    // 	self.inner.status()
    // }
}

impl<E: EngineMut> EngineMut for Emit<E> {
    fn sync(&mut self) -> crate::Result<()> {
        self.inner.sync()?;
        self.tx.send(crate::test::Operation::Sync).unwrap();
        Ok(())
    }

    fn remove(&mut self, key: &Key) -> crate::Result<()> {
        self.inner.remove(key)?;
        self.tx.send(crate::test::Operation::Delete { key: key.to_vec() }).unwrap();
        Ok(())
    }

    fn set(&mut self, key: &Key, value: Value) -> crate::Result<()> {
        self.inner.set(key, value.clone())?;
        self.tx.send(crate::test::Operation::Set { key: key.to_vec(), value }).unwrap();
        Ok(())
    }
}
