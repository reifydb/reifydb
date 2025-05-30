// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::{Key, Persistence, Value};
use std::ops::RangeBounds;

/// An engine that wraps two others and mirrors operations across them,
/// panicking if they produce different results. reifydb_engine implementations
/// should not have any observable differences in behavior.
pub struct Mirror<A: Persistence, B: Persistence> {
    pub a: A,
    pub b: B,
}

impl<A: Persistence, B: Persistence> Mirror<A, B> {
    pub fn new(a: A, b: B) -> Self {
        Self { a, b }
    }
}

impl<A: Persistence, B: Persistence> Persistence for Mirror<A, B> {
    type ScanIter<'a>
        = MirrorIterator<'a, A, B>
    where
        Self: Sized,
        A: 'a,
        B: 'a;

    fn get(&self, key: &Key) -> crate::Result<Option<Value>> {
        let a = self.a.get(key)?;
        let b = self.b.get(key)?;
        assert_eq!(a, b);
        Ok(a)
    }

    fn scan(&self, range: impl RangeBounds<Key> + Clone) -> Self::ScanIter<'_>
    where
        Self: Sized,
    {
        let a = self.a.scan((range.start_bound().cloned(), range.end_bound().cloned()));
        let b = self.b.scan(range);
        MirrorIterator { a, b }
    }

    // fn status(&mut self) -> crate::Result<Status> {
    // 	let a = self.a.status()?;
    // 	let b = self.b.status()?;
    // 	// Only some items are comparable.
    // 	assert_eq!(a.keys, b.keys);
    // 	assert_eq!(a.size, b.size);
    // 	Ok(a)
    // }

    fn remove(&mut self, key: &Key) -> crate::Result<()> {
        self.a.remove(key)?;
        self.b.remove(key)
    }

    fn sync(&mut self) -> crate::Result<()> {
        self.a.sync()?;
        self.b.sync()
    }

    fn set(&mut self, key: &Key, value: Value) -> crate::Result<()> {
        self.a.set(key, value.clone())?;
        self.b.set(key, value)
    }
}

pub struct MirrorIterator<'a, A: Persistence + 'a, B: Persistence + 'a> {
    a: A::ScanIter<'a>,
    b: B::ScanIter<'a>,
}

impl<A: Persistence, B: Persistence> Iterator for MirrorIterator<'_, A, B> {
    type Item = crate::Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        let a = self.a.next();
        let b = self.b.next();
        assert_eq!(a, b);
        a
    }
}
