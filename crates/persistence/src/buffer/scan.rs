// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{BeginBatch, Buffer, Key, Persistence, Value};
use std::collections::btree_map::Range;
use std::ops::RangeBounds;

pub struct BufferScanIter<'a, P: Persistence>
where
    P: 'a,
{
    staging: Range<'a, Key, Option<Value>>,
    cache: Range<'a, Key, Value>,
    underlying: P::ScanIter<'a>,
    next_staging: Option<(Key, Option<Value>)>,
    next_cache: Option<(Key, Value)>,
    next_underlying: Option<crate::Result<(Key, Value)>>,
}

impl<'a, P: Persistence + BeginBatch> BufferScanIter<'a, P> {
    pub fn new<R>(buffer: &'a Buffer<P>, range: R) -> Self
    where
        R: RangeBounds<Key> + Clone,
    {
        let staging = buffer.staging.range(range.clone());
        let cache = buffer.cache.range(range.clone());
        let underlying = buffer.underlying.scan(range);

        BufferScanIter {
            staging,
            cache,
            underlying,
            next_staging: None,
            next_cache: None,
            next_underlying: None,
        }
    }
}

impl<'a, P: Persistence> BufferScanIter<'a, P> {
    fn next_item<F1, F2, F3>(
        &mut self,
        take_staging: F1,
        take_cache: F2,
        take_underlying: F3,
    ) -> Option<crate::Result<(Key, Value)>>
    where
        F1: FnOnce(&mut Self) -> Option<(Key, Option<Value>)>,
        F2: FnOnce(&mut Self) -> Option<(Key, Value)>,
        F3: FnOnce(&mut Self) -> Option<crate::Result<(Key, Value)>>,
    {
        if self.next_staging.is_none() {
            self.next_staging = take_staging(self);
        }
        if self.next_cache.is_none() {
            self.next_cache = take_cache(self);
        }
        if self.next_underlying.is_none() {
            self.next_underlying = take_underlying(self);
        }

        let key_staging = self.next_staging.as_ref().map(|(k, _)| k);
        let key_cache = self.next_cache.as_ref().map(|(k, _)| k);
        let key_underlying =
            self.next_underlying.as_ref().and_then(|r| r.as_ref().ok()).map(|(k, _)| k);

        let next_key =
            [key_staging, key_cache, key_underlying].into_iter().flatten().min().cloned()?;

        // staging → cache → underlying
        if Some(&next_key) == key_staging {
            let (k, v) = self.next_staging.take().unwrap();
            return if let Some(v) = v {
                Some(Ok((k, v)))
            } else {
                // Deleted
                self.next() // skip and continue
            };
        }

        if Some(&next_key) == key_cache {
            return self.next_cache.take().map(|(k, v)| Ok((k, v)));
        }

        if Some(&next_key) == key_underlying {
            return self.next_underlying.take();
        }

        None
    }
}

impl<'a, P: Persistence> Iterator for BufferScanIter<'a, P> {
    type Item = crate::Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_item(
            |s| s.staging.next().map(|(k, v)| (k.clone(), v.clone())),
            |s| s.cache.next().map(|(k, v)| (k.clone(), v.clone())),
            |s| s.underlying.next(),
        )
    }
}

impl<P: Persistence> DoubleEndedIterator for BufferScanIter<'_, P> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.next_item(
            |s| s.staging.next_back().map(|(k, v)| (k.clone(), v.clone())),
            |s| s.cache.next_back().map(|(k, v)| (k.clone(), v.clone())),
            |s| s.underlying.next_back(),
        )
    }
}
