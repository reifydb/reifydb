// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::transaction::old_mvcc::{Error, Key, TransactionState, Version};
use reifydb_core::encoding::{Key as _, bincode};
use reifydb_persistence::{Persistence, Value};
use std::collections::{Bound, VecDeque};
use std::sync::{Arc, Mutex};

/// An iterator over the latest live and visible key-value pairs for the tx.
///
/// The (single-threaded) engine is shared via mutex, and holding the mutex for
/// the lifetime of the iterator can cause deadlocks (e.g. when the local SQL
/// reifydb_engine pulls from two tables concurrently during a join). Instead, we pull
/// and buffer a batch of rows at a time, and release the mutex in between.

pub struct ScanIterator<P: Persistence> {
    /// The persistence layer - most likely buffered
    persistence: Arc<Mutex<P>>,
    /// The transaction state.
    tx: TransactionState,
    /// A buffer of live and visible key-value pairs to emit.
    buffer: VecDeque<(Vec<u8>, Vec<u8>)>,
    /// The remaining range after the buffer.
    remainder: Option<(Bound<Vec<u8>>, Bound<Vec<u8>>)>,
}

/// Implement [`Clone`] manually. `derive(Clone)` isn't smart enough to figure
/// out that we don't need `reifydb_engine: Clone` when it's in an [`Arc`]. See:
/// <https://github.com/rust-lang/rust/issues/26925>.
impl<P: Persistence> Clone for ScanIterator<P> {
    fn clone(&self) -> Self {
        Self {
            persistence: self.persistence.clone(),
            tx: self.tx.clone(),
            buffer: self.buffer.clone(),
            remainder: self.remainder.clone(),
        }
    }
}

impl<P: Persistence> ScanIterator<P> {
    /// The number of live key-value pairs to pull from the reifydb_engine each time we
    /// lock it. Uses 2 in tests to exercise the buffering code.
    const BUFFER_SIZE: usize = if cfg!(test) { 2 } else { 32 };

    /// Creates a new scan iterator.
    pub(crate) fn new(
        persistence: Arc<Mutex<P>>,
        tx: TransactionState,
        range: (Bound<Vec<u8>>, Bound<Vec<u8>>),
    ) -> Self {
        let buffer = VecDeque::with_capacity(Self::BUFFER_SIZE);
        Self { persistence, tx, buffer, remainder: Some(range) }
    }

    /// Fills the buffer, if there's any pending items.
    fn fill_buffer(&mut self) -> crate::transaction::old_mvcc::Result<()> {
        // Check if there's anything to buffer.
        if self.buffer.len() >= Self::BUFFER_SIZE {
            return Ok(());
        }
        let Some(range) = self.remainder.take() else {
            return Ok(());
        };
        let range_end = range.1.clone();

        // FIXME
        // let mut reifydb_engine = self.reifydb_engine.lock()?;
        let mut reifydb_engine = self.persistence.lock().unwrap();
        let mut iter = VersionIterator::new(&self.tx, reifydb_engine.scan(range)).peekable();

        while let Some((key, _, value)) = iter.next().transpose()? {
            // If the next key equals this one, we're not at the latest version.
            match iter.peek() {
                Some(Ok((next, _, _))) if next == &key => continue,
                // FIXME
                // Some(Err(err)) => return Err(err.clone()),
                Some(Err(err)) => unimplemented!(),
                Some(Ok(_)) | None => {}
            }

            // Decode the value, and skip deleted keys (tombstones).
            let Some(value) = bincode::deserialize(&value)? else { continue };
            self.buffer.push_back((key, value));

            // If we filled the buffer, save the remaining range (if any) and
            // return. peek() has already buffered next(), so pull it.
            if self.buffer.len() == Self::BUFFER_SIZE {
                if let Some((next, version, _)) = iter.next().transpose()? {
                    // We have to re-encode it as a raw reifydb_engine key, since we
                    // only have access to the decoded MVCC user key.
                    let range_start = Bound::Included(Key::Version(next.into(), version).encode());
                    self.remainder = Some((range_start, range_end));
                }
                return Ok(());
            }
        }
        Ok(())
    }
}

impl<P: Persistence> Iterator for ScanIterator<P> {
    type Item = crate::transaction::old_mvcc::Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            if let Err(error) = self.fill_buffer() {
                return Some(Err(error));
            }
        }
        self.buffer.pop_front().map(Ok)
    }
}

/// An iterator that decodes raw reifydb_engine key-value pairs into MVCC key-value
/// versions, and skips invisible versions. Helper for ScanIterator.
struct VersionIterator<'a, I: reifydb_persistence::ScanIterator> {
    /// The transaction the scan is running in.
    tx: &'a TransactionState,
    /// The inner reifydb_engine scan iterator.
    inner: I,
}

impl<'a, I: reifydb_persistence::ScanIterator> VersionIterator<'a, I> {
    /// Creates a new MVCC version iterator for the given iterator.
    fn new(tx: &'a TransactionState, inner: I) -> Self {
        Self { tx: tx, inner }
    }

    // Fallible next(). Returns the next visible key/version/value tuple.
    fn try_next(
        &mut self,
    ) -> crate::transaction::old_mvcc::Result<Option<(Vec<u8>, Version, Vec<u8>)>> {
        while let Some((key, value)) = self.inner.next().transpose()? {
            let decoded_key = Key::decode(&key)?;
            let Key::Version(key, version) = decoded_key else {
                return Err(Error::unexpected_key("Key::Version", decoded_key));
            };
            if !self.tx.is_visible(version) {
                continue;
            }
            return Ok(Some((key.into_owned(), version, value)));
        }
        Ok(None)
    }
}

impl<I: reifydb_persistence::ScanIterator> Iterator for VersionIterator<'_, I> {
    type Item = crate::transaction::old_mvcc::Result<(Vec<u8>, Version, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.try_next().transpose()
    }
}
