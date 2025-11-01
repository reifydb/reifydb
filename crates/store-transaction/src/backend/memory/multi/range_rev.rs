// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::BTreeMap;

use parking_lot::RwLockReadGuard;
use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, Result, interface::MultiVersionValues};

use crate::backend::{
	memory::{MemoryBackend, VersionChain},
	multi::BackendMultiVersionRangeRev,
	result::MultiVersionIterResult,
};

impl BackendMultiVersionRangeRev for MemoryBackend {
	type RangeIterRev<'a>
		= MultiVersionRangeRevIter<'a>
	where
		Self: 'a;

	fn range_rev_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		_batch_size: u64,
	) -> Result<Self::RangeIterRev<'_>> {
		// Memory backend doesn't need batching as it's already in memory
		let guard = self.multi.read();
		// Convert EncodedKeyRange to range bounds
		let iter = unsafe { std::mem::transmute((*guard).range(range)) };
		Ok(MultiVersionRangeRevIter {
			_guard: guard,
			iter,
			version,
		})
	}
}

pub struct MultiVersionRangeRevIter<'a> {
	pub(crate) _guard: RwLockReadGuard<'a, BTreeMap<EncodedKey, VersionChain>>,
	pub(crate) iter: std::collections::btree_map::Range<'a, EncodedKey, VersionChain>,
	pub(crate) version: CommitVersion,
}

// SAFETY: We need to manually implement Send for the iterator
// The guard ensures the map stays valid for the iterator's lifetime
unsafe impl<'a> Send for MultiVersionRangeRevIter<'a> {}

impl Iterator for MultiVersionRangeRevIter<'_> {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		// Use next_back for reverse iteration
		while let Some((key, chain)) = self.iter.next_back() {
			if let Some(values) = chain.get_at(self.version) {
				return Some(if let Some(vals) = values {
					MultiVersionIterResult::Value(MultiVersionValues {
						key: key.clone(),
						values: vals,
						version: self.version,
					})
				} else {
					// Tombstone
					MultiVersionIterResult::Tombstone {
						key: key.clone(),
						version: self.version,
					}
				});
			}
		}
		None
	}
}
