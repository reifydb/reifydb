// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::BTreeMap;

use parking_lot::RwLockReadGuard;
use reifydb_core::{CommitVersion, EncodedKey, Result, interface::MultiVersionValues};

use crate::backend::{
	memory::{MemoryBackend, VersionChain},
	multi::BackendMultiVersionScan,
	result::MultiVersionIterResult,
};

impl BackendMultiVersionScan for MemoryBackend {
	type ScanIter<'a> = MultiVersionScanIter<'a>;

	fn scan(&self, version: CommitVersion) -> Result<Self::ScanIter<'_>> {
		let guard = self.multi.read();
		// SAFETY: We extend the lifetime to match the guard, which is held in the iterator
		let iter = unsafe { std::mem::transmute((*guard).iter()) };
		Ok(MultiVersionScanIter {
			_guard: guard,
			iter,
			version,
		})
	}
}

pub struct MultiVersionScanIter<'a> {
	pub(crate) _guard: RwLockReadGuard<'a, BTreeMap<EncodedKey, VersionChain>>,
	pub(crate) iter: std::collections::btree_map::Iter<'a, EncodedKey, VersionChain>,
	pub(crate) version: CommitVersion,
}

// SAFETY: We need to manually implement Send for the iterator
// The guard ensures the map stays valid for the iterator's lifetime
unsafe impl<'a> Send for MultiVersionScanIter<'a> {}

impl Iterator for MultiVersionScanIter<'_> {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		while let Some((key, chain)) = self.iter.next() {
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
