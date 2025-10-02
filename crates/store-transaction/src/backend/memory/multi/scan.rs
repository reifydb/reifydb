// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crossbeam_skiplist::map::Iter as MapIter;
use reifydb_core::{CommitVersion, EncodedKey, Result, interface::MultiVersionValues};

use crate::backend::{
	memory::{MemoryBackend, MultiVersionTransactionContainer},
	multi::BackendMultiVersionScan,
	result::MultiVersionIterResult,
};

impl BackendMultiVersionScan for MemoryBackend {
	type ScanIter<'a> = MultiVersionScanIter<'a>;

	fn scan(&self, version: CommitVersion) -> Result<Self::ScanIter<'_>> {
		let iter = self.multi.iter();
		Ok(MultiVersionScanIter {
			iter,
			version,
		})
	}
}

pub struct MultiVersionScanIter<'a> {
	pub(crate) iter: MapIter<'a, EncodedKey, MultiVersionTransactionContainer>,
	pub(crate) version: CommitVersion,
}

impl Iterator for MultiVersionScanIter<'_> {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let item = self.iter.next()?;
			if let Some(values) = item.value().get_or_tombstone(self.version) {
				return Some(match values {
					Some(v) => MultiVersionIterResult::Value(MultiVersionValues {
						key: item.key().clone(),
						version: self.version,
						values: v,
					}),
					None => MultiVersionIterResult::Tombstone {
						key: item.key().clone(),
						version: self.version,
					},
				});
			}
		}
	}
}
