// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use core::iter::Rev;

use crossbeam_skiplist::map::Iter as MapIter;
use reifydb_core::{CommitVersion, EncodedKey, Result, interface::MultiVersionValues};

use crate::backend::{
	memory::{MemoryBackend, MultiVersionTransactionContainer},
	multi::BackendMultiVersionScanRev,
	result::MultiVersionIterResult,
};

impl BackendMultiVersionScanRev for MemoryBackend {
	type ScanIterRev<'a> = MultiVersionScanRevIter<'a>;

	fn scan_rev(&self, version: CommitVersion) -> Result<Self::ScanIterRev<'_>> {
		let iter = self.multi.iter();
		Ok(MultiVersionScanRevIter {
			iter: iter.rev(),
			version,
		})
	}
}

pub struct MultiVersionScanRevIter<'a> {
	pub(crate) iter: Rev<MapIter<'a, EncodedKey, MultiVersionTransactionContainer>>,
	pub(crate) version: CommitVersion,
}

impl Iterator for MultiVersionScanRevIter<'_> {
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
