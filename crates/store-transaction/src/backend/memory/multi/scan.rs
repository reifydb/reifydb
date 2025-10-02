// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crossbeam_skiplist::map::Iter as MapIter;
use reifydb_core::{CommitVersion, EncodedKey, Result, interface::MultiVersionValues};

use crate::{
	MultiVersionScan,
	backend::memory::{MemoryBackend, MultiVersionTransactionContainer},
};

impl MultiVersionScan for MemoryBackend {
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
	type Item = MultiVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let item = self.iter.next()?;
			if let Some(row) = item.value().get(self.version) {
				return Some(MultiVersionValues {
					key: item.key().clone(),
					values: row,
					version: self.version,
				});
			}
		}
	}
}
