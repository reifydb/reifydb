// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::iter::Rev;

use crossbeam_skiplist::map::Iter as MapIter;
use reifydb_core::{CommitVersion, EncodedKey, Result, interface::MultiVersionValues};

use crate::{
	MultiVersionScanRev,
	backend::memory::{MemoryBackend, MultiVersionTransactionContainer},
};

impl MultiVersionScanRev for MemoryBackend {
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
