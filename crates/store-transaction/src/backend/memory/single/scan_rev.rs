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
use reifydb_core::{EncodedKey, Result, interface::SingleVersionValues, value::encoded::EncodedValues};

use crate::{SingleVersionScanRev, backend::memory::MemoryBackend};

impl SingleVersionScanRev for MemoryBackend {
	type ScanIterRev<'a> = SingleVersionScanRevIter<'a>;

	fn scan_rev(&self) -> Result<Self::ScanIterRev<'_>> {
		let iter = self.single.iter();
		Ok(SingleVersionScanRevIter {
			iter,
		})
	}
}

pub struct SingleVersionScanRevIter<'a> {
	pub(crate) iter: MapIter<'a, EncodedKey, EncodedValues>,
}

impl Iterator for SingleVersionScanRevIter<'_> {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.iter.next_back()?;
		Some(SingleVersionValues {
			key: item.key().clone(),
			values: item.value().clone(),
		})
	}
}
