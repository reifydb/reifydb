// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crossbeam_skiplist::map::Iter as MapIter;
use reifydb_core::{EncodedKey, Result, interface::SingleVersionValues, value::encoded::EncodedValues};

use crate::backend::{memory::MemoryBackend, result::SingleVersionIterResult, single::BackendSingleVersionScanRev};

impl BackendSingleVersionScanRev for MemoryBackend {
	type ScanIterRev<'a> = SingleVersionScanRevIter<'a>;

	fn scan_rev(&self) -> Result<Self::ScanIterRev<'_>> {
		let iter = self.single.iter();
		Ok(SingleVersionScanRevIter {
			iter,
		})
	}
}

pub struct SingleVersionScanRevIter<'a> {
	pub(crate) iter: MapIter<'a, EncodedKey, Option<EncodedValues>>,
}

impl Iterator for SingleVersionScanRevIter<'_> {
	type Item = SingleVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.iter.next_back()?;
		Some(match item.value().as_ref() {
			Some(v) => SingleVersionIterResult::Value(SingleVersionValues {
				key: item.key().clone(),
				values: v.clone(),
			}),
			None => SingleVersionIterResult::Tombstone {
				key: item.key().clone(),
			},
		})
	}
}
