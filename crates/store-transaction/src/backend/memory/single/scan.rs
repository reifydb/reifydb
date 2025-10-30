// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{EncodedKey, Result, interface::SingleVersionValues, value::encoded::EncodedValues};

use crate::backend::{memory::MemoryBackend, result::SingleVersionIterResult, single::BackendSingleVersionScan};

impl BackendSingleVersionScan for MemoryBackend {
	type ScanIter<'a> = SingleVersionScanIter;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		// Collect all items under read lock
		let single = self.single.read();
		let items: Vec<(EncodedKey, Option<EncodedValues>)> =
			single.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
		drop(single); // Release lock early

		Ok(SingleVersionScanIter {
			items,
			index: 0,
		})
	}
}

pub struct SingleVersionScanIter {
	pub(crate) items: Vec<(EncodedKey, Option<EncodedValues>)>,
	pub(crate) index: usize,
}

impl Iterator for SingleVersionScanIter {
	type Item = SingleVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		if self.index >= self.items.len() {
			return None;
		}

		let (key, values) = &self.items[self.index];
		self.index += 1;

		Some(match values {
			Some(v) => SingleVersionIterResult::Value(SingleVersionValues {
				key: key.clone(),
				values: v.clone(),
			}),
			None => SingleVersionIterResult::Tombstone {
				key: key.clone(),
			},
		})
	}
}
