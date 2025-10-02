// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crossbeam_skiplist::map::Iter as MapIter;
use reifydb_core::{EncodedKey, Result, interface::SingleVersionValues, value::encoded::EncodedValues};

use crate::backend::{memory::MemoryBackend, result::SingleVersionIterResult, single::BackendSingleVersionScan};

impl BackendSingleVersionScan for MemoryBackend {
	type ScanIter<'a> = SingleVersionScanIter<'a>;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		let iter = self.single.iter();
		Ok(SingleVersionScanIter {
			iter,
		})
	}
}

pub struct SingleVersionScanIter<'a> {
	pub(crate) iter: MapIter<'a, EncodedKey, Option<EncodedValues>>,
}

impl Iterator for SingleVersionScanIter<'_> {
	type Item = SingleVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.iter.next()?;
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
