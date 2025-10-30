// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey, EncodedKeyRange, Result, interface::SingleVersionValues, value::encoded::EncodedValues,
};

use crate::backend::{memory::MemoryBackend, result::SingleVersionIterResult, single::BackendSingleVersionRangeRev};

impl BackendSingleVersionRangeRev for MemoryBackend {
	type RangeRev<'a>
		= SingleVersionRangeRevIter
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange) -> Result<Self::RangeRev<'_>> {
		// Collect items in range under read lock, then reverse
		let single = self.single.read();
		let items: Vec<(EncodedKey, Option<EncodedValues>)> =
			single.range(range).rev().map(|(k, v)| (k.clone(), v.clone())).collect();
		drop(single); // Release lock early

		Ok(SingleVersionRangeRevIter {
			items,
			index: 0,
		})
	}
}

pub struct SingleVersionRangeRevIter {
	pub(crate) items: Vec<(EncodedKey, Option<EncodedValues>)>,
	pub(crate) index: usize,
}

impl Iterator for SingleVersionRangeRevIter {
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
