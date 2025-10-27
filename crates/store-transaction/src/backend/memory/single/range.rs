// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey, EncodedKeyRange, Result, interface::SingleVersionValues, value::encoded::EncodedValues,
};

use crate::backend::{memory::MemoryBackend, result::SingleVersionIterResult, single::BackendSingleVersionRange};

impl BackendSingleVersionRange for MemoryBackend {
	type Range<'a>
		= SingleVersionRangeIter
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange) -> Result<Self::Range<'_>> {
		// Collect items in range under read lock
		let single = self.single.read();
		let items: Vec<(EncodedKey, Option<EncodedValues>)> = single
			.range(range)
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect();
		drop(single); // Release lock early

		Ok(SingleVersionRangeIter {
			items,
			index: 0,
		})
	}
}

pub struct SingleVersionRangeIter {
	pub(crate) items: Vec<(EncodedKey, Option<EncodedValues>)>,
	pub(crate) index: usize,
}

impl Iterator for SingleVersionRangeIter {
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
