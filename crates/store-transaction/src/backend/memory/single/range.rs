// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crossbeam_skiplist::map::Range as MapRange;
use reifydb_core::{
	EncodedKey, EncodedKeyRange, Result, interface::SingleVersionValues, value::encoded::EncodedValues,
};

use crate::backend::{memory::MemoryBackend, result::SingleVersionIterResult, single::BackendSingleVersionRange};

impl BackendSingleVersionRange for MemoryBackend {
	type Range<'a>
		= SingleVersionRangeIter<'a>
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange) -> Result<Self::Range<'_>> {
		Ok(SingleVersionRangeIter {
			range: self.single.range(range),
		})
	}
}

pub struct SingleVersionRangeIter<'a> {
	pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, Option<EncodedValues>>,
}

impl Iterator for SingleVersionRangeIter<'_> {
	type Item = SingleVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.range.next()?;
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
