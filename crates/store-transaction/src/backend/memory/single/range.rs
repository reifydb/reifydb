// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crossbeam_skiplist::map::Range as MapRange;
use reifydb_core::{
	EncodedKey, EncodedKeyRange, Result, interface::SingleVersionValues, value::encoded::EncodedValues,
};

use crate::{SingleVersionRange as RangeInterface, backend::memory::MemoryBackend};

impl RangeInterface for MemoryBackend {
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
	pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, EncodedValues>,
}

impl Iterator for SingleVersionRangeIter<'_> {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.range.next()?;
		Some(SingleVersionValues {
			key: item.key().clone(),
			values: item.value().clone(),
		})
	}
}
