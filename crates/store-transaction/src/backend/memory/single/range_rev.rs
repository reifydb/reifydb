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

use crate::{SingleVersionRangeRev as RangeRevInterface, backend::memory::MemoryBackend};

impl RangeRevInterface for MemoryBackend {
	type RangeRev<'a>
		= SingleVersionRangeRevIter<'a>
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange) -> Result<Self::RangeRev<'_>> {
		Ok(SingleVersionRangeRevIter {
			range: self.single.range(range),
		})
	}
}

pub struct SingleVersionRangeRevIter<'a> {
	pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, EncodedValues>,
}

impl Iterator for SingleVersionRangeRevIter<'_> {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.range.next_back()?;
		Some(SingleVersionValues {
			key: item.key().clone(),
			values: item.value().clone(),
		})
	}
}
