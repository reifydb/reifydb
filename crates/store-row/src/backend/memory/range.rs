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
	CommitVersion, EncodedKey, EncodedKeyRange, Result,
	interface::{MultiVersionRange, MultiVersionRow, SingleVersionRange as RangeInterface, SingleVersionRow},
	value::row::EncodedRow,
};

use crate::backend::memory::{Memory, MultiVersionRowContainer};

impl MultiVersionRange for Memory {
	type RangeIter<'a>
		= Range<'a>
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIter<'_>> {
		Ok(Range {
			range: self.multi.range(range),
			version,
		})
	}
}

pub struct Range<'a> {
	pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, MultiVersionRowContainer>,
	pub(crate) version: CommitVersion,
}

impl Iterator for Range<'_> {
	type Item = MultiVersionRow;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let item = self.range.next()?;
			if let Some(row) = item.value().get(self.version) {
				return Some(MultiVersionRow {
					key: item.key().clone(),
					version: self.version,
					row,
				});
			}
		}
	}
}

impl RangeInterface for Memory {
	type Range<'a>
		= SingleVersionRange<'a>
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange) -> Result<Self::Range<'_>> {
		Ok(SingleVersionRange {
			range: self.single.range(range),
		})
	}
}

pub struct SingleVersionRange<'a> {
	pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, EncodedRow>,
}

impl Iterator for SingleVersionRange<'_> {
	type Item = SingleVersionRow;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.range.next()?;
		Some(SingleVersionRow {
			key: item.key().clone(),
			row: item.value().clone(),
		})
	}
}
