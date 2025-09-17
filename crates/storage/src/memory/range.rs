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
	interface::{Unversioned, UnversionedRange as RangeInterface, Versioned, VersionedRange},
	row::EncodedRow,
};

use crate::memory::{Memory, VersionedRow};

impl VersionedRange for Memory {
	type RangeIter<'a>
		= Range<'a>
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIter<'_>> {
		Ok(Range {
			range: self.versioned.range(range),
			version,
		})
	}
}

pub struct Range<'a> {
	pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, VersionedRow>,
	pub(crate) version: CommitVersion,
}

impl Iterator for Range<'_> {
	type Item = Versioned;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let item = self.range.next()?;
			if let Some(row) = item.value().get(self.version) {
				return Some(Versioned {
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
		= UnversionedRange<'a>
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange) -> Result<Self::Range<'_>> {
		Ok(UnversionedRange {
			range: self.unversioned.range(range),
		})
	}
}

pub struct UnversionedRange<'a> {
	pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, EncodedRow>,
}

impl Iterator for UnversionedRange<'_> {
	type Item = Unversioned;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.range.next()?;
		Some(Unversioned {
			key: item.key().clone(),
			row: item.value().clone(),
		})
	}
}
