// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::iter::Rev;

use crossbeam_skiplist::map::Range as MapRange;
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, Result,
	interface::{Unversioned, UnversionedRangeRev as RangeRevInterface, Versioned, VersionedRangeRev},
	row::EncodedRow,
};

use crate::memory::{Memory, VersionedRow};

impl VersionedRangeRev for Memory {
	type RangeIterRev<'a>
		= RangeRev<'a>
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIterRev<'_>> {
		Ok(RangeRev {
			range: self.versioned.range(range).rev(),
			version,
		})
	}
}

pub struct RangeRev<'a> {
	pub(crate) range: Rev<MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, VersionedRow>>,
	pub(crate) version: CommitVersion,
}

impl Iterator for RangeRev<'_> {
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

impl RangeRevInterface for Memory {
	type RangeRev<'a>
		= UnversionedRangeRev<'a>
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange) -> Result<Self::RangeRev<'_>> {
		Ok(UnversionedRangeRev {
			range: self.unversioned.range(range),
		})
	}
}

pub struct UnversionedRangeRev<'a> {
	pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, EncodedRow>,
}

impl Iterator for UnversionedRangeRev<'_> {
	type Item = Unversioned;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.range.next_back()?;
		Some(Unversioned {
			key: item.key().clone(),
			row: item.value().clone(),
		})
	}
}
