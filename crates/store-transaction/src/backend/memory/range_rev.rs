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
	interface::{
		MultiVersionRangeRev, MultiVersionValues, SingleVersionRangeRev as RangeRevInterface,
		SingleVersionValues,
	},
	value::encoded::EncodedValues,
};

use crate::backend::memory::{Memory, MultiVersionTransactionContainer};

impl MultiVersionRangeRev for Memory {
	type RangeIterRev<'a>
		= RangeRev<'a>
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIterRev<'_>> {
		Ok(RangeRev {
			range: self.multi.range(range).rev(),
			version,
		})
	}
}

pub struct RangeRev<'a> {
	pub(crate) range: Rev<MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, MultiVersionTransactionContainer>>,
	pub(crate) version: CommitVersion,
}

impl Iterator for RangeRev<'_> {
	type Item = MultiVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let item = self.range.next()?;
			if let Some(row) = item.value().get(self.version) {
				return Some(MultiVersionValues {
					key: item.key().clone(),
					version: self.version,
					values: row,
				});
			}
		}
	}
}

impl RangeRevInterface for Memory {
	type RangeRev<'a>
		= SingleVersionRangeRev<'a>
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange) -> Result<Self::RangeRev<'_>> {
		Ok(SingleVersionRangeRev {
			range: self.single.range(range),
		})
	}
}

pub struct SingleVersionRangeRev<'a> {
	pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, EncodedValues>,
}

impl Iterator for SingleVersionRangeRev<'_> {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		let item = self.range.next_back()?;
		Some(SingleVersionValues {
			key: item.key().clone(),
			values: item.value().clone(),
		})
	}
}
