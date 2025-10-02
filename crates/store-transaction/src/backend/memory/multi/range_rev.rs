// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use core::iter::Rev;

use crossbeam_skiplist::map::Range as MapRange;
use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, Result, interface::MultiVersionValues};

use crate::backend::{
	memory::{MemoryBackend, MultiVersionTransactionContainer},
	multi::BackendMultiVersionRangeRev,
	result::MultiVersionIterResult,
};

impl BackendMultiVersionRangeRev for MemoryBackend {
	type RangeIterRev<'a>
		= MultiVersionRangeRevIter<'a>
	where
		Self: 'a;

	fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIterRev<'_>> {
		Ok(MultiVersionRangeRevIter {
			range: self.multi.range(range).rev(),
			version,
		})
	}
}

pub struct MultiVersionRangeRevIter<'a> {
	pub(crate) range: Rev<MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, MultiVersionTransactionContainer>>,
	pub(crate) version: CommitVersion,
}

impl Iterator for MultiVersionRangeRevIter<'_> {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		loop {
			let item = self.range.next()?;
			if let Some(values) = item.value().get_or_tombstone(self.version) {
				return Some(match values {
					Some(v) => MultiVersionIterResult::Value(MultiVersionValues {
						key: item.key().clone(),
						version: self.version,
						values: v,
					}),
					None => MultiVersionIterResult::Tombstone {
						key: item.key().clone(),
						version: self.version,
					},
				});
			}
		}
	}
}
