// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crossbeam_skiplist::map::Range as MapRange;
use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, Result, interface::MultiVersionValues};

use crate::backend::{
	memory::{MemoryBackend, MultiVersionTransactionContainer},
	multi::BackendMultiVersionRange,
	result::MultiVersionIterResult,
};

impl BackendMultiVersionRange for MemoryBackend {
	type RangeIter<'a>
		= MultiVersionRangeIter<'a>
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIter<'_>> {
		Ok(MultiVersionRangeIter {
			range: self.multi.range(range),
			version,
		})
	}
}

pub struct MultiVersionRangeIter<'a> {
	pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, MultiVersionTransactionContainer>,
	pub(crate) version: CommitVersion,
}

impl Iterator for MultiVersionRangeIter<'_> {
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
