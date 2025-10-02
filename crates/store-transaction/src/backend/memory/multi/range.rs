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
use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, Result, interface::MultiVersionValues};

use crate::{
	MultiVersionRange,
	backend::memory::{MemoryBackend, MultiVersionTransactionContainer},
};

impl MultiVersionRange for MemoryBackend {
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
