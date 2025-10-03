// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use crossbeam_skiplist::map::Entry;
use reifydb_core::{CommitVersion, Result, interface::Cdc};

use crate::{CdcRange, memory::MemoryBackend};

impl CdcRange for MemoryBackend {
	type RangeIter<'a> = CdcRangeIter<'a>;

	fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> Result<Self::RangeIter<'_>> {
		Ok(CdcRangeIter {
			version_iter: Box::new(self.cdc.range((start, end))),
		})
	}
}

pub struct CdcRangeIter<'a> {
	version_iter: Box<dyn Iterator<Item = Entry<'a, CommitVersion, Cdc>> + 'a>,
}

impl<'a> Iterator for CdcRangeIter<'a> {
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		self.version_iter.next().map(|entry| entry.value().clone())
	}
}
