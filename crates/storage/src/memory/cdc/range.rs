// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use crossbeam_skiplist::map::Entry;
use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcRange},
};

use crate::memory::Memory;

impl CdcRange for Memory {
	type RangeIter<'a> = Range<'a>;

	fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> Result<Self::RangeIter<'_>> {
		Ok(Range {
			version_iter: Box::new(self.cdcs.range((start, end))),
		})
	}
}

pub struct Range<'a> {
	version_iter: Box<dyn Iterator<Item = Entry<'a, CommitVersion, Cdc>> + 'a>,
}

impl<'a> Iterator for Range<'a> {
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		self.version_iter.next().map(|entry| entry.value().clone())
	}
}
