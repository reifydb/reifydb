// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use crossbeam_skiplist::map::Entry;
use reifydb_core::{
	Result, Version,
	interface::{CdcEvent, CdcEventKey, CdcRange},
};

use crate::memory::Memory;

impl CdcRange for Memory {
	type RangeIter<'a> = Range<'a>;

	fn range(
		&self,
		start: Bound<Version>,
		end: Bound<Version>,
	) -> Result<Self::RangeIter<'_>> {
		let start_key = match start {
			Bound::Included(v) => Bound::Included(CdcEventKey {
				version: v,
				sequence: 0,
			}),
			Bound::Excluded(v) => Bound::Excluded(CdcEventKey {
				version: v,
				sequence: u16::MAX,
			}),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end_key = match end {
			Bound::Included(v) => Bound::Included(CdcEventKey {
				version: v,
				sequence: u16::MAX,
			}),
			Bound::Excluded(v) => Bound::Excluded(CdcEventKey {
				version: v.saturating_sub(1),
				sequence: u16::MAX,
			}),
			Bound::Unbounded => Bound::Unbounded,
		};

		Ok(Range {
			iter: Box::new(
				self.cdc_events.range((start_key, end_key)),
			),
		})
	}
}

pub struct Range<'a> {
	iter: Box<dyn Iterator<Item = Entry<'a, CdcEventKey, CdcEvent>> + 'a>,
}

impl<'a> Iterator for Range<'a> {
	type Item = CdcEvent;

	fn next(&mut self) -> Option<Self::Item> {
		self.iter.next().map(|entry| entry.value().clone())
	}
}
