// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ops::Bound;

use crossbeam_skiplist::map::Entry;
use reifydb_core::{
	Result, Version,
	interface::{CdcEvent, CdcRange},
};

use crate::{cdc::CdcTransaction, memory::Memory};

impl CdcRange for Memory {
	type RangeIter<'a> = Range<'a>;

	fn range(
		&self,
		start: Bound<Version>,
		end: Bound<Version>,
	) -> Result<Self::RangeIter<'_>> {
		Ok(Range {
			version_iter: Box::new(
				self.cdc_transactions.range((start, end)),
			),
			current_events: vec![],
			current_index: 0,
		})
	}
}

pub struct Range<'a> {
	version_iter: Box<
		dyn Iterator<Item = Entry<'a, Version, CdcTransaction>> + 'a,
	>,
	current_events: Vec<CdcEvent>,
	current_index: usize,
}

impl<'a> Iterator for Range<'a> {
	type Item = CdcEvent;

	fn next(&mut self) -> Option<Self::Item> {
		// If we have events in the current batch, return the next one
		if self.current_index < self.current_events.len() {
			let event =
				self.current_events[self.current_index].clone();
			self.current_index += 1;
			return Some(event);
		}

		// Otherwise, get the next version's events
		if let Some(entry) = self.version_iter.next() {
			self.current_events =
				entry.value().to_events().collect();
			self.current_index = 0;

			// Recursively call next() to get the first event from
			// the new batch
			if !self.current_events.is_empty() {
				self.next()
			} else {
				// Empty batch, try next version
				self.next()
			}
		} else {
			None
		}
	}
}
