// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use crossbeam_skiplist::map::Entry;
use reifydb_core::{
	Version,
	interface::{
		CdcCount, CdcEvent, CdcGet, CdcRange, CdcScan, CdcStorage,
	},
};

use crate::lmdb::Lmdb;

pub struct Range<'a> {
	iter: Box<dyn Iterator<Item = Entry<'a, Version, Vec<CdcEvent>>> + 'a>,
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
		if let Some(entry) = self.iter.next() {
			self.current_events = entry.value().clone();
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

pub struct Scan<'a> {
	iter: Box<dyn Iterator<Item = Entry<'a, Version, Vec<CdcEvent>>> + 'a>,
	current_events: Vec<CdcEvent>,
	current_index: usize,
}

impl<'a> Iterator for Scan<'a> {
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
		if let Some(entry) = self.iter.next() {
			self.current_events = entry.value().clone();
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

impl CdcGet for Lmdb {
	fn get(&self, _version: Version) -> crate::Result<Vec<CdcEvent>> {
		todo!()
	}
}

impl CdcRange for Lmdb {
	type RangeIter<'a> = Range<'a>;

	fn range(
		&self,
		_start: Bound<Version>,
		_end: Bound<Version>,
	) -> crate::Result<Self::RangeIter<'_>> {
		todo!()
	}
}

impl CdcScan for Lmdb {
	type ScanIter<'a> = Scan<'a>;

	fn scan(&self) -> crate::Result<Self::ScanIter<'_>> {
		todo!()
	}
}

impl CdcCount for Lmdb {
	fn count(&self, _version: Version) -> crate::Result<usize> {
		todo!()
	}
}

impl CdcStorage for Lmdb {}
