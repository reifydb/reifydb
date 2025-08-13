// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crossbeam_skiplist::map::Entry;
use reifydb_core::{
	Result,
	interface::{CdcEvent, CdcEventKey, CdcScan},
};

use crate::memory::Memory;

impl CdcScan for Memory {
	type ScanIter<'a> = Scan<'a>;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		Ok(Scan {
			iter: Box::new(self.cdc_events.iter()),
		})
	}
}

pub struct Scan<'a> {
	iter: Box<dyn Iterator<Item = Entry<'a, CdcEventKey, CdcEvent>> + 'a>,
}

impl<'a> Iterator for Scan<'a> {
	type Item = CdcEvent;

	fn next(&mut self) -> Option<Self::Item> {
		self.iter.next().map(|entry| entry.value().clone())
	}
}
