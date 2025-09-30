// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crossbeam_skiplist::map::Entry;
use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcScan},
};

use crate::memory::Memory;

impl CdcScan for Memory {
	type ScanIter<'a> = Scan<'a>;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		Ok(Scan {
			version_iter: Box::new(self.cdcs.iter()),
		})
	}
}

pub struct Scan<'a> {
	version_iter: Box<dyn Iterator<Item = Entry<'a, CommitVersion, Cdc>> + 'a>,
}

impl<'a> Iterator for Scan<'a> {
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		// Get the next transaction
		if let Some(entry) = self.version_iter.next() {
			Some(entry.value().clone())
		} else {
			None
		}
	}
}
