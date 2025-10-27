// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::BTreeMap;
use parking_lot::RwLockReadGuard;
use reifydb_core::{CommitVersion, Result, interface::Cdc};

use crate::{CdcScan, memory::MemoryBackend};

impl CdcScan for MemoryBackend {
	type ScanIter<'a> = CdcScanIter<'a>;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		let guard = self.cdc.read();
		// SAFETY: We extend the lifetime to match the guard, which is held in the iterator
		let iter = unsafe { std::mem::transmute((*guard).iter()) };
		Ok(CdcScanIter {
			_guard: guard,
			iter,
		})
	}
}

pub struct CdcScanIter<'a> {
	_guard: RwLockReadGuard<'a, BTreeMap<CommitVersion, Cdc>>,
	iter: std::collections::btree_map::Iter<'a, CommitVersion, Cdc>,
}

// SAFETY: We need to manually implement Send for the iterator
// The guard ensures the map stays valid for the iterator's lifetime
unsafe impl<'a> Send for CdcScanIter<'a> {}

impl<'a> Iterator for CdcScanIter<'a> {
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		// Get the next transaction
		self.iter.next().map(|(_, cdc)| cdc.clone())
	}
}
