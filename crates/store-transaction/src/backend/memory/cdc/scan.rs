// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::BTreeMap, sync::Arc};
use parking_lot::RwLockReadGuard;
use reifydb_core::{CommitVersion, Result, interface::Cdc};

use crate::{CdcScan, memory::MemoryBackend, cdc::{InternalCdc, converter::CdcConverter}};

impl CdcScan for MemoryBackend {
	type ScanIter<'a> = CdcScanIter<'a>;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		let guard = self.cdc.read();
		// SAFETY: We extend the lifetime to match the guard, which is held in the iterator
		let iter = unsafe { std::mem::transmute((*guard).iter()) };
		Ok(CdcScanIter {
			backend: Arc::new(self.clone()),
			_guard: guard,
			iter,
		})
	}
}

pub struct CdcScanIter<'a> {
	backend: Arc<MemoryBackend>,
	_guard: RwLockReadGuard<'a, BTreeMap<CommitVersion, InternalCdc>>,
	iter: std::collections::btree_map::Iter<'a, CommitVersion, InternalCdc>,
}

// SAFETY: We need to manually implement Send for the iterator
// The guard ensures the map stays valid for the iterator's lifetime
unsafe impl<'a> Send for CdcScanIter<'a> {}

impl<'a> Iterator for CdcScanIter<'a> {
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		// Get the next transaction and convert to public format
		self.iter.next().and_then(|(_, internal_cdc)| {
			self.backend.convert(internal_cdc.clone()).ok()
		})
	}
}
