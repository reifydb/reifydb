// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::BTreeMap, ops::Bound, sync::Arc};

use parking_lot::RwLockReadGuard;
use reifydb_core::{CommitVersion, Result, interface::Cdc};

use crate::{
	CdcRange,
	cdc::{InternalCdc, converter::CdcConverter},
	memory::MemoryBackend,
};

impl CdcRange for MemoryBackend {
	type RangeIter<'a> = CdcRangeIter<'a>;

	fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> Result<Self::RangeIter<'_>> {
		let guard = self.cdc.read();
		// SAFETY: We extend the lifetime to match the guard, which is held in the iterator
		let iter = unsafe { std::mem::transmute((*guard).range((start, end))) };
		Ok(CdcRangeIter {
			backend: Arc::new(self.clone()),
			_guard: guard,
			iter,
		})
	}
}

pub struct CdcRangeIter<'a> {
	backend: Arc<MemoryBackend>,
	_guard: RwLockReadGuard<'a, BTreeMap<CommitVersion, InternalCdc>>,
	iter: std::collections::btree_map::Range<'a, CommitVersion, InternalCdc>,
}

// SAFETY: We need to manually implement Send for the iterator
// The guard ensures the map stays valid for the iterator's lifetime
unsafe impl<'a> Send for CdcRangeIter<'a> {}

impl<'a> Iterator for CdcRangeIter<'a> {
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		self.iter.next().and_then(|(_, internal_cdc)| self.backend.convert(internal_cdc.clone()).ok())
	}
}
