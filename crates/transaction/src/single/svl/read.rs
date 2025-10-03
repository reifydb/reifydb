// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::RwLockReadGuard;

use reifydb_core::interface::{BoxedSingleVersionIter, SingleVersionQueryTransaction};
use reifydb_store_transaction::{
	SingleVersionContains, SingleVersionGet, SingleVersionRange, SingleVersionRangeRev, SingleVersionScan,
	SingleVersionScanRev,
};

use super::*;

pub struct SvlQueryTransaction<'a> {
	pub(super) store: RwLockReadGuard<'a, TransactionStore>,
}

impl SingleVersionQueryTransaction for SvlQueryTransaction<'_> {
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		self.store.get(key)
	}

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.store.contains(key)
	}

	fn scan(&mut self) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.store.scan()?;
		Ok(Box::new(iter.into_iter()))
	}

	fn scan_rev(&mut self) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.store.scan_rev()?;
		Ok(Box::new(iter.into_iter()))
	}

	fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.store.range(range)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.store.range_rev(range)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.store.prefix(prefix)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.store.prefix_rev(prefix)?;
		Ok(Box::new(iter.into_iter()))
	}
}
