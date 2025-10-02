// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::RwLockReadGuard;

use reifydb_core::interface::{BoxedSingleVersionIter, SingleVersionQueryTransaction};

use super::*;

pub struct SvlReadTransaction<'a, SVS> {
	pub(super) storage: RwLockReadGuard<'a, SVS>,
}

impl<SVS> SingleVersionQueryTransaction for SvlReadTransaction<'_, SVS>
where
	SVS: SingleVersionStore,
{
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<SingleVersionValues>> {
		self.storage.get(key)
	}

	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.storage.contains(key)
	}

	fn scan(&mut self) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.storage.scan()?;
		Ok(Box::new(iter.into_iter()))
	}

	fn scan_rev(&mut self) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.storage.scan_rev()?;
		Ok(Box::new(iter.into_iter()))
	}

	fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.storage.range(range)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.storage.range_rev(range)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.storage.prefix(prefix)?;
		Ok(Box::new(iter.into_iter()))
	}

	fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedSingleVersionIter> {
		let iter = self.storage.prefix_rev(prefix)?;
		Ok(Box::new(iter.into_iter()))
	}
}
