// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::Bound;

use reifydb_core::{CommitVersion, interface::Cdc};

use crate::{
	CdcStore, StandardTransactionStore,
	cdc::{CdcCount, CdcGet, CdcRange, CdcScan},
	store::cdc_iterator::CdcMergingIterator,
};

impl CdcGet for StandardTransactionStore {
	fn get(&self, version: CommitVersion) -> reifydb_type::Result<Option<Cdc>> {
		if let Some(hot) = &self.hot {
			if let Some(result) = hot.cdc.get(version)? {
				return Ok(Some(result));
			}
		}

		if let Some(warm) = &self.warm {
			if let Some(result) = warm.cdc.get(version)? {
				return Ok(Some(result));
			}
		}

		if let Some(cold) = &self.cold {
			if let Some(result) = cold.cdc.get(version)? {
				return Ok(Some(result));
			}
		}

		Ok(None)
	}
}

impl CdcRange for StandardTransactionStore {
	type RangeIter<'a> = Box<dyn Iterator<Item = Cdc> + 'a>;

	fn range(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
	) -> reifydb_type::Result<Self::RangeIter<'_>> {
		let mut iters: Vec<Box<dyn Iterator<Item = Cdc> + '_>> = Vec::new();

		if let Some(hot) = &self.hot {
			let iter = hot.cdc.range(start, end)?;
			iters.push(Box::new(iter));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.cdc.range(start, end)?;
			iters.push(Box::new(iter));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.cdc.range(start, end)?;
			iters.push(Box::new(iter));
		}

		Ok(Box::new(CdcMergingIterator::new(iters)))
	}
}

impl CdcScan for StandardTransactionStore {
	type ScanIter<'a> = Box<dyn Iterator<Item = Cdc> + 'a>;

	fn scan(&self) -> reifydb_type::Result<Self::ScanIter<'_>> {
		let mut iters: Vec<Box<dyn Iterator<Item = Cdc> + '_>> = Vec::new();

		if let Some(hot) = &self.hot {
			let iter = hot.cdc.scan()?;
			iters.push(Box::new(iter));
		}

		if let Some(warm) = &self.warm {
			let iter = warm.cdc.scan()?;
			iters.push(Box::new(iter));
		}

		if let Some(cold) = &self.cold {
			let iter = cold.cdc.scan()?;
			iters.push(Box::new(iter));
		}

		Ok(Box::new(CdcMergingIterator::new(iters)))
	}
}

impl CdcCount for StandardTransactionStore {
	fn count(&self, version: CommitVersion) -> reifydb_type::Result<usize> {
		if let Some(hot) = &self.hot {
			let count = hot.cdc.count(version)?;
			if count > 0 {
				return Ok(count);
			}
		}

		if let Some(warm) = &self.warm {
			let count = warm.cdc.count(version)?;
			if count > 0 {
				return Ok(count);
			}
		}

		if let Some(cold) = &self.cold {
			let count = cold.cdc.count(version)?;
			if count > 0 {
				return Ok(count);
			}
		}

		Ok(0)
	}
}

impl CdcStore for StandardTransactionStore {}
