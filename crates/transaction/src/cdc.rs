// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ops::Bound;

use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcBatch},
};
use reifydb_store_transaction::{CdcCount, CdcGet, CdcRange, TransactionStore};

pub trait CdcQueryTransaction: Send + Sync + Clone + 'static {
	fn get(&self, version: CommitVersion) -> Result<Option<Cdc>>;

	fn range_batch(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> Result<CdcBatch>;

	fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> Result<CdcBatch> {
		self.range_batch(start, end, 1024)
	}

	fn scan(&self, batch_size: u64) -> Result<CdcBatch> {
		self.range_batch(Bound::Unbounded, Bound::Unbounded, batch_size)
	}

	fn count(&self, version: CommitVersion) -> Result<usize>;
}

#[derive(Clone)]
pub struct TransactionCdc {
	store: TransactionStore,
}

impl TransactionCdc {
	pub fn new(store: TransactionStore) -> Self {
		Self {
			store,
		}
	}

	pub fn begin_query(&self) -> Result<StandardCdcQueryTransaction> {
		Ok(StandardCdcQueryTransaction::new(self.store.clone()))
	}
}

#[derive(Clone)]
pub struct StandardCdcQueryTransaction {
	store: TransactionStore,
}

impl StandardCdcQueryTransaction {
	pub fn new(store: TransactionStore) -> Self {
		Self {
			store,
		}
	}
}

impl CdcQueryTransaction for StandardCdcQueryTransaction {
	fn get(&self, version: CommitVersion) -> Result<Option<Cdc>> {
		CdcGet::get(&self.store, version)
	}

	fn range_batch(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> Result<CdcBatch> {
		let store_batch = CdcRange::range_batch(&self.store, start, end, batch_size)?;
		Ok(CdcBatch {
			items: store_batch.items,
			has_more: store_batch.has_more,
		})
	}

	fn count(&self, version: CommitVersion) -> Result<usize> {
		CdcCount::count(&self.store, version)
	}
}
