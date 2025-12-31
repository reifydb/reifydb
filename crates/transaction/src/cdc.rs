// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ops::Bound;

use async_trait::async_trait;
use reifydb_core::{
	CommitVersion, Result,
	interface::{Cdc, CdcBatch, CdcQueryTransaction, CdcTransaction},
};
use reifydb_store_transaction::{CdcCount, CdcGet, CdcRange, TransactionStore};

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
}

impl CdcTransaction for TransactionCdc {
	type Query<'a>
		= StandardCdcQueryTransaction
	where
		Self: 'a;

	fn begin_query(&self) -> Result<Self::Query<'_>> {
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

#[async_trait]
impl CdcQueryTransaction for StandardCdcQueryTransaction {
	async fn get(&self, version: CommitVersion) -> Result<Option<Cdc>> {
		CdcGet::get(&self.store, version).await
	}

	async fn range_batch(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> Result<CdcBatch> {
		let store_batch = CdcRange::range_batch(&self.store, start, end, batch_size).await?;
		Ok(CdcBatch {
			items: store_batch.items,
			has_more: store_batch.has_more,
		})
	}

	async fn count(&self, version: CommitVersion) -> Result<usize> {
		CdcCount::count(&self.store, version).await
	}
}
