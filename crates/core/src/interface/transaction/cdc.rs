// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ops::Bound;

use async_trait::async_trait;
use reifydb_type::Result;

use crate::{CommitVersion, interface::Cdc};

/// A batch of CDC entries with continuation info.
#[derive(Debug, Clone)]
pub struct CdcBatch {
	/// The CDC entries in this batch.
	pub items: Vec<Cdc>,
	/// Whether there are more items after this batch.
	pub has_more: bool,
}

impl CdcBatch {
	/// Creates an empty batch with no more results.
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	/// Returns true if this batch contains no items.
	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}

pub trait CdcTransaction: Send + Sync + Clone + 'static {
	type Query<'a>: CdcQueryTransaction;

	fn begin_query(&self) -> Result<Self::Query<'_>>;

	fn with_query<F, R>(&self, f: F) -> Result<R>
	where
		F: FnOnce(&mut Self::Query<'_>) -> Result<R>,
	{
		let mut tx = self.begin_query()?;
		f(&mut tx)
	}
}

#[async_trait]
pub trait CdcQueryTransaction: Send + Sync + Clone + 'static {
	async fn get(&self, version: CommitVersion) -> Result<Option<Cdc>>;

	async fn range_batch(
		&self,
		start: Bound<CommitVersion>,
		end: Bound<CommitVersion>,
		batch_size: u64,
	) -> Result<CdcBatch>;

	async fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> Result<CdcBatch> {
		self.range_batch(start, end, 1024).await
	}

	async fn scan(&self, batch_size: u64) -> Result<CdcBatch> {
		self.range_batch(Bound::Unbounded, Bound::Unbounded, batch_size).await
	}

	async fn count(&self, version: CommitVersion) -> Result<usize>;
}
