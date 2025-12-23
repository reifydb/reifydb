// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_catalog::{MaterializedCatalog, transaction::MaterializedCatalogTransaction};
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	interface::{
		CdcTransaction, MultiVersionBatch, MultiVersionQueryTransaction, MultiVersionTransaction,
		MultiVersionValues, QueryTransaction, SingleVersionTransaction, TransactionId, TransactionalChanges,
	},
};
use reifydb_transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingle};
use tracing::instrument;

/// An active query transaction that holds a multi query transaction
/// and provides query-only access to single storage.
pub struct StandardQueryTransaction {
	pub(crate) multi: <TransactionMultiVersion as MultiVersionTransaction>::Query,
	pub(crate) single: TransactionSingle,
	pub(crate) cdc: TransactionCdc,
	pub(crate) catalog: MaterializedCatalog,
}

impl StandardQueryTransaction {
	/// Creates a new active query transaction
	#[instrument(name = "engine::transaction::query::new", level = "debug", skip_all)]
	pub fn new(
		multi: <TransactionMultiVersion as MultiVersionTransaction>::Query,
		single: TransactionSingle,
		cdc: TransactionCdc,
		catalog: MaterializedCatalog,
	) -> Self {
		Self {
			multi,
			single,
			cdc,
			catalog,
		}
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "engine::transaction::query::with_single_query", level = "trace", skip(self, keys, f))]
	pub async fn with_single_query<'a, I, F, R>(&self, keys: I, f: F) -> crate::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut <TransactionSingle as SingleVersionTransaction>::Query<'_>) -> crate::Result<R> + Send,
		R: Send,
	{
		self.single.with_query(keys, f).await
	}

	/// Execute a function with access to the multi query transaction.
	/// This operates within the same transaction context.
	#[instrument(name = "engine::transaction::query::with_multi_query", level = "trace", skip(self, f))]
	pub fn with_multi_query<F, R>(&mut self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut <TransactionMultiVersion as MultiVersionTransaction>::Query) -> crate::Result<R>,
	{
		f(&mut self.multi)
	}

	/// Get access to the CDC transaction interface
	#[instrument(name = "engine::transaction::query::cdc", level = "trace", skip(self))]
	pub fn cdc(&self) -> &TransactionCdc {
		&self.cdc
	}
}

#[async_trait]
impl MultiVersionQueryTransaction for StandardQueryTransaction {
	#[inline]
	fn version(&self) -> CommitVersion {
		MultiVersionQueryTransaction::version(&self.multi)
	}

	#[inline]
	fn id(&self) -> TransactionId {
		MultiVersionQueryTransaction::id(&self.multi)
	}

	#[inline]
	async fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionValues>> {
		MultiVersionQueryTransaction::get(&mut self.multi, key).await
	}

	#[inline]
	async fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		MultiVersionQueryTransaction::contains_key(&mut self.multi, key).await
	}

	#[inline]
	async fn range_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> crate::Result<MultiVersionBatch> {
		MultiVersionQueryTransaction::range_batch(&mut self.multi, range, batch_size).await
	}

	#[inline]
	async fn range_rev_batch(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<MultiVersionBatch> {
		MultiVersionQueryTransaction::range_rev_batch(&mut self.multi, range, batch_size).await
	}

	#[inline]
	async fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> crate::Result<()> {
		MultiVersionQueryTransaction::read_as_of_version_exclusive(&mut self.multi, version).await
	}
}

#[async_trait]
impl QueryTransaction for StandardQueryTransaction {
	type SingleVersionQuery<'a> = <TransactionSingle as SingleVersionTransaction>::Query<'a>;
	type CdcQuery<'a>
		= <TransactionCdc as CdcTransaction>::Query<'a>
	where
		Self: 'a;

	async fn begin_single_query<'a, I>(&self, keys: I) -> crate::Result<Self::SingleVersionQuery<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		self.single.begin_query(keys).await
	}

	async fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>> {
		Ok(self.cdc.begin_query()?)
	}
}

impl MaterializedCatalogTransaction for StandardQueryTransaction {
	fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}
}

impl TransactionalChanges for StandardQueryTransaction {}
