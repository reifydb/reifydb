// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use async_trait::async_trait;
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	interface::{
		CdcTransaction, DictionaryDef, DictionaryId, FlowDef, FlowId, MultiVersionBatch,
		MultiVersionTransaction, MultiVersionValues, NamespaceDef, NamespaceId, QueryTransaction,
		RingBufferDef, RingBufferId, SingleVersionTransaction, TableDef, TableId, TransactionId,
		TransactionalChanges, TransactionalDictionaryChanges, TransactionalFlowChanges,
		TransactionalNamespaceChanges, TransactionalRingBufferChanges, TransactionalTableChanges,
		TransactionalViewChanges, ViewDef, ViewId,
	},
};
use reifydb_type::Result;
use tracing::instrument;

use crate::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingle};

/// An active query transaction that holds a multi query transaction
/// and provides query-only access to single storage.
pub struct StandardQueryTransaction {
	pub(crate) multi: <TransactionMultiVersion as MultiVersionTransaction>::Query,
	pub(crate) single: TransactionSingle,
	pub(crate) cdc: TransactionCdc,
}

impl StandardQueryTransaction {
	/// Creates a new active query transaction
	#[instrument(name = "transaction::standard::query::new", level = "debug", skip_all)]
	pub fn new(
		multi: <TransactionMultiVersion as MultiVersionTransaction>::Query,
		single: TransactionSingle,
		cdc: TransactionCdc,
	) -> Self {
		Self {
			multi,
			single,
			cdc,
		}
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "transaction::standard::query::with_single_query", level = "trace", skip(self, keys, f))]
	pub async fn with_single_query<'a, I, F, R>(&self, keys: I, f: F) -> Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut <TransactionSingle as SingleVersionTransaction>::Query<'_>) -> Result<R> + Send,
		R: Send,
	{
		self.single.with_query(keys, f).await
	}

	/// Execute a function with access to the multi query transaction.
	/// This operates within the same transaction context.
	#[instrument(name = "transaction::standard::query::with_multi_query", level = "trace", skip(self, f))]
	pub fn with_multi_query<F, R>(&mut self, f: F) -> Result<R>
	where
		F: FnOnce(&mut <TransactionMultiVersion as MultiVersionTransaction>::Query) -> Result<R>,
	{
		f(&mut self.multi)
	}

	/// Get access to the CDC transaction interface
	#[instrument(name = "transaction::standard::query::cdc", level = "trace", skip(self))]
	pub fn cdc(&self) -> &TransactionCdc {
		&self.cdc
	}

	/// Begin a single-version query transaction for specific keys
	#[instrument(name = "transaction::standard::query::begin_single_query", level = "trace", skip(self, keys))]
	pub async fn begin_single_query<'a, I>(
		&self,
		keys: I,
	) -> Result<<TransactionSingle as SingleVersionTransaction>::Query<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		self.single.begin_query(keys).await
	}

	/// Begin a CDC query transaction
	#[instrument(name = "transaction::standard::query::begin_cdc_query", level = "trace", skip(self))]
	pub async fn begin_cdc_query(&self) -> Result<<TransactionCdc as CdcTransaction>::Query<'_>> {
		Ok(self.cdc.begin_query()?)
	}
}

#[async_trait]
impl QueryTransaction for StandardQueryTransaction {
	type SingleVersionQuery<'a> = <TransactionSingle as SingleVersionTransaction>::Query<'a>;
	type CdcQuery<'a> = <TransactionCdc as CdcTransaction>::Query<'a>;

	#[inline]
	fn version(&self) -> CommitVersion {
		QueryTransaction::version(&self.multi)
	}

	#[inline]
	fn id(&self) -> TransactionId {
		QueryTransaction::id(&self.multi)
	}

	#[inline]
	async fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>> {
		QueryTransaction::get(&mut self.multi, key).await
	}

	#[inline]
	async fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		QueryTransaction::contains_key(&mut self.multi, key).await
	}

	#[inline]
	async fn range_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> Result<MultiVersionBatch> {
		QueryTransaction::range_batch(&mut self.multi, range, batch_size).await
	}

	#[inline]
	async fn range_rev_batch(&mut self, range: EncodedKeyRange, batch_size: u64) -> Result<MultiVersionBatch> {
		QueryTransaction::range_rev_batch(&mut self.multi, range, batch_size).await
	}

	#[inline]
	async fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		QueryTransaction::read_as_of_version_exclusive(&mut self.multi, version).await
	}

	async fn begin_single_query<'a, I>(&self, keys: I) -> Result<Self::SingleVersionQuery<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
	{
		self.single.begin_query(keys).await
	}

	async fn begin_cdc_query(&self) -> Result<Self::CdcQuery<'_>> {
		Ok(self.cdc.begin_query()?)
	}
}

// No-op implementations of TransactionalChanges for StandardQueryTransaction.
// Query transactions don't track changes, so all methods return None/false.

impl TransactionalDictionaryChanges for StandardQueryTransaction {
	fn find_dictionary(&self, _id: DictionaryId) -> Option<&DictionaryDef> {
		None
	}

	fn find_dictionary_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&DictionaryDef> {
		None
	}

	fn is_dictionary_deleted(&self, _id: DictionaryId) -> bool {
		false
	}

	fn is_dictionary_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalFlowChanges for StandardQueryTransaction {
	fn find_flow(&self, _id: FlowId) -> Option<&FlowDef> {
		None
	}

	fn find_flow_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&FlowDef> {
		None
	}

	fn is_flow_deleted(&self, _id: FlowId) -> bool {
		false
	}

	fn is_flow_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalNamespaceChanges for StandardQueryTransaction {
	fn find_namespace(&self, _id: NamespaceId) -> Option<&NamespaceDef> {
		None
	}

	fn find_namespace_by_name(&self, _name: &str) -> Option<&NamespaceDef> {
		None
	}

	fn is_namespace_deleted(&self, _id: NamespaceId) -> bool {
		false
	}

	fn is_namespace_deleted_by_name(&self, _name: &str) -> bool {
		false
	}
}

impl TransactionalRingBufferChanges for StandardQueryTransaction {
	fn find_ringbuffer(&self, _id: RingBufferId) -> Option<&RingBufferDef> {
		None
	}

	fn find_ringbuffer_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&RingBufferDef> {
		None
	}

	fn is_ringbuffer_deleted(&self, _id: RingBufferId) -> bool {
		false
	}

	fn is_ringbuffer_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalTableChanges for StandardQueryTransaction {
	fn find_table(&self, _id: TableId) -> Option<&TableDef> {
		None
	}

	fn find_table_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&TableDef> {
		None
	}

	fn is_table_deleted(&self, _id: TableId) -> bool {
		false
	}

	fn is_table_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalViewChanges for StandardQueryTransaction {
	fn find_view(&self, _id: ViewId) -> Option<&ViewDef> {
		None
	}

	fn find_view_by_name(&self, _namespace: NamespaceId, _name: &str) -> Option<&ViewDef> {
		None
	}

	fn is_view_deleted(&self, _id: ViewId) -> bool {
		false
	}

	fn is_view_deleted_by_name(&self, _namespace: NamespaceId, _name: &str) -> bool {
		false
	}
}

impl TransactionalChanges for StandardQueryTransaction {}
