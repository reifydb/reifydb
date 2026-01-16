// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{
			dictionary::DictionaryDef,
			flow::{FlowDef, FlowId},
			id::{DictionaryId, NamespaceId, RingBufferId, SubscriptionId, TableId, ViewId},
			namespace::NamespaceDef,
			ringbuffer::RingBufferDef,
			subscription::SubscriptionDef,
			table::TableDef,
			view::ViewDef,
		},
		store::{MultiVersionBatch, MultiVersionValues},
	},
	value::encoded::key::{EncodedKey, EncodedKeyRange},
};
use reifydb_type::Result;
use tracing::instrument;

use crate::{
	TransactionId,
	change::{
		TransactionalChanges, TransactionalDictionaryChanges, TransactionalFlowChanges,
		TransactionalNamespaceChanges, TransactionalRingBufferChanges, TransactionalSubscriptionChanges,
		TransactionalTableChanges, TransactionalViewChanges,
	},
	multi::transaction::query::QueryTransaction,
	single::{TransactionSingle, svl::read::SvlQueryTransaction},
};

/// An active query transaction that holds a multi query transaction
/// and provides query-only access to single storage.
pub struct StandardQueryTransaction {
	pub(crate) multi: QueryTransaction,
	pub(crate) single: TransactionSingle,
}

impl StandardQueryTransaction {
	/// Creates a new active query transaction
	#[instrument(name = "transaction::standard::query::new", level = "debug", skip_all)]
	pub fn new(multi: QueryTransaction, single: TransactionSingle) -> Self {
		Self {
			multi,
			single,
		}
	}

	/// Get the transaction version
	#[inline]
	pub fn version(&self) -> CommitVersion {
		self.multi.version()
	}

	/// Get the transaction ID
	#[inline]
	pub fn id(&self) -> TransactionId {
		self.multi.tm.id()
	}

	/// Get a value by key
	#[inline]
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>> {
		Ok(self.multi.get(key)?.map(|v| v.into_multi_version_values()))
	}

	/// Check if a key exists
	#[inline]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		self.multi.contains_key(key)
	}

	/// Get a prefix batch
	#[inline]
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.multi.prefix(prefix)
	}

	/// Get a reverse prefix batch
	#[inline]
	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.multi.prefix_rev(prefix)
	}

	/// Read as of version exclusive
	#[inline]
	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		self.multi.read_as_of_version_exclusive(version);
		Ok(())
	}

	/// Create a streaming iterator for forward range queries.
	#[inline]
	pub fn range(
		&self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_> {
		self.multi.range(range, batch_size)
	}

	/// Create a streaming iterator for reverse range queries.
	#[inline]
	pub fn range_rev(
		&self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_> {
		self.multi.range_rev(range, batch_size)
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "transaction::standard::query::with_single_query", level = "trace", skip(self, keys, f))]
	pub fn with_single_query<'a, I, F, R>(&self, keys: I, f: F) -> Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut SvlQueryTransaction<'_>) -> Result<R> + Send,
		R: Send,
	{
		self.single.with_query(keys, f)
	}

	/// Execute a function with access to the multi query transaction.
	/// This operates within the same transaction context.
	#[instrument(name = "transaction::standard::query::with_multi_query", level = "trace", skip(self, f))]
	pub fn with_multi_query<F, R>(&mut self, f: F) -> Result<R>
	where
		F: FnOnce(&mut QueryTransaction) -> Result<R>,
	{
		f(&mut self.multi)
	}

	/// Begin a single-version query transaction for specific keys
	#[instrument(name = "transaction::standard::query::begin_single_query", level = "trace", skip(self, keys))]
	pub fn begin_single_query<'a, I>(&self, keys: I) -> Result<SvlQueryTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.single.begin_query(keys)
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

impl TransactionalSubscriptionChanges for StandardQueryTransaction {
	fn find_subscription(&self, _id: SubscriptionId) -> Option<&SubscriptionDef> {
		None
	}

	fn is_subscription_deleted(&self, _id: SubscriptionId) -> bool {
		false
	}
}

impl TransactionalChanges for StandardQueryTransaction {}
