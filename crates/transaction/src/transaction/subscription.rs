// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::{
		encoded::EncodedValues,
		key::{EncodedKey, EncodedKeyRange},
	},
	event::EventBus,
	interface::{
		WithEventBus,
		change::Change,
		store::{MultiVersionBatch, MultiVersionValues},
	},
};
use reifydb_type::{
	Result,
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};

use crate::{
	TransactionId,
	change::{RowChange, TransactionalChanges, TransactionalDefChanges},
	interceptor::{
		WithInterceptors,
		chain::InterceptorChain as Chain,
		dictionary::{DictionaryPostInsertInterceptor, DictionaryPreInsertInterceptor},
		interceptors::Interceptors,
		namespace::{
			NamespacePostCreateInterceptor, NamespacePostUpdateInterceptor, NamespacePreDeleteInterceptor,
			NamespacePreUpdateInterceptor,
		},
		ringbuffer::{
			RingBufferPostDeleteInterceptor, RingBufferPostInsertInterceptor,
			RingBufferPostUpdateInterceptor, RingBufferPreDeleteInterceptor,
			RingBufferPreInsertInterceptor, RingBufferPreUpdateInterceptor,
		},
		ringbuffer_def::{
			RingBufferDefPostCreateInterceptor, RingBufferDefPostUpdateInterceptor,
			RingBufferDefPreDeleteInterceptor, RingBufferDefPreUpdateInterceptor,
		},
		series::{
			SeriesPostDeleteInterceptor, SeriesPostInsertInterceptor, SeriesPostUpdateInterceptor,
			SeriesPreDeleteInterceptor, SeriesPreInsertInterceptor, SeriesPreUpdateInterceptor,
		},
		table::{
			TablePostDeleteInterceptor, TablePostInsertInterceptor, TablePostUpdateInterceptor,
			TablePreDeleteInterceptor, TablePreInsertInterceptor, TablePreUpdateInterceptor,
		},
		table_def::{
			TableDefPostCreateInterceptor, TableDefPostUpdateInterceptor, TableDefPreDeleteInterceptor,
			TableDefPreUpdateInterceptor,
		},
		transaction::{PostCommitInterceptor, PreCommitInterceptor},
		view::{
			ViewPostDeleteInterceptor, ViewPostInsertInterceptor, ViewPostUpdateInterceptor,
			ViewPreDeleteInterceptor, ViewPreInsertInterceptor, ViewPreUpdateInterceptor,
		},
		view_def::{
			ViewDefPostCreateInterceptor, ViewDefPostUpdateInterceptor, ViewDefPreDeleteInterceptor,
			ViewDefPreUpdateInterceptor,
		},
	},
	multi::{pending::PendingWrites, transaction::MultiTransaction},
	single::{SingleTransaction, read::SingleReadTransaction, write::SingleWriteTransaction},
	transaction::{RqlExecutor, Transaction, admin::AdminTransaction},
};

/// A subscription transaction that wraps AdminTransaction with restricted access.
///
/// SubscriptionTransaction provides the same storage and change-tracking capabilities
/// as AdminTransaction but is intended for subscription-only DDL operations
/// (CREATE SUBSCRIPTION, DROP SUBSCRIPTION). The VM and executor enforce that only
/// subscription DDL is executed through this transaction type.
pub struct SubscriptionTransaction {
	pub(crate) inner: AdminTransaction,

	/// The identity executing this transaction — own field, NOT delegated to inner AdminTransaction.
	pub identity: IdentityId,

	/// Optional RQL executor — own field, NOT delegated to inner AdminTransaction.
	pub(crate) executor: Option<Arc<dyn RqlExecutor>>,
}

impl SubscriptionTransaction {
	/// Creates a new subscription transaction by delegating to AdminTransaction::new().
	pub fn new(
		multi: MultiTransaction,
		single: SingleTransaction,
		event_bus: EventBus,
		interceptors: Interceptors,
		identity: IdentityId,
	) -> Result<Self> {
		Ok(Self {
			inner: AdminTransaction::new(multi, single, event_bus, interceptors, identity)?,
			identity,
			executor: None,
		})
	}

	/// Set the RQL executor for this transaction.
	pub fn set_executor(&mut self, executor: Arc<dyn RqlExecutor>) {
		self.executor = Some(executor);
	}

	/// Execute RQL within this transaction using the attached executor.
	///
	/// Panics if no `RqlExecutor` has been set on this transaction.
	pub fn rql(&mut self, rql: &str, params: Params) -> Result<Vec<Frame>> {
		self.inner.check_active()?;
		let executor = self.executor.clone().expect("RqlExecutor not set");
		let result = executor.rql(&mut Transaction::Subscription(self), rql, params);
		if let Err(ref e) = result {
			self.inner.poison(e.0.clone());
		}
		result
	}

	/// Get access to the inner AdminTransaction (immutable).
	pub fn as_admin(&self) -> &AdminTransaction {
		&self.inner
	}

	/// Get access to the inner AdminTransaction for instruction functions
	/// that require `&mut AdminTransaction`.
	pub fn as_admin_mut(&mut self) -> &mut AdminTransaction {
		&mut self.inner
	}

	pub fn commit(&mut self) -> Result<CommitVersion> {
		self.inner.commit()
	}

	pub fn rollback(&mut self) -> Result<()> {
		self.inner.rollback()
	}

	pub fn event_bus(&self) -> &EventBus {
		self.inner.event_bus()
	}

	pub fn pending_writes(&self) -> &PendingWrites {
		self.inner.pending_writes()
	}

	pub fn version(&self) -> CommitVersion {
		self.inner.version()
	}

	pub fn id(&self) -> TransactionId {
		self.inner.id()
	}

	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>> {
		self.inner.get(key)
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		self.inner.contains_key(key)
	}

	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.inner.prefix(prefix)
	}

	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.inner.prefix_rev(prefix)
	}

	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		self.inner.read_as_of_version_exclusive(version)
	}

	pub fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> Result<()> {
		self.inner.set(key, row)
	}

	pub fn unset(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<()> {
		self.inner.unset(key, values)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.inner.remove(key)
	}

	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_>> {
		self.inner.range(range, batch_size)
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_>> {
		self.inner.range_rev(range, batch_size)
	}

	pub fn begin_single_query<'a, I>(&self, keys: I) -> Result<SingleReadTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.inner.begin_single_query(keys)
	}

	pub fn begin_single_command<'a, I>(&self, keys: I) -> Result<SingleWriteTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.inner.begin_single_command(keys)
	}

	pub fn get_changes(&self) -> &TransactionalDefChanges {
		self.inner.get_changes()
	}

	pub fn track_row_change(&mut self, change: RowChange) {
		self.inner.track_row_change(change)
	}

	pub fn track_flow_change(&mut self, change: Change) {
		self.inner.track_flow_change(change)
	}
}

impl WithEventBus for SubscriptionTransaction {
	fn event_bus(&self) -> &EventBus {
		self.inner.event_bus()
	}
}

impl WithInterceptors for SubscriptionTransaction {
	fn table_pre_insert_interceptors(&mut self) -> &mut Chain<dyn TablePreInsertInterceptor + Send + Sync> {
		self.inner.table_pre_insert_interceptors()
	}

	fn table_post_insert_interceptors(&mut self) -> &mut Chain<dyn TablePostInsertInterceptor + Send + Sync> {
		self.inner.table_post_insert_interceptors()
	}

	fn table_pre_update_interceptors(&mut self) -> &mut Chain<dyn TablePreUpdateInterceptor + Send + Sync> {
		self.inner.table_pre_update_interceptors()
	}

	fn table_post_update_interceptors(&mut self) -> &mut Chain<dyn TablePostUpdateInterceptor + Send + Sync> {
		self.inner.table_post_update_interceptors()
	}

	fn table_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TablePreDeleteInterceptor + Send + Sync> {
		self.inner.table_pre_delete_interceptors()
	}

	fn table_post_delete_interceptors(&mut self) -> &mut Chain<dyn TablePostDeleteInterceptor + Send + Sync> {
		self.inner.table_post_delete_interceptors()
	}

	fn ringbuffer_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreInsertInterceptor + Send + Sync> {
		self.inner.ringbuffer_pre_insert_interceptors()
	}

	fn ringbuffer_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostInsertInterceptor + Send + Sync> {
		self.inner.ringbuffer_post_insert_interceptors()
	}

	fn ringbuffer_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreUpdateInterceptor + Send + Sync> {
		self.inner.ringbuffer_pre_update_interceptors()
	}

	fn ringbuffer_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostUpdateInterceptor + Send + Sync> {
		self.inner.ringbuffer_post_update_interceptors()
	}

	fn ringbuffer_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreDeleteInterceptor + Send + Sync> {
		self.inner.ringbuffer_pre_delete_interceptors()
	}

	fn ringbuffer_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostDeleteInterceptor + Send + Sync> {
		self.inner.ringbuffer_post_delete_interceptors()
	}

	fn pre_commit_interceptors(&mut self) -> &mut Chain<dyn PreCommitInterceptor + Send + Sync> {
		self.inner.pre_commit_interceptors()
	}

	fn post_commit_interceptors(&mut self) -> &mut Chain<dyn PostCommitInterceptor + Send + Sync> {
		self.inner.post_commit_interceptors()
	}

	fn namespace_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespacePostCreateInterceptor + Send + Sync> {
		self.inner.namespace_post_create_interceptors()
	}

	fn namespace_pre_update_interceptors(&mut self) -> &mut Chain<dyn NamespacePreUpdateInterceptor + Send + Sync> {
		self.inner.namespace_pre_update_interceptors()
	}

	fn namespace_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespacePostUpdateInterceptor + Send + Sync> {
		self.inner.namespace_post_update_interceptors()
	}

	fn namespace_pre_delete_interceptors(&mut self) -> &mut Chain<dyn NamespacePreDeleteInterceptor + Send + Sync> {
		self.inner.namespace_pre_delete_interceptors()
	}

	fn table_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn TableDefPostCreateInterceptor + Send + Sync> {
		self.inner.table_def_post_create_interceptors()
	}

	fn table_def_pre_update_interceptors(&mut self) -> &mut Chain<dyn TableDefPreUpdateInterceptor + Send + Sync> {
		self.inner.table_def_pre_update_interceptors()
	}

	fn table_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn TableDefPostUpdateInterceptor + Send + Sync> {
		self.inner.table_def_post_update_interceptors()
	}

	fn table_def_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TableDefPreDeleteInterceptor + Send + Sync> {
		self.inner.table_def_pre_delete_interceptors()
	}

	fn view_pre_insert_interceptors(&mut self) -> &mut Chain<dyn ViewPreInsertInterceptor + Send + Sync> {
		self.inner.view_pre_insert_interceptors()
	}

	fn view_post_insert_interceptors(&mut self) -> &mut Chain<dyn ViewPostInsertInterceptor + Send + Sync> {
		self.inner.view_post_insert_interceptors()
	}

	fn view_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewPreUpdateInterceptor + Send + Sync> {
		self.inner.view_pre_update_interceptors()
	}

	fn view_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewPostUpdateInterceptor + Send + Sync> {
		self.inner.view_post_update_interceptors()
	}

	fn view_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewPreDeleteInterceptor + Send + Sync> {
		self.inner.view_pre_delete_interceptors()
	}

	fn view_post_delete_interceptors(&mut self) -> &mut Chain<dyn ViewPostDeleteInterceptor + Send + Sync> {
		self.inner.view_post_delete_interceptors()
	}

	fn view_def_post_create_interceptors(&mut self) -> &mut Chain<dyn ViewDefPostCreateInterceptor + Send + Sync> {
		self.inner.view_def_post_create_interceptors()
	}

	fn view_def_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewDefPreUpdateInterceptor + Send + Sync> {
		self.inner.view_def_pre_update_interceptors()
	}

	fn view_def_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewDefPostUpdateInterceptor + Send + Sync> {
		self.inner.view_def_post_update_interceptors()
	}

	fn view_def_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewDefPreDeleteInterceptor + Send + Sync> {
		self.inner.view_def_pre_delete_interceptors()
	}

	fn ringbuffer_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPostCreateInterceptor + Send + Sync> {
		self.inner.ringbuffer_def_post_create_interceptors()
	}

	fn ringbuffer_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPreUpdateInterceptor + Send + Sync> {
		self.inner.ringbuffer_def_pre_update_interceptors()
	}

	fn ringbuffer_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPostUpdateInterceptor + Send + Sync> {
		self.inner.ringbuffer_def_post_update_interceptors()
	}

	fn ringbuffer_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPreDeleteInterceptor + Send + Sync> {
		self.inner.ringbuffer_def_pre_delete_interceptors()
	}

	fn dictionary_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPreInsertInterceptor + Send + Sync> {
		self.inner.dictionary_pre_insert_interceptors()
	}

	fn dictionary_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPostInsertInterceptor + Send + Sync> {
		self.inner.dictionary_post_insert_interceptors()
	}

	fn series_pre_insert_interceptors(&mut self) -> &mut Chain<dyn SeriesPreInsertInterceptor + Send + Sync> {
		self.inner.series_pre_insert_interceptors()
	}

	fn series_post_insert_interceptors(&mut self) -> &mut Chain<dyn SeriesPostInsertInterceptor + Send + Sync> {
		self.inner.series_post_insert_interceptors()
	}

	fn series_pre_update_interceptors(&mut self) -> &mut Chain<dyn SeriesPreUpdateInterceptor + Send + Sync> {
		self.inner.series_pre_update_interceptors()
	}

	fn series_post_update_interceptors(&mut self) -> &mut Chain<dyn SeriesPostUpdateInterceptor + Send + Sync> {
		self.inner.series_post_update_interceptors()
	}

	fn series_pre_delete_interceptors(&mut self) -> &mut Chain<dyn SeriesPreDeleteInterceptor + Send + Sync> {
		self.inner.series_pre_delete_interceptors()
	}

	fn series_post_delete_interceptors(&mut self) -> &mut Chain<dyn SeriesPostDeleteInterceptor + Send + Sync> {
		self.inner.series_post_delete_interceptors()
	}
}

impl TransactionalChanges for SubscriptionTransaction {}
