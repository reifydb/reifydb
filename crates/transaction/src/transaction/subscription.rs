// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	event::EventBus,
	interface::{
		WithEventBus,
		change::Change,
		store::{MultiVersionBatch, MultiVersionRow},
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
		authentication::{AuthenticationPostCreateInterceptor, AuthenticationPreDeleteInterceptor},
		chain::InterceptorChain as Chain,
		dictionary::{
			DictionaryPostCreateInterceptor, DictionaryPostUpdateInterceptor,
			DictionaryPreDeleteInterceptor, DictionaryPreUpdateInterceptor,
		},
		dictionary_row::{
			DictionaryRowPostDeleteInterceptor, DictionaryRowPostInsertInterceptor,
			DictionaryRowPostUpdateInterceptor, DictionaryRowPreDeleteInterceptor,
			DictionaryRowPreInsertInterceptor, DictionaryRowPreUpdateInterceptor,
		},
		granted_role::{GrantedRolePostCreateInterceptor, GrantedRolePreDeleteInterceptor},
		identity::{
			IdentityPostCreateInterceptor, IdentityPostUpdateInterceptor, IdentityPreDeleteInterceptor,
			IdentityPreUpdateInterceptor,
		},
		interceptors::Interceptors,
		namespace::{
			NamespacePostCreateInterceptor, NamespacePostUpdateInterceptor, NamespacePreDeleteInterceptor,
			NamespacePreUpdateInterceptor,
		},
		ringbuffer::{
			RingBufferPostCreateInterceptor, RingBufferPostUpdateInterceptor,
			RingBufferPreDeleteInterceptor, RingBufferPreUpdateInterceptor,
		},
		ringbuffer_row::{
			RingBufferRowPostDeleteInterceptor, RingBufferRowPostInsertInterceptor,
			RingBufferRowPostUpdateInterceptor, RingBufferRowPreDeleteInterceptor,
			RingBufferRowPreInsertInterceptor, RingBufferRowPreUpdateInterceptor,
		},
		role::{
			RolePostCreateInterceptor, RolePostUpdateInterceptor, RolePreDeleteInterceptor,
			RolePreUpdateInterceptor,
		},
		series::{
			SeriesPostCreateInterceptor, SeriesPostUpdateInterceptor, SeriesPreDeleteInterceptor,
			SeriesPreUpdateInterceptor,
		},
		series_row::{
			SeriesRowPostDeleteInterceptor, SeriesRowPostInsertInterceptor, SeriesRowPostUpdateInterceptor,
			SeriesRowPreDeleteInterceptor, SeriesRowPreInsertInterceptor, SeriesRowPreUpdateInterceptor,
		},
		table::{
			TablePostCreateInterceptor, TablePostUpdateInterceptor, TablePreDeleteInterceptor,
			TablePreUpdateInterceptor,
		},
		table_row::{
			TableRowPostDeleteInterceptor, TableRowPostInsertInterceptor, TableRowPostUpdateInterceptor,
			TableRowPreDeleteInterceptor, TableRowPreInsertInterceptor, TableRowPreUpdateInterceptor,
		},
		transaction::{PostCommitInterceptor, PreCommitInterceptor},
		view::{
			ViewPostCreateInterceptor, ViewPostUpdateInterceptor, ViewPreDeleteInterceptor,
			ViewPreUpdateInterceptor,
		},
		view_row::{
			ViewRowPostDeleteInterceptor, ViewRowPostInsertInterceptor, ViewRowPostUpdateInterceptor,
			ViewRowPreDeleteInterceptor, ViewRowPreInsertInterceptor, ViewRowPreUpdateInterceptor,
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

	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionRow>> {
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

	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.inner.set(key, row)
	}

	pub fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.inner.unset(key, row)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.inner.remove(key)
	}

	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		self.inner.range(range, batch_size)
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
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
	fn table_row_pre_insert_interceptors(&mut self) -> &mut Chain<dyn TableRowPreInsertInterceptor + Send + Sync> {
		self.inner.table_row_pre_insert_interceptors()
	}

	fn table_row_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn TableRowPostInsertInterceptor + Send + Sync> {
		self.inner.table_row_post_insert_interceptors()
	}

	fn table_row_pre_update_interceptors(&mut self) -> &mut Chain<dyn TableRowPreUpdateInterceptor + Send + Sync> {
		self.inner.table_row_pre_update_interceptors()
	}

	fn table_row_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn TableRowPostUpdateInterceptor + Send + Sync> {
		self.inner.table_row_post_update_interceptors()
	}

	fn table_row_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TableRowPreDeleteInterceptor + Send + Sync> {
		self.inner.table_row_pre_delete_interceptors()
	}

	fn table_row_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn TableRowPostDeleteInterceptor + Send + Sync> {
		self.inner.table_row_post_delete_interceptors()
	}

	fn ringbuffer_row_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPreInsertInterceptor + Send + Sync> {
		self.inner.ringbuffer_row_pre_insert_interceptors()
	}

	fn ringbuffer_row_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPostInsertInterceptor + Send + Sync> {
		self.inner.ringbuffer_row_post_insert_interceptors()
	}

	fn ringbuffer_row_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPreUpdateInterceptor + Send + Sync> {
		self.inner.ringbuffer_row_pre_update_interceptors()
	}

	fn ringbuffer_row_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPostUpdateInterceptor + Send + Sync> {
		self.inner.ringbuffer_row_post_update_interceptors()
	}

	fn ringbuffer_row_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPreDeleteInterceptor + Send + Sync> {
		self.inner.ringbuffer_row_pre_delete_interceptors()
	}

	fn ringbuffer_row_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferRowPostDeleteInterceptor + Send + Sync> {
		self.inner.ringbuffer_row_post_delete_interceptors()
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

	fn table_post_create_interceptors(&mut self) -> &mut Chain<dyn TablePostCreateInterceptor + Send + Sync> {
		self.inner.table_post_create_interceptors()
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

	fn view_row_pre_insert_interceptors(&mut self) -> &mut Chain<dyn ViewRowPreInsertInterceptor + Send + Sync> {
		self.inner.view_row_pre_insert_interceptors()
	}

	fn view_row_post_insert_interceptors(&mut self) -> &mut Chain<dyn ViewRowPostInsertInterceptor + Send + Sync> {
		self.inner.view_row_post_insert_interceptors()
	}

	fn view_row_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewRowPreUpdateInterceptor + Send + Sync> {
		self.inner.view_row_pre_update_interceptors()
	}

	fn view_row_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewRowPostUpdateInterceptor + Send + Sync> {
		self.inner.view_row_post_update_interceptors()
	}

	fn view_row_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewRowPreDeleteInterceptor + Send + Sync> {
		self.inner.view_row_pre_delete_interceptors()
	}

	fn view_row_post_delete_interceptors(&mut self) -> &mut Chain<dyn ViewRowPostDeleteInterceptor + Send + Sync> {
		self.inner.view_row_post_delete_interceptors()
	}

	fn view_post_create_interceptors(&mut self) -> &mut Chain<dyn ViewPostCreateInterceptor + Send + Sync> {
		self.inner.view_post_create_interceptors()
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

	fn ringbuffer_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostCreateInterceptor + Send + Sync> {
		self.inner.ringbuffer_post_create_interceptors()
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

	fn dictionary_row_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPreInsertInterceptor + Send + Sync> {
		self.inner.dictionary_row_pre_insert_interceptors()
	}

	fn dictionary_row_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPostInsertInterceptor + Send + Sync> {
		self.inner.dictionary_row_post_insert_interceptors()
	}

	fn dictionary_row_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPreUpdateInterceptor + Send + Sync> {
		self.inner.dictionary_row_pre_update_interceptors()
	}

	fn dictionary_row_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPostUpdateInterceptor + Send + Sync> {
		self.inner.dictionary_row_post_update_interceptors()
	}

	fn dictionary_row_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPreDeleteInterceptor + Send + Sync> {
		self.inner.dictionary_row_pre_delete_interceptors()
	}

	fn dictionary_row_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryRowPostDeleteInterceptor + Send + Sync> {
		self.inner.dictionary_row_post_delete_interceptors()
	}

	fn dictionary_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPostCreateInterceptor + Send + Sync> {
		self.inner.dictionary_post_create_interceptors()
	}

	fn dictionary_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPreUpdateInterceptor + Send + Sync> {
		self.inner.dictionary_pre_update_interceptors()
	}

	fn dictionary_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPostUpdateInterceptor + Send + Sync> {
		self.inner.dictionary_post_update_interceptors()
	}

	fn dictionary_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPreDeleteInterceptor + Send + Sync> {
		self.inner.dictionary_pre_delete_interceptors()
	}

	fn series_row_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesRowPreInsertInterceptor + Send + Sync> {
		self.inner.series_row_pre_insert_interceptors()
	}

	fn series_row_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesRowPostInsertInterceptor + Send + Sync> {
		self.inner.series_row_post_insert_interceptors()
	}

	fn series_row_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesRowPreUpdateInterceptor + Send + Sync> {
		self.inner.series_row_pre_update_interceptors()
	}

	fn series_row_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesRowPostUpdateInterceptor + Send + Sync> {
		self.inner.series_row_post_update_interceptors()
	}

	fn series_row_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesRowPreDeleteInterceptor + Send + Sync> {
		self.inner.series_row_pre_delete_interceptors()
	}

	fn series_row_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesRowPostDeleteInterceptor + Send + Sync> {
		self.inner.series_row_post_delete_interceptors()
	}

	fn series_post_create_interceptors(&mut self) -> &mut Chain<dyn SeriesPostCreateInterceptor + Send + Sync> {
		self.inner.series_post_create_interceptors()
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

	fn identity_post_create_interceptors(&mut self) -> &mut Chain<dyn IdentityPostCreateInterceptor + Send + Sync> {
		self.inner.identity_post_create_interceptors()
	}

	fn identity_pre_update_interceptors(&mut self) -> &mut Chain<dyn IdentityPreUpdateInterceptor + Send + Sync> {
		self.inner.identity_pre_update_interceptors()
	}

	fn identity_post_update_interceptors(&mut self) -> &mut Chain<dyn IdentityPostUpdateInterceptor + Send + Sync> {
		self.inner.identity_post_update_interceptors()
	}

	fn identity_pre_delete_interceptors(&mut self) -> &mut Chain<dyn IdentityPreDeleteInterceptor + Send + Sync> {
		self.inner.identity_pre_delete_interceptors()
	}

	fn role_post_create_interceptors(&mut self) -> &mut Chain<dyn RolePostCreateInterceptor + Send + Sync> {
		self.inner.role_post_create_interceptors()
	}

	fn role_pre_update_interceptors(&mut self) -> &mut Chain<dyn RolePreUpdateInterceptor + Send + Sync> {
		self.inner.role_pre_update_interceptors()
	}

	fn role_post_update_interceptors(&mut self) -> &mut Chain<dyn RolePostUpdateInterceptor + Send + Sync> {
		self.inner.role_post_update_interceptors()
	}

	fn role_pre_delete_interceptors(&mut self) -> &mut Chain<dyn RolePreDeleteInterceptor + Send + Sync> {
		self.inner.role_pre_delete_interceptors()
	}

	fn granted_role_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn GrantedRolePostCreateInterceptor + Send + Sync> {
		self.inner.granted_role_post_create_interceptors()
	}

	fn granted_role_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn GrantedRolePreDeleteInterceptor + Send + Sync> {
		self.inner.granted_role_pre_delete_interceptors()
	}

	fn authentication_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn AuthenticationPostCreateInterceptor + Send + Sync> {
		self.inner.authentication_post_create_interceptors()
	}

	fn authentication_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn AuthenticationPreDeleteInterceptor + Send + Sync> {
		self.inner.authentication_pre_delete_interceptors()
	}
}

impl TransactionalChanges for SubscriptionTransaction {}
