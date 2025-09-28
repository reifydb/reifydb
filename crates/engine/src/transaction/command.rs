// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::marker::PhantomData;

use reifydb_catalog::{MaterializedCatalog, transaction::MaterializedCatalogTransaction};
use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	diagnostic::transaction,
	event::EventBus,
	interceptor,
	interceptor::{
		Chain, Interceptors, PostCommitInterceptor, PreCommitInterceptor, RingBufferPostDeleteInterceptor,
		RingBufferPostInsertInterceptor, RingBufferPostUpdateInterceptor, RingBufferPreDeleteInterceptor,
		RingBufferPreInsertInterceptor, RingBufferPreUpdateInterceptor, TablePostDeleteInterceptor,
		TablePostInsertInterceptor, TablePreDeleteInterceptor, TablePreInsertInterceptor,
		TablePreUpdateInterceptor,
	},
	interface::{
		BoxedMultiVersionIter, CdcTransaction, CommandTransaction, MultiVersionCommandTransaction,
		MultiVersionQueryTransaction, MultiVersionRow, MultiVersionTransaction, QueryTransaction,
		SingleVersionTransaction, Transaction, TransactionId, TransactionalChanges, TransactionalDefChanges,
		WithEventBus,
		interceptor::{TransactionInterceptor, WithInterceptors},
	},
	return_error,
	value::row::EncodedRow,
};

/// An active command transaction that holds a multi command transaction
/// and provides query/command access to single storage.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct StandardCommandTransaction<T: Transaction> {
	pub(crate) multi: Option<<T::MultiVersion as MultiVersionTransaction>::Command>,
	pub(crate) single: T::SingleVersion,
	pub(crate) cdc: T::Cdc,
	state: TransactionState,
	pub(crate) event_bus: EventBus,
	pub(crate) changes: TransactionalDefChanges,
	pub(crate) catalog: MaterializedCatalog,

	pub(crate) interceptors: Interceptors<Self>,
	// Marker to prevent Send and Sync
	_not_send_sync: PhantomData<*const ()>,
}

#[derive(Clone, Copy, PartialEq)]
enum TransactionState {
	Active,
	Committed,
	RolledBack,
}

impl<T: Transaction> StandardCommandTransaction<T> {
	/// Creates a new active command transaction with a pre-commit callback
	pub fn new(
		multi: <T::MultiVersion as MultiVersionTransaction>::Command,
		single: T::SingleVersion,
		cdc: T::Cdc,
		event_bus: EventBus,
		catalog: MaterializedCatalog,
		interceptors: Interceptors<Self>,
	) -> Self {
		let txn_id = multi.id();
		Self {
			multi: Some(multi),
			single,
			cdc,
			state: TransactionState::Active,
			event_bus,
			catalog,
			interceptors,
			changes: TransactionalDefChanges::new(txn_id),
			_not_send_sync: PhantomData,
		}
	}

	pub fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}

	/// Check if transaction is still active and return appropriate error if
	/// not
	fn check_active(&self) -> crate::Result<()> {
		match self.state {
			TransactionState::Active => Ok(()),
			TransactionState::Committed => {
				return_error!(transaction::transaction_already_committed())
			}
			TransactionState::RolledBack => {
				return_error!(transaction::transaction_already_rolled_back())
			}
		}
	}

	/// Execute a function with query access to the single transaction.
	pub fn with_single_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut <T::SingleVersion as SingleVersionTransaction>::Query<'_>) -> crate::Result<R>,
	{
		self.check_active()?;
		self.single.with_query(f)
	}

	/// Execute a function with command access to the single
	/// transaction.
	///
	/// Note: If this operation fails, the multi transaction is NOT
	/// automatically rolled back. The caller should handle transaction
	/// rollback if needed.
	pub fn with_single_command<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut <T::SingleVersion as SingleVersionTransaction>::Command<'_>) -> crate::Result<R>,
	{
		self.check_active()?;
		self.single.with_command(f)
	}

	/// Execute a function with access to the multi command transaction.
	/// This operates within the same transaction context.
	pub fn with_multi_command<F, R>(&mut self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut <T::MultiVersion as MultiVersionTransaction>::Command) -> crate::Result<R>,
	{
		self.check_active()?;
		let result = f(self.multi.as_mut().unwrap());

		// If there was an error, we should roll back the transaction
		if result.is_err() {
			if let Some(multi) = self.multi.take() {
				self.state = TransactionState::RolledBack;
				let _ = multi.rollback(); // Ignore rollback errors
			}
		}

		result
	}

	/// Execute a function with access to the multi query capabilities.
	/// This operates within the same transaction context and provides
	/// read-only access.
	pub fn with_multi_query<F, R>(&mut self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut <T::MultiVersion as MultiVersionTransaction>::Command) -> crate::Result<R>,
		<T::MultiVersion as MultiVersionTransaction>::Command: MultiVersionQueryTransaction,
	{
		self.check_active()?;
		let result = f(self.multi.as_mut().unwrap());

		// If there was an error, we should roll back the transaction
		if result.is_err() {
			if let Some(multi) = self.multi.take() {
				self.state = TransactionState::RolledBack;
				let _ = multi.rollback(); // Ignore rollback errors
			}
		}

		result
	}

	/// Commit the transaction.
	/// Since single transactions are short-lived and auto-commit,
	/// this only commits the multi transaction.
	pub fn commit(&mut self) -> crate::Result<CommitVersion> {
		self.check_active()?;

		TransactionInterceptor::pre_commit(self)?;

		if let Some(multi) = self.multi.take() {
			let id = multi.id();
			self.state = TransactionState::Committed;

			let changes = std::mem::take(&mut self.changes);

			let version = multi.commit()?;
			TransactionInterceptor::post_commit(self, id, version, changes)?;

			Ok(version)
		} else {
			// This should never happen due to check_active
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Rollback the transaction.
	pub fn rollback(&mut self) -> crate::Result<()> {
		self.check_active()?;
		if let Some(multi) = self.multi.take() {
			self.state = TransactionState::RolledBack;
			multi.rollback()
		} else {
			// This should never happen due to check_active
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Get access to the CDC transaction interface
	pub fn cdc(&self) -> &T::Cdc {
		&self.cdc
	}
}

impl<T: Transaction> MaterializedCatalogTransaction for StandardCommandTransaction<T> {
	fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}
}

impl<T: Transaction> MultiVersionQueryTransaction for StandardCommandTransaction<T> {
	#[inline]
	fn version(&self) -> CommitVersion {
		self.multi.as_ref().unwrap().version()
	}

	#[inline]
	fn id(&self) -> TransactionId {
		self.multi.as_ref().unwrap().id()
	}

	#[inline]
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionRow>> {
		self.check_active()?;
		self.multi.as_mut().unwrap().get(key)
	}

	#[inline]
	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.check_active()?;
		self.multi.as_mut().unwrap().contains_key(key)
	}

	#[inline]
	fn scan(&mut self) -> crate::Result<BoxedMultiVersionIter> {
		self.check_active()?;
		self.multi.as_mut().unwrap().scan()
	}

	#[inline]
	fn scan_rev(&mut self) -> crate::Result<BoxedMultiVersionIter> {
		self.check_active()?;
		self.multi.as_mut().unwrap().scan_rev()
	}

	#[inline]
	fn range(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedMultiVersionIter> {
		self.check_active()?;
		self.multi.as_mut().unwrap().range(range)
	}

	#[inline]
	fn range_rev(&mut self, range: EncodedKeyRange) -> crate::Result<BoxedMultiVersionIter> {
		self.check_active()?;
		self.multi.as_mut().unwrap().range_rev(range)
	}

	#[inline]
	fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter> {
		self.check_active()?;
		self.multi.as_mut().unwrap().prefix(prefix)
	}

	#[inline]
	fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter> {
		self.check_active()?;
		self.multi.as_mut().unwrap().prefix_rev(prefix)
	}
}

impl<T: Transaction> MultiVersionCommandTransaction for StandardCommandTransaction<T> {
	#[inline]
	fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> crate::Result<()> {
		self.check_active()?;
		self.multi.as_mut().unwrap().set(key, row)
	}

	#[inline]
	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		self.check_active()?;
		self.multi.as_mut().unwrap().remove(key)
	}

	#[inline]
	fn commit(mut self) -> crate::Result<CommitVersion> {
		self.check_active()?;
		self.state = TransactionState::Committed;
		self.multi.take().unwrap().commit()
	}

	#[inline]
	fn rollback(mut self) -> crate::Result<()> {
		self.check_active()?;
		self.state = TransactionState::RolledBack;
		self.multi.take().unwrap().rollback()
	}

	#[inline]
	fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> crate::Result<()> {
		self.check_active()?;
		self.multi.as_mut().unwrap().read_as_of_version_exclusive(version)
	}
}

impl<T: Transaction> WithEventBus for StandardCommandTransaction<T> {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

impl<T: Transaction> QueryTransaction for StandardCommandTransaction<T> {
	type SingleVersionQuery<'a> = <T::SingleVersion as SingleVersionTransaction>::Query<'a>;

	type CdcQuery<'a> = <T::Cdc as CdcTransaction>::Query<'a>;

	fn begin_single_query(&self) -> crate::Result<Self::SingleVersionQuery<'_>> {
		self.check_active()?;
		self.single.begin_query()
	}

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>> {
		self.check_active()?;
		self.cdc.begin_query()
	}
}

impl<T: Transaction> CommandTransaction for StandardCommandTransaction<T> {
	type SingleVersionCommand<'a> = <T::SingleVersion as SingleVersionTransaction>::Command<'a>;

	fn begin_single_command(&self) -> crate::Result<Self::SingleVersionCommand<'_>> {
		self.check_active()?;
		self.single.begin_command()
	}

	fn get_changes(&self) -> &TransactionalDefChanges {
		&self.changes
	}
}

impl<T: Transaction> WithInterceptors<StandardCommandTransaction<T>> for StandardCommandTransaction<T> {
	fn table_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn TablePreInsertInterceptor<StandardCommandTransaction<T>>> {
		&mut self.interceptors.table_pre_insert
	}

	fn table_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn TablePostInsertInterceptor<StandardCommandTransaction<T>>> {
		&mut self.interceptors.table_post_insert
	}

	fn table_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn TablePreUpdateInterceptor<StandardCommandTransaction<T>>> {
		&mut self.interceptors.table_pre_update
	}

	fn table_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::TablePostUpdateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.table_post_update
	}

	fn table_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn TablePreDeleteInterceptor<StandardCommandTransaction<T>>> {
		&mut self.interceptors.table_pre_delete
	}

	fn table_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn TablePostDeleteInterceptor<StandardCommandTransaction<T>>> {
		&mut self.interceptors.table_post_delete
	}

	fn ring_buffer_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn RingBufferPreInsertInterceptor<StandardCommandTransaction<T>>>
	{
		&mut self.interceptors.ring_buffer_pre_insert
	}

	fn ring_buffer_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn RingBufferPostInsertInterceptor<StandardCommandTransaction<T>>>
	{
		&mut self.interceptors.ring_buffer_post_insert
	}

	fn ring_buffer_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn RingBufferPreUpdateInterceptor<StandardCommandTransaction<T>>>
	{
		&mut self.interceptors.ring_buffer_pre_update
	}

	fn ring_buffer_post_update_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn RingBufferPostUpdateInterceptor<StandardCommandTransaction<T>>>
	{
		&mut self.interceptors.ring_buffer_post_update
	}

	fn ring_buffer_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn RingBufferPreDeleteInterceptor<StandardCommandTransaction<T>>>
	{
		&mut self.interceptors.ring_buffer_pre_delete
	}

	fn ring_buffer_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn RingBufferPostDeleteInterceptor<StandardCommandTransaction<T>>>
	{
		&mut self.interceptors.ring_buffer_post_delete
	}

	fn pre_commit_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn PreCommitInterceptor<StandardCommandTransaction<T>>> {
		&mut self.interceptors.pre_commit
	}

	fn post_commit_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction<T>, dyn PostCommitInterceptor<StandardCommandTransaction<T>>> {
		&mut self.interceptors.post_commit
	}

	// Namespace definition interceptors
	fn namespace_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::NamespaceDefPostCreateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.namespace_def_post_create
	}

	fn namespace_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::NamespaceDefPreUpdateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.namespace_def_pre_update
	}

	fn namespace_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::NamespaceDefPostUpdateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.namespace_def_post_update
	}

	fn namespace_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::NamespaceDefPreDeleteInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.namespace_def_pre_delete
	}

	// Table definition interceptors
	fn table_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::TableDefPostCreateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.table_def_post_create
	}

	fn table_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::TableDefPreUpdateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.table_def_pre_update
	}

	fn table_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::TableDefPostUpdateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.table_def_post_update
	}

	fn table_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::TableDefPreDeleteInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.table_def_pre_delete
	}

	// View definition interceptors
	fn view_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::ViewDefPostCreateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.view_def_post_create
	}

	fn view_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::ViewDefPreUpdateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.view_def_pre_update
	}

	fn view_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::ViewDefPostUpdateInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.view_def_post_update
	}

	fn view_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction<T>,
		dyn interceptor::ViewDefPreDeleteInterceptor<StandardCommandTransaction<T>>,
	> {
		&mut self.interceptors.view_def_pre_delete
	}
}

impl<T: Transaction> TransactionalChanges for StandardCommandTransaction<T> {}

impl<T: Transaction> Drop for StandardCommandTransaction<T> {
	fn drop(&mut self) {
		if let Some(multi) = self.multi.take() {
			// Auto-rollback if still active (not committed or
			// rolled back)
			if self.state == TransactionState::Active {
				let _ = multi.rollback();
			}
		}
	}
}
