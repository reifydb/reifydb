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
		MultiVersionQueryTransaction, MultiVersionTransaction, MultiVersionValues, QueryTransaction, RowChange,
		SingleVersionTransaction, TransactionId, TransactionalChanges, TransactionalDefChanges, WithEventBus,
		interceptor::{TransactionInterceptor, WithInterceptors},
	},
	return_error,
	value::encoded::EncodedValues,
};
use reifydb_transaction::{
	cdc::TransactionCdc,
	multi::{TransactionMultiVersion, pending::PendingWrites},
	single::TransactionSingleVersion,
};
use tracing::instrument;

use crate::transaction::query::StandardQueryTransaction;

/// An active command transaction that holds a multi command transaction
/// and provides query/command access to single storage.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct StandardCommandTransaction {
	pub multi: TransactionMultiVersion,
	pub single: TransactionSingleVersion,
	pub(crate) cdc: TransactionCdc,
	state: TransactionState,

	pub(crate) cmd: Option<<TransactionMultiVersion as MultiVersionTransaction>::Command>,
	pub(crate) event_bus: EventBus,
	pub(crate) changes: TransactionalDefChanges,
	pub(crate) catalog: MaterializedCatalog,

	// Track row changes for post-commit events
	pub(crate) row_changes: Vec<RowChange>,

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

impl StandardCommandTransaction {
	/// Creates a new active command transaction with a pre-commit callback
	#[instrument(name = "engine::transaction::command::new", level = "debug", skip_all)]
	pub fn new(
		multi: TransactionMultiVersion,
		single: TransactionSingleVersion,
		cdc: TransactionCdc,
		event_bus: EventBus,
		catalog: MaterializedCatalog,
		interceptors: Interceptors<Self>,
	) -> reifydb_core::Result<Self> {
		let cmd = multi.begin_command()?;
		let txn_id = cmd.id();
		Ok(Self {
			cmd: Some(cmd),
			multi,
			single,
			cdc,
			state: TransactionState::Active,
			event_bus,
			catalog,
			interceptors,
			changes: TransactionalDefChanges::new(txn_id),
			row_changes: Vec::new(),
			_not_send_sync: PhantomData,
		})
	}

	#[instrument(name = "engine::transaction::command::event_bus", level = "trace", skip(self))]
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

	/// Commit the transaction.
	/// Since single transactions are short-lived and auto-commit,
	/// this only commits the multi transaction.
	#[instrument(name = "engine::transaction::command::commit", level = "debug", skip(self))]
	pub fn commit(&mut self) -> crate::Result<CommitVersion> {
		self.check_active()?;

		TransactionInterceptor::pre_commit(self)?;

		if let Some(multi) = self.cmd.take() {
			let id = multi.id();
			self.state = TransactionState::Committed;

			let changes = std::mem::take(&mut self.changes);
			let row_changes = std::mem::take(&mut self.row_changes);

			let version = multi.commit()?;
			TransactionInterceptor::post_commit(self, id, version, changes, row_changes)?;

			Ok(version)
		} else {
			// This should never happen due to check_active
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Rollback the transaction.
	#[instrument(name = "engine::transaction::command::rollback", level = "debug", skip(self))]
	pub fn rollback(&mut self) -> crate::Result<()> {
		self.check_active()?;
		if let Some(multi) = self.cmd.take() {
			self.state = TransactionState::RolledBack;
			multi.rollback()
		} else {
			// This should never happen due to check_active
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Get access to the CDC transaction interface
	#[instrument(name = "engine::transaction::command::cdc", level = "trace", skip(self))]
	pub fn cdc(&self) -> &TransactionCdc {
		&self.cdc
	}

	/// Get access to the pending writes in this transaction
	///
	/// This allows checking for key conflicts when committing FlowTransactions
	/// to ensure they operate on non-overlapping keyspaces.
	#[instrument(name = "engine::transaction::command::pending_writes", level = "trace", skip(self))]
	pub fn pending_writes(&self) -> &PendingWrites {
		self.cmd.as_ref().unwrap().pending_writes()
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "engine::transaction::command::with_single_query", level = "trace", skip(self, keys, f))]
	pub fn with_single_query<'a, I, F, R>(&self, keys: I, f: F) -> crate::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
		F: FnOnce(&mut <TransactionSingleVersion as SingleVersionTransaction>::Query<'_>) -> crate::Result<R>,
	{
		self.check_active()?;
		self.single.with_query(keys, f)
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "engine::transaction::command::with_single_command", level = "trace", skip(self, keys, f))]
	pub fn with_single_command<'a, I, F, R>(&self, keys: I, f: F) -> crate::Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
		F: FnOnce(&mut <TransactionSingleVersion as SingleVersionTransaction>::Command<'_>) -> crate::Result<R>,
	{
		self.check_active()?;
		self.single.with_command(keys, f)
	}

	/// Execute a function with a query transaction view.
	/// This creates a new query transaction using the stored multi-version storage.
	/// The query transaction will operate independently but share the same single/CDC storage.
	#[instrument(name = "engine::transaction::command::with_multi_query", level = "trace", skip(self, f))]
	pub fn with_multi_query<F, R>(&self, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> crate::Result<R>,
	{
		self.check_active()?;

		let mut query_txn = StandardQueryTransaction::new(
			self.multi.begin_query()?,
			self.single.clone(),
			self.cdc.clone(),
			self.catalog.clone(),
		);

		f(&mut query_txn)
	}

	#[instrument(
		name = "engine::transaction::command::with_multi_query_as_of_exclusive",
		level = "trace",
		skip(self, f)
	)]
	pub fn with_multi_query_as_of_exclusive<F, R>(&self, version: CommitVersion, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> crate::Result<R>,
	{
		self.check_active()?;

		let mut query_txn = StandardQueryTransaction::new(
			self.multi.begin_query()?,
			self.single.clone(),
			self.cdc.clone(),
			self.catalog.clone(),
		);

		query_txn.read_as_of_version_exclusive(version)?;

		f(&mut query_txn)
	}

	#[instrument(
		name = "engine::transaction::command::with_multi_query_as_of_inclusive",
		level = "trace",
		skip(self, f)
	)]
	pub fn with_multi_query_as_of_inclusive<F, R>(&self, version: CommitVersion, f: F) -> crate::Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> crate::Result<R>,
	{
		self.check_active()?;

		let mut query_txn = StandardQueryTransaction::new(
			self.multi.begin_query()?,
			self.single.clone(),
			self.cdc.clone(),
			self.catalog.clone(),
		);

		query_txn.read_as_of_version_inclusive(version)?;

		f(&mut query_txn)
	}
}

impl MaterializedCatalogTransaction for StandardCommandTransaction {
	fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}
}

impl MultiVersionQueryTransaction for StandardCommandTransaction {
	#[inline]
	fn version(&self) -> CommitVersion {
		MultiVersionQueryTransaction::version(self.cmd.as_ref().unwrap())
	}

	#[inline]
	fn id(&self) -> TransactionId {
		MultiVersionQueryTransaction::id(self.cmd.as_ref().unwrap())
	}

	#[inline]
	fn get(&mut self, key: &EncodedKey) -> crate::Result<Option<MultiVersionValues>> {
		self.check_active()?;
		MultiVersionQueryTransaction::get(self.cmd.as_mut().unwrap(), key)
	}

	#[inline]
	fn contains_key(&mut self, key: &EncodedKey) -> crate::Result<bool> {
		self.check_active()?;
		MultiVersionQueryTransaction::contains_key(self.cmd.as_mut().unwrap(), key)
	}

	#[inline]
	fn range_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<BoxedMultiVersionIter<'_>> {
		self.check_active()?;
		MultiVersionQueryTransaction::range_batched(self.cmd.as_mut().unwrap(), range, batch_size)
	}

	#[inline]
	fn range_rev_batched(
		&mut self,
		range: EncodedKeyRange,
		batch_size: u64,
	) -> crate::Result<BoxedMultiVersionIter<'_>> {
		self.check_active()?;
		MultiVersionQueryTransaction::range_rev_batched(self.cmd.as_mut().unwrap(), range, batch_size)
	}

	#[inline]
	fn prefix(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter<'_>> {
		self.check_active()?;
		MultiVersionQueryTransaction::prefix(self.cmd.as_mut().unwrap(), prefix)
	}

	#[inline]
	fn prefix_rev(&mut self, prefix: &EncodedKey) -> crate::Result<BoxedMultiVersionIter<'_>> {
		self.check_active()?;
		MultiVersionQueryTransaction::prefix_rev(self.cmd.as_mut().unwrap(), prefix)
	}

	#[inline]
	fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> crate::Result<()> {
		self.check_active()?;
		MultiVersionQueryTransaction::read_as_of_version_exclusive(self.cmd.as_mut().unwrap(), version)
	}
}

impl MultiVersionCommandTransaction for StandardCommandTransaction {
	#[inline]
	fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> crate::Result<()> {
		self.check_active()?;
		MultiVersionCommandTransaction::set(self.cmd.as_mut().unwrap(), key, row)
	}

	#[inline]
	fn remove(&mut self, key: &EncodedKey) -> crate::Result<()> {
		self.check_active()?;
		MultiVersionCommandTransaction::remove(self.cmd.as_mut().unwrap(), key)
	}

	#[inline]
	fn commit(mut self) -> crate::Result<CommitVersion> {
		self.check_active()?;
		self.state = TransactionState::Committed;
		MultiVersionCommandTransaction::commit(self.cmd.take().unwrap())
	}

	#[inline]
	fn rollback(mut self) -> crate::Result<()> {
		self.check_active()?;
		self.state = TransactionState::RolledBack;
		MultiVersionCommandTransaction::rollback(self.cmd.take().unwrap())
	}
}

impl WithEventBus for StandardCommandTransaction {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

impl QueryTransaction for StandardCommandTransaction {
	type SingleVersionQuery<'a> = <TransactionSingleVersion as SingleVersionTransaction>::Query<'a>;

	type CdcQuery<'a> = <TransactionCdc as CdcTransaction>::Query<'a>;

	fn begin_single_query<'a, I>(&self, keys: I) -> crate::Result<Self::SingleVersionQuery<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.check_active()?;
		self.single.begin_query(keys)
	}

	fn begin_cdc_query(&self) -> crate::Result<Self::CdcQuery<'_>> {
		self.check_active()?;
		self.cdc.begin_query()
	}
}

impl CommandTransaction for StandardCommandTransaction {
	type SingleVersionCommand<'a> = <TransactionSingleVersion as SingleVersionTransaction>::Command<'a>;

	fn begin_single_command<'a, I>(&self, keys: I) -> crate::Result<Self::SingleVersionCommand<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.check_active()?;
		self.single.begin_command(keys)
	}

	fn get_changes(&self) -> &TransactionalDefChanges {
		&self.changes
	}
}

impl WithInterceptors<StandardCommandTransaction> for StandardCommandTransaction {
	fn table_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn TablePreInsertInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.table_pre_insert
	}

	fn table_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn TablePostInsertInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.table_post_insert
	}

	fn table_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn TablePreUpdateInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.table_pre_update
	}

	fn table_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::TablePostUpdateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.table_post_update
	}

	fn table_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn TablePreDeleteInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.table_pre_delete
	}

	fn table_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn TablePostDeleteInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.table_post_delete
	}

	fn ringbuffer_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn RingBufferPreInsertInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.ringbuffer_pre_insert
	}

	fn ringbuffer_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn RingBufferPostInsertInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.ringbuffer_post_insert
	}

	fn ringbuffer_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn RingBufferPreUpdateInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.ringbuffer_pre_update
	}

	fn ringbuffer_post_update_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn RingBufferPostUpdateInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.ringbuffer_post_update
	}

	fn ringbuffer_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn RingBufferPreDeleteInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.ringbuffer_pre_delete
	}

	fn ringbuffer_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn RingBufferPostDeleteInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.ringbuffer_post_delete
	}

	fn pre_commit_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn PreCommitInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.pre_commit
	}

	fn post_commit_interceptors(
		&mut self,
	) -> &mut Chain<StandardCommandTransaction, dyn PostCommitInterceptor<StandardCommandTransaction>> {
		&mut self.interceptors.post_commit
	}

	// Namespace definition interceptors
	fn namespace_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::NamespaceDefPostCreateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.namespace_def_post_create
	}

	fn namespace_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::NamespaceDefPreUpdateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.namespace_def_pre_update
	}

	fn namespace_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::NamespaceDefPostUpdateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.namespace_def_post_update
	}

	fn namespace_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::NamespaceDefPreDeleteInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.namespace_def_pre_delete
	}

	// Table definition interceptors
	fn table_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::TableDefPostCreateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.table_def_post_create
	}

	fn table_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::TableDefPreUpdateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.table_def_pre_update
	}

	fn table_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::TableDefPostUpdateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.table_def_post_update
	}

	fn table_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::TableDefPreDeleteInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.table_def_pre_delete
	}

	// View definition interceptors
	fn view_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::ViewDefPostCreateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.view_def_post_create
	}

	fn view_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::ViewDefPreUpdateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.view_def_pre_update
	}

	fn view_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::ViewDefPostUpdateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.view_def_post_update
	}

	fn view_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::ViewDefPreDeleteInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.view_def_pre_delete
	}

	// Ring buffer definition interceptors
	fn ringbuffer_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::RingBufferDefPostCreateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.ringbuffer_def_post_create
	}

	fn ringbuffer_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::RingBufferDefPreUpdateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.ringbuffer_def_pre_update
	}

	fn ringbuffer_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::RingBufferDefPostUpdateInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.ringbuffer_def_post_update
	}

	fn ringbuffer_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<
		StandardCommandTransaction,
		dyn interceptor::RingBufferDefPreDeleteInterceptor<StandardCommandTransaction>,
	> {
		&mut self.interceptors.ringbuffer_def_pre_delete
	}
}

impl TransactionalChanges for StandardCommandTransaction {}

impl Drop for StandardCommandTransaction {
	fn drop(&mut self) {
		if let Some(multi) = self.cmd.take() {
			// Auto-rollback if still active (not committed or
			// rolled back)
			if self.state == TransactionState::Active {
				let _ = multi.rollback();
			}
		}
	}
}
