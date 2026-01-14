// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::mem::take;

use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange,
	diagnostic::transaction,
	event::EventBus,
	interface::{MultiVersionValues, WithEventBus},
	return_error,
	value::encoded::EncodedValues,
};
use reifydb_store_multi::MultiVersionBatch;
use reifydb_type::Result;
use tracing::instrument;

use crate::{
	TransactionId,
	change::{RowChange, TransactionalChanges, TransactionalDefChanges},
	interceptor::{
		Chain, Interceptors, NamespaceDefPostCreateInterceptor, NamespaceDefPostUpdateInterceptor,
		NamespaceDefPreDeleteInterceptor, NamespaceDefPreUpdateInterceptor, PostCommitContext,
		PostCommitInterceptor, PreCommitContext, PreCommitInterceptor, RingBufferDefPostCreateInterceptor,
		RingBufferDefPostUpdateInterceptor, RingBufferDefPreDeleteInterceptor,
		RingBufferDefPreUpdateInterceptor, RingBufferPostDeleteInterceptor, RingBufferPostInsertInterceptor,
		RingBufferPostUpdateInterceptor, RingBufferPreDeleteInterceptor, RingBufferPreInsertInterceptor,
		RingBufferPreUpdateInterceptor, TableDefPostCreateInterceptor, TableDefPostUpdateInterceptor,
		TableDefPreDeleteInterceptor, TableDefPreUpdateInterceptor, TablePostDeleteInterceptor,
		TablePostInsertInterceptor, TablePostUpdateInterceptor, TablePreDeleteInterceptor,
		TablePreInsertInterceptor, TablePreUpdateInterceptor, ViewDefPostCreateInterceptor,
		ViewDefPostUpdateInterceptor, ViewDefPreDeleteInterceptor, ViewDefPreUpdateInterceptor,
		WithInterceptors,
	},
	multi::{TransactionMultiVersion, pending::PendingWrites},
	single::{SvlCommandTransaction, SvlQueryTransaction, TransactionSingle},
	standard::query::StandardQueryTransaction,
};

/// An active command transaction that holds a multi command transaction
/// and provides query/command access to single storage.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct StandardCommandTransaction {
	pub multi: TransactionMultiVersion,
	pub single: TransactionSingle,
	state: TransactionState,

	pub cmd: Option<crate::multi::CommandTransaction>,
	pub event_bus: EventBus,
	pub changes: TransactionalDefChanges,

	// Track row changes for post-commit events
	pub(crate) row_changes: Vec<RowChange>,
	pub(crate) interceptors: Interceptors,
}

#[derive(Clone, Copy, PartialEq)]
enum TransactionState {
	Active,
	Committed,
	RolledBack,
}

impl StandardCommandTransaction {
	/// Creates a new active command transaction with a pre-commit callback
	#[instrument(name = "transaction::standard::command::new", level = "debug", skip_all)]
	pub fn new(
		multi: TransactionMultiVersion,
		single: TransactionSingle,
		event_bus: EventBus,
		interceptors: Interceptors,
	) -> Result<Self> {
		let cmd = multi.begin_command()?;
		let txn_id = cmd.tm.id();
		Ok(Self {
			cmd: Some(cmd),
			multi,
			single,
			state: TransactionState::Active,
			event_bus,
			interceptors,
			changes: TransactionalDefChanges::new(txn_id),
			row_changes: Vec::new(),
		})
	}

	#[instrument(name = "transaction::standard::command::event_bus", level = "trace", skip(self))]
	pub fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}

	/// Check if transaction is still active and return appropriate error if
	/// not
	fn check_active(&self) -> Result<()> {
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
	#[instrument(name = "transaction::standard::command::commit", level = "debug", skip(self))]
	pub fn commit(&mut self) -> Result<CommitVersion> {
		self.check_active()?;

		self.interceptors.pre_commit.execute(PreCommitContext::new())?;

		if let Some(mut multi) = self.cmd.take() {
			let id = multi.tm.id();
			self.state = TransactionState::Committed;

			let changes = take(&mut self.changes);
			let row_changes = take(&mut self.row_changes);

			let version = multi.commit()?;
			self.interceptors.post_commit.execute(PostCommitContext::new(
				id,
				version,
				changes,
				row_changes,
			))?;

			Ok(version)
		} else {
			// This should never happen due to check_active
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Rollback the transaction.
	#[instrument(name = "transaction::standard::command::rollback", level = "debug", skip(self))]
	pub fn rollback(&mut self) -> Result<()> {
		self.check_active()?;
		if let Some(mut multi) = self.cmd.take() {
			self.state = TransactionState::RolledBack;
			multi.rollback()
		} else {
			// This should never happen due to check_active
			unreachable!("Transaction state inconsistency")
		}
	}

	/// Get access to the pending writes in this transaction
	///
	/// This allows checking for key conflicts when committing FlowTransactions
	/// to ensure they operate on non-overlapping keyspaces.
	#[instrument(name = "transaction::standard::command::pending_writes", level = "trace", skip(self))]
	pub fn pending_writes(&self) -> &PendingWrites {
		self.cmd.as_ref().unwrap().pending_writes()
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "transaction::standard::command::with_single_query", level = "trace", skip(self, keys, f))]
	pub fn with_single_query<'a, I, F, R>(&self, keys: I, f: F) -> Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut SvlQueryTransaction<'_>) -> Result<R> + Send,
		R: Send,
	{
		self.check_active()?;
		self.single.with_query(keys, f)
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(
		name = "transaction::standard::command::with_single_command",
		level = "trace",
		skip(self, keys, f)
	)]
	pub fn with_single_command<'a, I, F, R>(&self, keys: I, f: F) -> Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut SvlCommandTransaction<'_>) -> Result<R> + Send,
		R: Send,
	{
		self.check_active()?;
		self.single.with_command(keys, f)
	}

	/// Execute a function with a query transaction view.
	/// This creates a new query transaction using the stored multi-version storage.
	/// The query transaction will operate independently but share the same single/CDC storage.
	#[instrument(name = "transaction::standard::command::with_multi_query", level = "trace", skip(self, f))]
	pub fn with_multi_query<F, R>(&self, f: F) -> Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn =
			StandardQueryTransaction::new(self.multi.begin_query()?, self.single.clone());

		f(&mut query_txn)
	}

	#[instrument(
		name = "transaction::standard::command::with_multi_query_as_of_exclusive",
		level = "trace",
		skip(self, f)
	)]
	pub fn with_multi_query_as_of_exclusive<F, R>(&self, version: CommitVersion, f: F) -> Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn =
			StandardQueryTransaction::new(self.multi.begin_query()?, self.single.clone());

		query_txn.read_as_of_version_exclusive(version)?;

		f(&mut query_txn)
	}

	#[instrument(
		name = "transaction::standard::command::with_multi_query_as_of_inclusive",
		level = "trace",
		skip(self, f)
	)]
	pub fn with_multi_query_as_of_inclusive<F, R>(&self, version: CommitVersion, f: F) -> Result<R>
	where
		F: FnOnce(&mut StandardQueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn =
			StandardQueryTransaction::new(self.multi.begin_query()?, self.single.clone());

		query_txn.multi.read_as_of_version_inclusive(version);

		f(&mut query_txn)
	}

	/// Begin a single-version query transaction for specific keys
	#[instrument(name = "transaction::standard::command::begin_single_query", level = "trace", skip(self, keys))]
	pub fn begin_single_query<'a, I>(&self, keys: I) -> Result<SvlQueryTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.check_active()?;
		self.single.begin_query(keys)
	}

	/// Begin a single-version command transaction for specific keys
	#[instrument(name = "transaction::standard::command::begin_single_command", level = "trace", skip(self, keys))]
	pub fn begin_single_command<'a, I>(&self, keys: I) -> Result<SvlCommandTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.check_active()?;
		self.single.begin_command(keys)
	}

	/// Get reference to catalog changes for this transaction
	pub fn get_changes(&self) -> &TransactionalDefChanges {
		&self.changes
	}

	/// Track a row change for post-commit event emission
	pub fn track_row_change(&mut self, change: RowChange) {
		self.row_changes.push(change);
	}

	/// Get the transaction version
	#[inline]
	pub fn version(&self) -> CommitVersion {
		self.cmd.as_ref().unwrap().version()
	}

	/// Get the transaction ID
	#[inline]
	pub fn id(&self) -> TransactionId {
		self.cmd.as_ref().unwrap().tm.id()
	}

	/// Get a value by key
	#[inline]
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionValues>> {
		self.check_active()?;
		Ok(self.cmd.as_mut().unwrap().get(key)?.map(|v| v.into_multi_version_values()))
	}

	/// Check if a key exists
	#[inline]
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().contains_key(key)
	}

	/// Get a prefix batch
	#[inline]
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().prefix(prefix)
	}

	/// Get a reverse prefix batch
	#[inline]
	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().prefix_rev(prefix)
	}

	/// Read as of version exclusive
	#[inline]
	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().read_as_of_version_exclusive(version);
		Ok(())
	}

	/// Set a key-value pair
	#[inline]
	pub fn set(&mut self, key: &EncodedKey, row: EncodedValues) -> Result<()> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().set(key, row)
	}

	/// Remove a key
	#[inline]
	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().remove(key)
	}

	/// Create a streaming iterator for forward range queries.
	#[inline]
	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_>> {
		self.check_active()?;
		Ok(self.cmd.as_mut().unwrap().range(range, batch_size))
	}

	/// Create a streaming iterator for reverse range queries.
	#[inline]
	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionValues>> + Send + '_>> {
		self.check_active()?;
		Ok(self.cmd.as_mut().unwrap().range_rev(range, batch_size))
	}
}

impl WithEventBus for StandardCommandTransaction {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

impl WithInterceptors for StandardCommandTransaction {
	fn table_pre_insert_interceptors(&mut self) -> &mut Chain<dyn TablePreInsertInterceptor + Send + Sync> {
		&mut self.interceptors.table_pre_insert
	}

	fn table_post_insert_interceptors(&mut self) -> &mut Chain<dyn TablePostInsertInterceptor + Send + Sync> {
		&mut self.interceptors.table_post_insert
	}

	fn table_pre_update_interceptors(&mut self) -> &mut Chain<dyn TablePreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.table_pre_update
	}

	fn table_post_update_interceptors(&mut self) -> &mut Chain<dyn TablePostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.table_post_update
	}

	fn table_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TablePreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.table_pre_delete
	}

	fn table_post_delete_interceptors(&mut self) -> &mut Chain<dyn TablePostDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.table_post_delete
	}

	fn ringbuffer_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreInsertInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_pre_insert
	}

	fn ringbuffer_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostInsertInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_post_insert
	}

	fn ringbuffer_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_pre_update
	}

	fn ringbuffer_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_post_update
	}

	fn ringbuffer_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_pre_delete
	}

	fn ringbuffer_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferPostDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_post_delete
	}

	fn pre_commit_interceptors(&mut self) -> &mut Chain<dyn PreCommitInterceptor + Send + Sync> {
		&mut self.interceptors.pre_commit
	}

	fn post_commit_interceptors(&mut self) -> &mut Chain<dyn PostCommitInterceptor + Send + Sync> {
		&mut self.interceptors.post_commit
	}

	fn namespace_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_def_post_create
	}

	fn namespace_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_def_pre_update
	}

	fn namespace_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_def_post_update
	}

	fn namespace_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespaceDefPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_def_pre_delete
	}

	fn table_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn TableDefPostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.table_def_post_create
	}

	fn table_def_pre_update_interceptors(&mut self) -> &mut Chain<dyn TableDefPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.table_def_pre_update
	}

	fn table_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn TableDefPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.table_def_post_update
	}

	fn table_def_pre_delete_interceptors(&mut self) -> &mut Chain<dyn TableDefPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.table_def_pre_delete
	}

	fn view_def_post_create_interceptors(&mut self) -> &mut Chain<dyn ViewDefPostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.view_def_post_create
	}

	fn view_def_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewDefPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.view_def_pre_update
	}

	fn view_def_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewDefPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.view_def_post_update
	}

	fn view_def_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewDefPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.view_def_pre_delete
	}

	fn ringbuffer_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_def_post_create
	}

	fn ringbuffer_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_def_pre_update
	}

	fn ringbuffer_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_def_post_update
	}

	fn ringbuffer_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn RingBufferDefPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.ringbuffer_def_pre_delete
	}
}

impl TransactionalChanges for StandardCommandTransaction {}

impl Drop for StandardCommandTransaction {
	fn drop(&mut self) {
		if let Some(mut multi) = self.cmd.take() {
			// Auto-rollback if still active (not committed or rolled back)
			if self.state == TransactionState::Active {
				let _ = multi.rollback();
			}
		}
	}
}
