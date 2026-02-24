// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::mem::take;

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
use reifydb_type::Result;
use tracing::instrument;

use crate::{
	TransactionId,
	change::{RowChange, TransactionalDefChanges},
	interceptor::{
		WithInterceptors,
		chain::InterceptorChain as Chain,
		interceptors::Interceptors,
		namespace_def::{
			NamespaceDefPostCreateInterceptor, NamespaceDefPostUpdateInterceptor,
			NamespaceDefPreDeleteInterceptor, NamespaceDefPreUpdateInterceptor,
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
		table::{
			TablePostDeleteInterceptor, TablePostInsertInterceptor, TablePostUpdateInterceptor,
			TablePreDeleteInterceptor, TablePreInsertInterceptor, TablePreUpdateInterceptor,
		},
		table_def::{
			TableDefPostCreateInterceptor, TableDefPostUpdateInterceptor, TableDefPreDeleteInterceptor,
			TableDefPreUpdateInterceptor,
		},
		transaction::{PostCommitContext, PostCommitInterceptor, PreCommitContext, PreCommitInterceptor},
		view::{
			ViewPostDeleteInterceptor, ViewPostInsertInterceptor, ViewPostUpdateInterceptor,
			ViewPreDeleteInterceptor, ViewPreInsertInterceptor, ViewPreUpdateInterceptor,
		},
		view_def::{
			ViewDefPostCreateInterceptor, ViewDefPostUpdateInterceptor, ViewDefPreDeleteInterceptor,
			ViewDefPreUpdateInterceptor,
		},
	},
	multi::{
		pending::PendingWrites,
		transaction::{MultiTransaction, write::MultiWriteTransaction},
	},
	single::{SingleTransaction, read::SingleReadTransaction, write::SingleWriteTransaction},
	transaction::query::QueryTransaction,
};

/// An active command transaction that holds a multi command transaction
/// and provides query/command access to single storage.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct CommandTransaction {
	pub multi: MultiTransaction,
	pub single: SingleTransaction,
	state: TransactionState,

	pub cmd: Option<MultiWriteTransaction>,
	pub event_bus: EventBus,

	// Track row changes for post-commit events
	pub(crate) row_changes: Vec<RowChange>,
	pub(crate) interceptors: Interceptors,

	// Track table changes for transactional flow pre-commit processing
	pub(crate) pending_flow_changes: Vec<Change>,
}

#[derive(Clone, Copy, PartialEq)]
enum TransactionState {
	Active,
	Committed,
	RolledBack,
}

impl CommandTransaction {
	/// Creates a new active command transaction with a pre-commit callback
	#[instrument(name = "transaction::command::new", level = "debug", skip_all)]
	pub fn new(
		multi: MultiTransaction,
		single: SingleTransaction,
		event_bus: EventBus,
		interceptors: Interceptors,
	) -> Result<Self> {
		let cmd = multi.begin_command()?;
		Ok(Self {
			cmd: Some(cmd),
			multi,
			single,
			state: TransactionState::Active,
			event_bus,
			interceptors,
			row_changes: Vec::new(),
			pending_flow_changes: Vec::new(),
		})
	}

	#[instrument(name = "transaction::command::event_bus", level = "trace", skip(self))]
	pub fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}

	/// Check if transaction is still active and return appropriate error if
	/// not
	fn check_active(&self) -> Result<()> {
		match self.state {
			TransactionState::Active => Ok(()),
			TransactionState::Committed => {
				return Err(crate::error::TransactionError::AlreadyCommitted.into());
			}
			TransactionState::RolledBack => {
				return Err(crate::error::TransactionError::AlreadyRolledBack.into());
			}
		}
	}

	/// Commit the transaction.
	/// Since single transactions are short-lived and auto-commit,
	/// this only commits the multi transaction.
	#[instrument(name = "transaction::command::commit", level = "debug", skip(self))]
	pub fn commit(&mut self) -> Result<CommitVersion> {
		self.check_active()?;

		let transaction_writes: Vec<(EncodedKey, Option<EncodedValues>)> = self
			.pending_writes()
			.iter()
			.map(|(key, pending)| match &pending.delta {
				reifydb_core::delta::Delta::Set {
					values,
					..
				} => (key.clone(), Some(values.clone())),
				_ => (key.clone(), None),
			})
			.collect();

		let mut ctx = PreCommitContext {
			flow_changes: take(&mut self.pending_flow_changes),
			pending_writes: Vec::new(),
			transaction_writes,
		};
		self.interceptors.pre_commit.execute(&mut ctx)?;

		if let Some(mut multi) = self.cmd.take() {
			// Apply pending view writes produced by pre-commit interceptors
			for (key, value) in &ctx.pending_writes {
				match value {
					Some(v) => multi.set(key, v.clone())?,
					None => multi.remove(key)?,
				}
			}

			let id = multi.tm.id();
			self.state = TransactionState::Committed;

			let changes = TransactionalDefChanges::default();
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
	#[instrument(name = "transaction::command::rollback", level = "debug", skip(self))]
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
	#[instrument(name = "transaction::command::pending_writes", level = "trace", skip(self))]
	pub fn pending_writes(&self) -> &PendingWrites {
		self.cmd.as_ref().unwrap().pending_writes()
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "transaction::command::with_single_query", level = "trace", skip(self, keys, f))]
	pub fn with_single_query<'a, I, F, R>(&self, keys: I, f: F) -> Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut SingleReadTransaction<'_>) -> Result<R> + Send,
		R: Send,
	{
		self.check_active()?;
		self.single.with_query(keys, f)
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "transaction::command::with_single_command", level = "trace", skip(self, keys, f))]
	pub fn with_single_command<'a, I, F, R>(&self, keys: I, f: F) -> Result<R>
	where
		I: IntoIterator<Item = &'a EncodedKey> + Send,
		F: FnOnce(&mut SingleWriteTransaction<'_>) -> Result<R> + Send,
		R: Send,
	{
		self.check_active()?;
		self.single.with_command(keys, f)
	}

	/// Execute a function with a query transaction view.
	/// This creates a new query transaction using the stored multi-version storage.
	/// The query transaction will operate independently but share the same single/CDC storage.
	#[instrument(name = "transaction::command::with_multi_query", level = "trace", skip(self, f))]
	pub fn with_multi_query<F, R>(&self, f: F) -> Result<R>
	where
		F: FnOnce(&mut QueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn = QueryTransaction::new(self.multi.begin_query()?, self.single.clone());

		f(&mut query_txn)
	}

	#[instrument(name = "transaction::command::with_multi_query_as_of_exclusive", level = "trace", skip(self, f))]
	pub fn with_multi_query_as_of_exclusive<F, R>(&self, version: CommitVersion, f: F) -> Result<R>
	where
		F: FnOnce(&mut QueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn = QueryTransaction::new(self.multi.begin_query()?, self.single.clone());

		query_txn.read_as_of_version_exclusive(version)?;

		f(&mut query_txn)
	}

	#[instrument(name = "transaction::command::with_multi_query_as_of_inclusive", level = "trace", skip(self, f))]
	pub fn with_multi_query_as_of_inclusive<F, R>(&self, version: CommitVersion, f: F) -> Result<R>
	where
		F: FnOnce(&mut QueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn = QueryTransaction::new(self.multi.begin_query()?, self.single.clone());

		query_txn.multi.read_as_of_version_inclusive(version);

		f(&mut query_txn)
	}

	/// Begin a single-version query transaction for specific keys
	#[instrument(name = "transaction::command::begin_single_query", level = "trace", skip(self, keys))]
	pub fn begin_single_query<'a, I>(&self, keys: I) -> Result<SingleReadTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.check_active()?;
		self.single.begin_query(keys)
	}

	/// Begin a single-version command transaction for specific keys
	#[instrument(name = "transaction::command::begin_single_command", level = "trace", skip(self, keys))]
	pub fn begin_single_command<'a, I>(&self, keys: I) -> Result<SingleWriteTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.check_active()?;
		self.single.begin_command(keys)
	}

	/// Track a row change for post-commit event emission
	pub fn track_row_change(&mut self, change: RowChange) {
		self.row_changes.push(change);
	}

	/// Track a flow change for transactional view pre-commit processing
	pub fn track_flow_change(&mut self, change: Change) {
		self.pending_flow_changes.push(change);
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

	/// Unset a key, preserving the deleted values.
	///
	/// The `values` parameter contains the deleted values for CDC and metrics.
	#[inline]
	pub fn unset(&mut self, key: &EncodedKey, values: EncodedValues) -> Result<()> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().unset(key, values)
	}

	/// Remove a key without preserving the deleted values.
	///
	/// Use when only the key matters (e.g., index entries, catalog metadata).
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

impl WithEventBus for CommandTransaction {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

impl WithInterceptors for CommandTransaction {
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

	fn view_pre_insert_interceptors(&mut self) -> &mut Chain<dyn ViewPreInsertInterceptor + Send + Sync> {
		&mut self.interceptors.view_pre_insert
	}

	fn view_post_insert_interceptors(&mut self) -> &mut Chain<dyn ViewPostInsertInterceptor + Send + Sync> {
		&mut self.interceptors.view_post_insert
	}

	fn view_pre_update_interceptors(&mut self) -> &mut Chain<dyn ViewPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.view_pre_update
	}

	fn view_post_update_interceptors(&mut self) -> &mut Chain<dyn ViewPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.view_post_update
	}

	fn view_pre_delete_interceptors(&mut self) -> &mut Chain<dyn ViewPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.view_pre_delete
	}

	fn view_post_delete_interceptors(&mut self) -> &mut Chain<dyn ViewPostDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.view_post_delete
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

impl Drop for CommandTransaction {
	fn drop(&mut self) {
		if let Some(mut multi) = self.cmd.take() {
			// Auto-rollback if still active (not committed or rolled back)
			if self.state == TransactionState::Active {
				let _ = multi.rollback();
			}
		}
	}
}
