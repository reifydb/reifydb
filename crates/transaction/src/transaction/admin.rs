// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem::take, sync::Arc};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	event::EventBus,
	interface::{
		WithEventBus,
		catalog::primitive::PrimitiveId,
		change::{Change, ChangeOrigin, Diff},
		store::{MultiVersionBatch, MultiVersionRow},
	},
};
use reifydb_type::{
	Result,
	error::Diagnostic,
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};
use tracing::instrument;

use crate::{
	TransactionId,
	change::{RowChange, TransactionalChanges, TransactionalDefChanges},
	change_accumulator::ChangeAccumulator,
	error::TransactionError,
	interceptor::{
		WithInterceptors,
		chain::InterceptorChain as Chain,
		dictionary::{
			DictionaryPostDeleteInterceptor, DictionaryPostInsertInterceptor,
			DictionaryPostUpdateInterceptor, DictionaryPreDeleteInterceptor,
			DictionaryPreInsertInterceptor, DictionaryPreUpdateInterceptor,
		},
		dictionary_def::{
			DictionaryDefPostCreateInterceptor, DictionaryDefPostUpdateInterceptor,
			DictionaryDefPreDeleteInterceptor, DictionaryDefPreUpdateInterceptor,
		},
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
		series_def::{
			SeriesDefPostCreateInterceptor, SeriesDefPostUpdateInterceptor, SeriesDefPreDeleteInterceptor,
			SeriesDefPreUpdateInterceptor,
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
		transaction::{
			MultiTransaction,
			write::{MultiWriteTransaction, WriteSavepoint},
		},
	},
	single::{SingleTransaction, read::SingleReadTransaction, write::SingleWriteTransaction},
	transaction::{RqlExecutor, Transaction, query::QueryTransaction},
};

/// An active admin transaction that supports Query + DML + DDL operations.
///
/// AdminTransaction is the most privileged transaction type, capable of
/// executing DDL (schema changes), DML (data mutations), and queries.
/// It tracks catalog definition changes (TransactionalDefChanges) for DDL.
///
/// The transaction will auto-rollback on drop if not explicitly committed.
pub struct AdminTransaction {
	pub multi: MultiTransaction,
	pub single: SingleTransaction,
	state: TransactionState,

	pub cmd: Option<MultiWriteTransaction>,
	pub event_bus: EventBus,
	pub changes: TransactionalDefChanges,

	// Track row changes for post-commit events
	pub(crate) row_changes: Vec<RowChange>,
	pub interceptors: Interceptors,

	// Accumulate flow changes for transactional view pre-commit processing
	pub(crate) accumulator: ChangeAccumulator,

	/// The identity executing this transaction.
	pub identity: IdentityId,

	/// Optional RQL executor for running RQL within this transaction.
	pub(crate) executor: Option<Arc<dyn RqlExecutor>>,

	/// When the transaction has been poisoned, stores the original error diagnostic.
	poison_cause: Option<Diagnostic>,
}

/// Opaque savepoint for per-test transaction isolation.
pub struct Savepoint {
	write: WriteSavepoint,
	row_changes_len: usize,
	accumulator_len: usize,
}

#[derive(Clone, Copy, PartialEq)]
enum TransactionState {
	Active,
	Committed,
	RolledBack,
	Poisoned,
}

impl AdminTransaction {
	/// Create a savepoint capturing current transaction state.
	pub fn savepoint(&self) -> Savepoint {
		Savepoint {
			write: self.cmd.as_ref().unwrap().savepoint(),
			row_changes_len: self.row_changes.len(),
			accumulator_len: self.accumulator.len(),
		}
	}

	/// Restore transaction state to a previously created savepoint.
	pub fn restore_savepoint(&mut self, sp: Savepoint) {
		self.cmd.as_mut().unwrap().restore_savepoint(sp.write);
		self.row_changes.truncate(sp.row_changes_len);
		self.accumulator.truncate(sp.accumulator_len);
	}

	/// Returns `true` when the accumulator contains flow changes that have not
	/// yet been processed.  Used by the VM to trigger eager flow processing in
	/// testing mode.
	pub fn has_pending_flow_changes(&self) -> bool {
		!self.accumulator.is_empty()
	}

	/// Number of accumulated flow change entries.
	/// Used as a baseline offset for diff-based mutation tracking.
	pub fn accumulator_len(&self) -> usize {
		self.accumulator.len()
	}

	/// Read accumulator entries from a given offset without draining.
	/// Used by testing::*::changed() to read mutations since the baseline.
	pub fn accumulator_entries_from(
		&self,
		offset: usize,
	) -> &[(PrimitiveId, Diff)] {
		self.accumulator.entries_from(offset)
	}

	/// Execute test-only pre-commit style processing without committing.
	///
	/// This is used by testing helpers that need commit-time flow work
	/// materialized while still staying inside the test savepoint.
	pub fn capture_testing_pre_commit<F>(&mut self, f: F) -> Result<()>
	where
		F: FnOnce(&mut PreCommitContext) -> Result<()>,
	{
		self.capture_testing_pre_commit_from(0, f)
	}

	/// Like `capture_testing_pre_commit` but only processes accumulator entries
	/// from `offset` onwards.  Entries before `offset` are preserved.
	///
	/// The closure receives a `PreCommitContext` built from the accumulator and
	/// should call `execute_inline_flow_changes` (or similar) to populate
	/// `ctx.pending_writes` and `ctx.view_entries`.
	pub fn capture_testing_pre_commit_from<F>(&mut self, offset: usize, f: F) -> Result<()>
	where
		F: FnOnce(&mut PreCommitContext) -> Result<()>,
	{
		let transaction_writes: Vec<(EncodedKey, Option<EncodedRow>)> = self
			.pending_writes()
			.iter()
			.map(|(key, pending)| match &pending.delta {
				Delta::Set {
					row,
					..
				} => (key.clone(), Some(row.clone())),
				_ => (key.clone(), None),
			})
			.collect();

		let mut ctx = PreCommitContext {
			flow_changes: self.accumulator.take_changes_from(offset, CommitVersion(0)),
			pending_writes: Vec::new(),
			transaction_writes,
			view_entries: Vec::new(),
		};

		f(&mut ctx)?;

		for (key, value) in &ctx.pending_writes {
			match value {
				Some(v) => self.cmd.as_mut().unwrap().set(key, v.clone())?,
				None => self.cmd.as_mut().unwrap().remove(key)?,
			}
		}

		for (id, diff) in ctx.view_entries {
			self.accumulator.track(id, diff);
		}

		Ok(())
	}
}

impl AdminTransaction {
	/// Creates a new active admin transaction with a pre-commit callback
	#[instrument(name = "transaction::admin::new", level = "debug", skip_all)]
	pub fn new(
		multi: MultiTransaction,
		single: SingleTransaction,
		event_bus: EventBus,
		interceptors: Interceptors,
		identity: IdentityId,
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
			accumulator: ChangeAccumulator::new(),
			identity,
			executor: None,
			poison_cause: None,
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
		self.check_active()?;
		let executor = self.executor.clone().expect("RqlExecutor not set");
		let result = executor.rql(&mut Transaction::Admin(self), rql, params);
		if let Err(ref e) = result {
			self.poison(e.0.clone());
		}
		result
	}

	#[instrument(name = "transaction::admin::event_bus", level = "trace", skip(self))]
	pub fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}

	/// Check if transaction is still active and return appropriate error if
	/// not
	pub(crate) fn check_active(&self) -> Result<()> {
		match self.state {
			TransactionState::Active => Ok(()),
			TransactionState::Committed => {
				return Err(TransactionError::AlreadyCommitted.into());
			}
			TransactionState::RolledBack => {
				return Err(TransactionError::AlreadyRolledBack.into());
			}
			TransactionState::Poisoned => {
				return Err(TransactionError::Poisoned {
					cause: self.poison_cause.clone().unwrap(),
				}
				.into());
			}
		}
	}

	/// Mark this transaction as poisoned, storing the original error diagnostic.
	pub(crate) fn poison(&mut self, cause: Diagnostic) {
		self.state = TransactionState::Poisoned;
		self.poison_cause = Some(cause);
	}

	/// Commit the transaction.
	/// Since single transactions are short-lived and auto-commit,
	/// this only commits the multi transaction.
	#[instrument(name = "transaction::admin::commit", level = "debug", skip(self))]
	pub fn commit(&mut self) -> Result<CommitVersion> {
		self.check_active()?;

		let transaction_writes: Vec<(EncodedKey, Option<EncodedRow>)> = self
			.pending_writes()
			.iter()
			.map(|(key, pending)| match &pending.delta {
				Delta::Set {
					row,
					..
				} => (key.clone(), Some(row.clone())),
				_ => (key.clone(), None),
			})
			.collect();

		let mut ctx = PreCommitContext {
			flow_changes: self.accumulator.take_changes(CommitVersion(0)),
			pending_writes: Vec::new(),
			transaction_writes,
			view_entries: Vec::new(),
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
	#[instrument(name = "transaction::admin::rollback", level = "debug", skip(self))]
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
	#[instrument(name = "transaction::admin::pending_writes", level = "trace", skip(self))]
	pub fn pending_writes(&self) -> &PendingWrites {
		self.cmd.as_ref().unwrap().pending_writes()
	}

	/// Execute a function with query access to the single transaction.
	#[instrument(name = "transaction::admin::with_single_query", level = "trace", skip(self, keys, f))]
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
	#[instrument(name = "transaction::admin::with_single_command", level = "trace", skip(self, keys, f))]
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
	#[instrument(name = "transaction::admin::with_multi_query", level = "trace", skip(self, f))]
	pub fn with_multi_query<F, R>(&self, f: F) -> Result<R>
	where
		F: FnOnce(&mut QueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn =
			QueryTransaction::new(self.multi.begin_query()?, self.single.clone(), self.identity);

		f(&mut query_txn)
	}

	#[instrument(name = "transaction::admin::with_multi_query_as_of_exclusive", level = "trace", skip(self, f))]
	pub fn with_multi_query_as_of_exclusive<F, R>(&self, version: CommitVersion, f: F) -> Result<R>
	where
		F: FnOnce(&mut QueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn =
			QueryTransaction::new(self.multi.begin_query()?, self.single.clone(), self.identity);

		query_txn.read_as_of_version_exclusive(version)?;

		f(&mut query_txn)
	}

	#[instrument(name = "transaction::admin::with_multi_query_as_of_inclusive", level = "trace", skip(self, f))]
	pub fn with_multi_query_as_of_inclusive<F, R>(&self, version: CommitVersion, f: F) -> Result<R>
	where
		F: FnOnce(&mut QueryTransaction) -> Result<R>,
	{
		self.check_active()?;

		let mut query_txn =
			QueryTransaction::new(self.multi.begin_query()?, self.single.clone(), self.identity);

		query_txn.multi.read_as_of_version_inclusive(version);

		f(&mut query_txn)
	}

	/// Begin a single-version query transaction for specific keys
	#[instrument(name = "transaction::admin::begin_single_query", level = "trace", skip(self, keys))]
	pub fn begin_single_query<'a, I>(&self, keys: I) -> Result<SingleReadTransaction<'_>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		self.check_active()?;
		self.single.begin_query(keys)
	}

	/// Begin a single-version command transaction for specific keys
	#[instrument(name = "transaction::admin::begin_single_command", level = "trace", skip(self, keys))]
	pub fn begin_single_command<'a, I>(&self, keys: I) -> Result<SingleWriteTransaction<'_>>
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

	/// Track a flow change for transactional view pre-commit processing.
	pub fn track_flow_change(&mut self, change: Change) {
		if let ChangeOrigin::Primitive(id) = change.origin {
			for diff in change.diffs {
				self.accumulator.track(id, diff);
			}
		}
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
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionRow>> {
		self.check_active()?;
		Ok(self.cmd.as_mut().unwrap().get(key)?.map(|v| v.into_multi_version_row()))
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
	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().set(key, row)
	}

	/// Unset a key, preserving the deleted values.
	#[inline]
	pub fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		self.check_active()?;
		self.cmd.as_mut().unwrap().unset(key, row)
	}

	/// Remove a key without preserving the deleted values.
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
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		self.check_active()?;
		Ok(self.cmd.as_mut().unwrap().range(range, batch_size))
	}

	/// Create a streaming iterator for reverse range queries.
	#[inline]
	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		self.check_active()?;
		Ok(self.cmd.as_mut().unwrap().range_rev(range, batch_size))
	}
}

impl WithEventBus for AdminTransaction {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

impl WithInterceptors for AdminTransaction {
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

	fn namespace_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespacePostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_post_create
	}

	fn namespace_pre_update_interceptors(&mut self) -> &mut Chain<dyn NamespacePreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_pre_update
	}

	fn namespace_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn NamespacePostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_post_update
	}

	fn namespace_pre_delete_interceptors(&mut self) -> &mut Chain<dyn NamespacePreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.namespace_pre_delete
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

	fn dictionary_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPreInsertInterceptor + Send + Sync> {
		&mut self.interceptors.dictionary_pre_insert
	}

	fn dictionary_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPostInsertInterceptor + Send + Sync> {
		&mut self.interceptors.dictionary_post_insert
	}

	fn dictionary_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.dictionary_pre_update
	}

	fn dictionary_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.dictionary_post_update
	}

	fn dictionary_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.dictionary_pre_delete
	}

	fn dictionary_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryPostDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.dictionary_post_delete
	}

	fn dictionary_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryDefPostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.dictionary_def_post_create
	}

	fn dictionary_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryDefPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.dictionary_def_pre_update
	}

	fn dictionary_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryDefPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.dictionary_def_post_update
	}

	fn dictionary_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn DictionaryDefPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.dictionary_def_pre_delete
	}

	fn series_pre_insert_interceptors(&mut self) -> &mut Chain<dyn SeriesPreInsertInterceptor + Send + Sync> {
		&mut self.interceptors.series_pre_insert
	}

	fn series_post_insert_interceptors(&mut self) -> &mut Chain<dyn SeriesPostInsertInterceptor + Send + Sync> {
		&mut self.interceptors.series_post_insert
	}

	fn series_pre_update_interceptors(&mut self) -> &mut Chain<dyn SeriesPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.series_pre_update
	}

	fn series_post_update_interceptors(&mut self) -> &mut Chain<dyn SeriesPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.series_post_update
	}

	fn series_pre_delete_interceptors(&mut self) -> &mut Chain<dyn SeriesPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.series_pre_delete
	}

	fn series_post_delete_interceptors(&mut self) -> &mut Chain<dyn SeriesPostDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.series_post_delete
	}

	fn series_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesDefPostCreateInterceptor + Send + Sync> {
		&mut self.interceptors.series_def_post_create
	}

	fn series_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesDefPreUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.series_def_pre_update
	}

	fn series_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesDefPostUpdateInterceptor + Send + Sync> {
		&mut self.interceptors.series_def_post_update
	}

	fn series_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<dyn SeriesDefPreDeleteInterceptor + Send + Sync> {
		&mut self.interceptors.series_def_pre_delete
	}
}

impl TransactionalChanges for AdminTransaction {}

impl Drop for AdminTransaction {
	fn drop(&mut self) {
		if let Some(mut multi) = self.cmd.take() {
			// Auto-rollback if still active or poisoned (not committed or rolled back)
			if self.state == TransactionState::Active || self.state == TransactionState::Poisoned {
				let _ = multi.rollback();
			}
		}
	}
}
