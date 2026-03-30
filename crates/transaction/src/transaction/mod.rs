// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{
		key::{EncodedKey, EncodedKeyRange},
		row::EncodedRow,
	},
	interface::{
		catalog::shape::ShapeId,
		change::{Change, Diff},
		store::{MultiVersionBatch, MultiVersionRow},
	},
	testing::{CapturedEvent, CapturedInvocation},
	value::column::columns::Columns,
};
use reifydb_type::{
	Result,
	error::Diagnostic,
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};

use crate::{
	TransactionId,
	change::{CatalogChangesSavepoint, RowChange},
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
		transaction::{PostCommitInterceptor, PreCommitContext, PreCommitInterceptor},
		view::{
			ViewPostCreateInterceptor, ViewPostUpdateInterceptor, ViewPreDeleteInterceptor,
			ViewPreUpdateInterceptor,
		},
		view_row::{
			ViewRowPostDeleteInterceptor, ViewRowPostInsertInterceptor, ViewRowPostUpdateInterceptor,
			ViewRowPreDeleteInterceptor, ViewRowPreInsertInterceptor, ViewRowPreUpdateInterceptor,
		},
	},
	multi::transaction::write::WriteSavepoint,
	single::{read::SingleReadTransaction, write::SingleWriteTransaction},
	transaction::{
		admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction,
		replica::ReplicaTransaction, subscription::SubscriptionTransaction,
	},
};

/// Trait for executing RQL within a transaction.
///
/// This trait decouples RQL execution from the transaction layer, allowing
/// any component (procedures, ProcedureContext, tests, etc.) to execute
/// RQL through a transaction without a direct dependency on the engine crate.
pub trait RqlExecutor: Send + Sync {
	fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> Result<Vec<Frame>>;
}

pub mod admin;
pub mod catalog;
pub mod command;
pub mod query;
pub mod replica;
pub mod subscription;

/// Opaque savepoint for per-test transaction isolation.
pub struct Savepoint {
	write: WriteSavepoint,
	row_changes_len: usize,
	accumulator_len: usize,
	changes: CatalogChangesSavepoint,
}

pub struct TestTransaction<'a> {
	pub inner: &'a mut AdminTransaction,
	pub baseline: usize,
	pub events: &'a mut Vec<CapturedEvent>,
	pub invocations: &'a mut Vec<CapturedInvocation>,
	pub event_seq: &'a mut u64,
	pub handler_seq: &'a mut u64,
	pub savepoint: Option<Savepoint>,
	pub session_type: String,
	pub session_default_deny: bool,
}

impl<'a> TestTransaction<'a> {
	pub fn new(
		inner: &'a mut AdminTransaction,
		events: &'a mut Vec<CapturedEvent>,
		invocations: &'a mut Vec<CapturedInvocation>,
		event_seq: &'a mut u64,
		handler_seq: &'a mut u64,
		session_type: impl Into<String>,
		session_default_deny: bool,
	) -> Self {
		let baseline = inner.accumulator.len();
		let savepoint = Savepoint {
			write: inner.cmd.as_ref().unwrap().savepoint(),
			row_changes_len: inner.row_changes.len(),
			accumulator_len: inner.accumulator.len(),
			changes: inner.changes.savepoint(),
		};
		Self {
			inner,
			baseline,
			events,
			invocations,
			event_seq,
			handler_seq,
			savepoint: Some(savepoint),
			session_type: session_type.into(),
			session_default_deny,
		}
	}

	/// Restore transaction state to the savepoint captured at construction.
	pub fn restore(&mut self) {
		if let Some(sp) = self.savepoint.take() {
			self.inner.cmd.as_mut().unwrap().restore_savepoint(sp.write);
			self.inner.row_changes.truncate(sp.row_changes_len);
			self.inner.accumulator.truncate(sp.accumulator_len);
			self.inner.changes.restore_savepoint(sp.changes);
			self.inner.unpoison();
		}
	}

	/// Re-borrow this test transaction with a shorter lifetime,
	/// producing a TestTransaction suitable for embedding in a
	/// `Transaction::Test` variant without consuming `self`.
	pub fn reborrow(&mut self) -> TestTransaction<'_> {
		TestTransaction {
			inner: &mut *self.inner,
			baseline: self.baseline,
			events: &mut *self.events,
			invocations: &mut *self.invocations,
			event_seq: &mut *self.event_seq,
			handler_seq: &mut *self.handler_seq,
			savepoint: None,
			session_type: self.session_type.clone(),
			session_default_deny: self.session_default_deny,
		}
	}

	/// Read accumulator entries since the baseline.
	/// Used by testing helpers to inspect mutations within the current test.
	pub fn accumulator_entries_from(&self) -> &[(ShapeId, Diff)] {
		self.inner.accumulator.entries_from(self.baseline)
	}

	/// Execute test-only pre-commit style processing without committing.
	///
	/// If a `test_pre_commit` hook is registered on the interceptors, it is
	/// called first to ensure uncommitted flows are registered in the shared
	/// flow engine.  Then the pre-commit interceptor chain runs (which
	/// includes transactional flow processing) over accumulator entries from
	/// the baseline onwards.
	///
	/// Used by testing helpers that need commit-time flow work materialized
	/// while still staying inside the test savepoint.
	pub fn capture_testing_pre_commit(&mut self) -> Result<()> {
		// Only process if there are non-view source changes; view-only entries
		// are flow output from a previous call and must not be re-consumed.
		let has_source_changes = self
			.inner
			.accumulator
			.entries_from(self.baseline)
			.iter()
			.any(|(id, _)| !matches!(id, ShapeId::View(_)));

		if !has_source_changes {
			return Ok(());
		}

		// Clone the hook before re-borrowing self.
		let hook = self.inner.interceptors.test_pre_commit.clone();

		if let Some(hook) = hook {
			hook(self)?;
		}

		let offset = self.baseline;
		let transaction_writes: Vec<(EncodedKey, Option<EncodedRow>)> = self
			.inner
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
			flow_changes: self.inner.accumulator.take_changes_from(offset, CommitVersion(0)),
			pending_writes: Vec::new(),
			pending_shapes: Vec::new(),
			transaction_writes,
			view_entries: Vec::new(),
		};

		self.inner.interceptors.pre_commit.execute(&mut ctx)?;

		for (key, value) in &ctx.pending_writes {
			match value {
				Some(v) => self.inner.cmd.as_mut().unwrap().set(key, v.clone())?,
				None => self.inner.cmd.as_mut().unwrap().remove(key)?,
			}
		}

		for (id, diff) in ctx.view_entries {
			self.inner.accumulator.track(id, diff);
		}

		Ok(())
	}
}

/// An enum that can hold either a command, admin, query, or subscription transaction
/// for flexible execution
pub enum Transaction<'a> {
	Command(&'a mut CommandTransaction),
	Admin(&'a mut AdminTransaction),
	Query(&'a mut QueryTransaction),
	Subscription(&'a mut SubscriptionTransaction),
	Test(Box<TestTransaction<'a>>),
	Replica(&'a mut ReplicaTransaction),
}

impl<'a> Transaction<'a> {
	/// Get the transaction version
	pub fn version(&self) -> CommitVersion {
		match self {
			Self::Command(txn) => txn.version(),
			Self::Admin(txn) => txn.version(),
			Self::Query(txn) => txn.version(),
			Self::Subscription(txn) => txn.version(),
			Self::Test(t) => t.inner.version(),
			Self::Replica(txn) => txn.version(),
		}
	}

	/// Get the transaction ID
	pub fn id(&self) -> TransactionId {
		match self {
			Self::Command(txn) => txn.id(),
			Self::Admin(txn) => txn.id(),
			Self::Query(txn) => txn.id(),
			Self::Subscription(txn) => txn.id(),
			Self::Test(t) => t.inner.id(),
			Self::Replica(txn) => txn.id(),
		}
	}

	/// Get a value by key (async method)
	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionRow>> {
		match self {
			Self::Command(txn) => txn.get(key),
			Self::Admin(txn) => txn.get(key),
			Self::Query(txn) => txn.get(key),
			Self::Subscription(txn) => txn.get(key),
			Self::Test(t) => t.inner.get(key),
			Self::Replica(txn) => txn.get(key),
		}
	}

	/// Check if a key exists (async method)
	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		match self {
			Self::Command(txn) => txn.contains_key(key),
			Self::Admin(txn) => txn.contains_key(key),
			Self::Query(txn) => txn.contains_key(key),
			Self::Subscription(txn) => txn.contains_key(key),
			Self::Test(t) => t.inner.contains_key(key),
			Self::Replica(txn) => txn.contains_key(key),
		}
	}

	/// Get a prefix batch (async method)
	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix(prefix),
			Self::Admin(txn) => txn.prefix(prefix),
			Self::Query(txn) => txn.prefix(prefix),
			Self::Subscription(txn) => txn.prefix(prefix),
			Self::Test(t) => t.inner.prefix(prefix),
			Self::Replica(txn) => txn.prefix(prefix),
		}
	}

	/// Get a reverse prefix batch (async method)
	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix_rev(prefix),
			Self::Admin(txn) => txn.prefix_rev(prefix),
			Self::Query(txn) => txn.prefix_rev(prefix),
			Self::Subscription(txn) => txn.prefix_rev(prefix),
			Self::Test(t) => t.inner.prefix_rev(prefix),
			Self::Replica(txn) => txn.prefix_rev(prefix),
		}
	}

	/// Read as of version exclusive (async method)
	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.read_as_of_version_exclusive(version),
			Transaction::Admin(txn) => txn.read_as_of_version_exclusive(version),
			Transaction::Query(txn) => txn.read_as_of_version_exclusive(version),
			Transaction::Subscription(txn) => txn.read_as_of_version_exclusive(version),
			Transaction::Test(t) => t.inner.read_as_of_version_exclusive(version),
			Transaction::Replica(_) => {
				panic!("read_as_of_version_exclusive not supported on Replica transaction")
			}
		}
	}

	/// Create a streaming iterator for forward range queries.
	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		match self {
			Transaction::Command(txn) => txn.range(range, batch_size),
			Transaction::Admin(txn) => txn.range(range, batch_size),
			Transaction::Query(txn) => Ok(txn.range(range, batch_size)),
			Transaction::Subscription(txn) => txn.range(range, batch_size),
			Transaction::Test(t) => t.inner.range(range, batch_size),
			Transaction::Replica(txn) => txn.range(range, batch_size),
		}
	}

	/// Create a streaming iterator for reverse range queries.
	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		match self {
			Transaction::Command(txn) => txn.range_rev(range, batch_size),
			Transaction::Admin(txn) => txn.range_rev(range, batch_size),
			Transaction::Query(txn) => Ok(txn.range_rev(range, batch_size)),
			Transaction::Subscription(txn) => txn.range_rev(range, batch_size),
			Transaction::Test(t) => t.inner.range_rev(range, batch_size),
			Transaction::Replica(txn) => txn.range_rev(range, batch_size),
		}
	}
}

impl<'a> From<&'a mut CommandTransaction> for Transaction<'a> {
	fn from(txn: &'a mut CommandTransaction) -> Self {
		Self::Command(txn)
	}
}

impl<'a> From<&'a mut AdminTransaction> for Transaction<'a> {
	fn from(txn: &'a mut AdminTransaction) -> Self {
		Self::Admin(txn)
	}
}

impl<'a> From<&'a mut QueryTransaction> for Transaction<'a> {
	fn from(txn: &'a mut QueryTransaction) -> Self {
		Self::Query(txn)
	}
}

impl<'a> From<&'a mut SubscriptionTransaction> for Transaction<'a> {
	fn from(txn: &'a mut SubscriptionTransaction) -> Self {
		Self::Subscription(txn)
	}
}

impl<'a> From<&'a mut ReplicaTransaction> for Transaction<'a> {
	fn from(txn: &'a mut ReplicaTransaction) -> Self {
		Self::Replica(txn)
	}
}

impl<'a> Transaction<'a> {
	/// Get the identity associated with this transaction.
	pub fn identity(&self) -> IdentityId {
		match self {
			Self::Command(txn) => txn.identity,
			Self::Admin(txn) => txn.identity,
			Self::Query(txn) => txn.identity,
			Self::Subscription(txn) => txn.identity,
			Self::Test(t) => t.inner.identity,
			Self::Replica(_) => IdentityId::system(),
		}
	}

	/// Set the identity associated with this transaction.
	pub fn set_identity(&mut self, identity: IdentityId) {
		match self {
			Self::Command(txn) => txn.identity = identity,
			Self::Admin(txn) => txn.identity = identity,
			Self::Query(txn) => txn.identity = identity,
			Self::Subscription(txn) => txn.identity = identity,
			Self::Test(t) => t.inner.identity = identity,
			Self::Replica(_) => {}
		}
	}

	/// Clone the RQL executor, if one is set.
	fn executor_clone(&self) -> Option<Arc<dyn RqlExecutor>> {
		match self {
			Self::Command(txn) => txn.executor.clone(),
			Self::Admin(txn) => txn.executor.clone(),
			Self::Query(txn) => txn.executor.clone(),
			Self::Subscription(txn) => txn.executor.clone(),
			Self::Test(t) => t.inner.executor.clone(),
			Self::Replica(_) => None,
		}
	}

	/// Execute RQL within this transaction using the attached executor.
	///
	/// Panics if no `RqlExecutor` has been set on the underlying transaction.
	pub fn rql(&mut self, rql: &str, params: Params) -> Result<Vec<Frame>> {
		let executor = self.executor_clone().expect("RqlExecutor not set");
		let mut tx = self.reborrow();
		let result = executor.rql(&mut tx, rql, params);
		if let Err(ref e) = result {
			self.poison(*e.0.clone());
		}
		result
	}

	/// Mark this transaction as poisoned, storing the original error diagnostic.
	/// No-op for Query and Replica transactions.
	fn poison(&mut self, cause: Diagnostic) {
		match self {
			Transaction::Command(txn) => txn.poison(cause),
			Transaction::Admin(txn) => txn.poison(cause),
			Transaction::Query(_) => {}
			Transaction::Subscription(txn) => txn.inner.poison(cause),
			Transaction::Test(t) => t.inner.poison(cause),
			Transaction::Replica(_) => {}
		}
	}

	/// Re-borrow this transaction with a shorter lifetime, enabling
	/// multiple sequential uses of the same transaction binding.
	pub fn reborrow(&mut self) -> Transaction<'_> {
		match self {
			Transaction::Command(cmd) => Transaction::Command(cmd),
			Transaction::Admin(admin) => Transaction::Admin(admin),
			Transaction::Query(qry) => Transaction::Query(qry),
			Transaction::Subscription(sub) => Transaction::Subscription(sub),
			Transaction::Test(t) => Transaction::Test(Box::new(TestTransaction {
				inner: t.inner,
				baseline: t.baseline,
				events: t.events,
				invocations: t.invocations,
				event_seq: t.event_seq,
				handler_seq: t.handler_seq,
				savepoint: None,
				session_type: t.session_type.clone(),
				session_default_deny: t.session_default_deny,
			})),
			Transaction::Replica(rep) => Transaction::Replica(rep),
		}
	}

	/// Extract the underlying CommandTransaction, panics if this is
	/// not a Command transaction
	pub fn command(self) -> &'a mut CommandTransaction {
		match self {
			Self::Command(txn) => txn,
			_ => panic!("Expected Command transaction"),
		}
	}

	/// Extract the underlying AdminTransaction, panics if this is
	/// not an Admin transaction
	pub fn admin(self) -> &'a mut AdminTransaction {
		match self {
			Self::Admin(txn) => txn,
			Self::Test(t) => t.inner,
			_ => panic!("Expected Admin transaction"),
		}
	}

	/// Extract the underlying QueryTransaction, panics if this is
	/// not a Query transaction
	pub fn query(self) -> &'a mut QueryTransaction {
		match self {
			Self::Query(txn) => txn,
			_ => panic!("Expected Query transaction"),
		}
	}

	/// Extract the underlying SubscriptionTransaction, panics if this is
	/// not a Subscription transaction
	pub fn subscription(self) -> &'a mut SubscriptionTransaction {
		match self {
			Self::Subscription(txn) => txn,
			_ => panic!("Expected Subscription transaction"),
		}
	}

	/// Extract the underlying ReplicaTransaction, panics if this is
	/// not a Replica transaction
	pub fn replica(self) -> &'a mut ReplicaTransaction {
		match self {
			Self::Replica(txn) => txn,
			_ => panic!("Expected Replica transaction"),
		}
	}

	/// Get a mutable reference to the underlying
	/// CommandTransaction, panics if this is not a Command transaction
	pub fn command_mut(&mut self) -> &mut CommandTransaction {
		match self {
			Self::Command(txn) => txn,
			_ => panic!("Expected Command transaction"),
		}
	}

	/// Get a mutable reference to the underlying
	/// AdminTransaction, panics if this is not an Admin transaction
	pub fn admin_mut(&mut self) -> &mut AdminTransaction {
		match self {
			Self::Admin(txn) => txn,
			Self::Test(t) => t.inner,
			_ => panic!("Expected Admin transaction"),
		}
	}

	/// Get a mutable reference to the underlying QueryTransaction,
	/// panics if this is not a Query transaction
	pub fn query_mut(&mut self) -> &mut QueryTransaction {
		match self {
			Self::Query(txn) => txn,
			_ => panic!("Expected Query transaction"),
		}
	}

	/// Get a mutable reference to the underlying SubscriptionTransaction,
	/// panics if this is not a Subscription transaction
	pub fn subscription_mut(&mut self) -> &mut SubscriptionTransaction {
		match self {
			Self::Subscription(txn) => txn,
			_ => panic!("Expected Subscription transaction"),
		}
	}

	/// Get a mutable reference to the underlying ReplicaTransaction,
	/// panics if this is not a Replica transaction
	pub fn replica_mut(&mut self) -> &mut ReplicaTransaction {
		match self {
			Self::Replica(txn) => txn,
			_ => panic!("Expected Replica transaction"),
		}
	}

	/// Begin a single-version query transaction for specific keys
	pub fn begin_single_query<'b, I>(&self, keys: I) -> Result<SingleReadTransaction<'_>>
	where
		I: IntoIterator<Item = &'b EncodedKey>,
	{
		match self {
			Transaction::Command(txn) => txn.begin_single_query(keys),
			Transaction::Admin(txn) => txn.begin_single_query(keys),
			Transaction::Query(txn) => txn.begin_single_query(keys),
			Transaction::Subscription(txn) => txn.begin_single_query(keys),
			Transaction::Test(t) => t.inner.begin_single_query(keys),
			Transaction::Replica(_) => panic!("Single queries not supported on Replica transaction"),
		}
	}

	/// Begin a single-version write transaction for specific keys.
	/// Panics on Query and Replica transactions.
	pub fn begin_single_command<'b, I>(&self, keys: I) -> Result<SingleWriteTransaction<'_>>
	where
		I: IntoIterator<Item = &'b EncodedKey>,
	{
		match self {
			Transaction::Command(txn) => txn.begin_single_command(keys),
			Transaction::Admin(txn) => txn.begin_single_command(keys),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Subscription(txn) => txn.begin_single_command(keys),
			Transaction::Test(t) => t.inner.begin_single_command(keys),
			Transaction::Replica(_) => panic!("Single commands not supported on Replica transaction"),
		}
	}

	/// Set a key-value pair. Panics on Query transactions.
	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.set(key, row),
			Transaction::Admin(txn) => txn.set(key, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Subscription(txn) => txn.set(key, row),
			Transaction::Test(t) => t.inner.set(key, row),
			Transaction::Replica(txn) => txn.set(key, row),
		}
	}

	/// Unset (delete with tombstone) a key-value pair. Panics on Query transactions.
	pub fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.unset(key, row),
			Transaction::Admin(txn) => txn.unset(key, row),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Subscription(txn) => txn.unset(key, row),
			Transaction::Test(t) => t.inner.unset(key, row),
			Transaction::Replica(txn) => txn.unset(key, row),
		}
	}

	/// Remove a key. Panics on Query transactions.
	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.remove(key),
			Transaction::Admin(txn) => txn.remove(key),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Subscription(txn) => txn.remove(key),
			Transaction::Test(t) => t.inner.remove(key),
			Transaction::Replica(txn) => txn.remove(key),
		}
	}

	/// Track a row change for post-commit event emission.
	/// No-op on Replica transactions. Panics on Query transactions.
	pub fn track_row_change(&mut self, change: RowChange) {
		match self {
			Transaction::Command(txn) => txn.track_row_change(change),
			Transaction::Admin(txn) => txn.track_row_change(change),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Subscription(txn) => txn.track_row_change(change),
			Transaction::Test(t) => t.inner.track_row_change(change),
			Transaction::Replica(_) => {}
		}
	}

	/// Track a flow change for transactional view pre-commit processing.
	/// No-op on Replica transactions. Panics on Query transactions.
	pub fn track_flow_change(&mut self, change: Change) {
		match self {
			Transaction::Command(txn) => txn.track_flow_change(change),
			Transaction::Admin(txn) => txn.track_flow_change(change),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Subscription(txn) => txn.track_flow_change(change),
			Transaction::Test(t) => t.inner.track_flow_change(change),
			Transaction::Replica(_) => {}
		}
	}

	/// Record a test event dispatch. No-op for non-Test transactions.
	pub fn record_test_event(
		&mut self,
		namespace: String,
		event: String,
		variant: String,
		depth: u8,
		columns: Columns,
	) {
		if let Transaction::Test(t) = self {
			*t.event_seq += 1;
			t.events.push(CapturedEvent {
				sequence: *t.event_seq,
				namespace,
				event,
				variant,
				depth,
				columns,
			});
		}
	}

	/// Record a test handler invocation. No-op for non-Test transactions.
	///
	/// The `sequence` field of `invocation` will be overwritten with the next handler sequence number.
	pub fn record_test_handler(&mut self, mut invocation: CapturedInvocation) {
		if let Transaction::Test(t) = self {
			*t.handler_seq += 1;
			invocation.sequence = *t.handler_seq;
			t.invocations.push(invocation);
		}
	}
}

macro_rules! delegate_interceptor {
	($method:ident, $ret:ty) => {
		fn $method(&mut self) -> $ret {
			match self {
				Transaction::Command(txn) => txn.$method(),
				Transaction::Admin(txn) => txn.$method(),
				Transaction::Query(_) => panic!("Interceptors not supported on Query transaction"),
				Transaction::Subscription(txn) => txn.$method(),
				Transaction::Test(t) => t.inner.$method(),
				Transaction::Replica(_) => panic!("Interceptors not supported on Replica transaction"),
			}
		}
	};
}

impl WithInterceptors for Transaction<'_> {
	delegate_interceptor!(
		table_row_pre_insert_interceptors,
		&mut Chain<dyn TableRowPreInsertInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		table_row_post_insert_interceptors,
		&mut Chain<dyn TableRowPostInsertInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		table_row_pre_update_interceptors,
		&mut Chain<dyn TableRowPreUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		table_row_post_update_interceptors,
		&mut Chain<dyn TableRowPostUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		table_row_pre_delete_interceptors,
		&mut Chain<dyn TableRowPreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		table_row_post_delete_interceptors,
		&mut Chain<dyn TableRowPostDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		ringbuffer_row_pre_insert_interceptors,
		&mut Chain<dyn RingBufferRowPreInsertInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		ringbuffer_row_post_insert_interceptors,
		&mut Chain<dyn RingBufferRowPostInsertInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		ringbuffer_row_pre_update_interceptors,
		&mut Chain<dyn RingBufferRowPreUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		ringbuffer_row_post_update_interceptors,
		&mut Chain<dyn RingBufferRowPostUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		ringbuffer_row_pre_delete_interceptors,
		&mut Chain<dyn RingBufferRowPreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		ringbuffer_row_post_delete_interceptors,
		&mut Chain<dyn RingBufferRowPostDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(pre_commit_interceptors, &mut Chain<dyn PreCommitInterceptor + Send + Sync>);
	delegate_interceptor!(post_commit_interceptors, &mut Chain<dyn PostCommitInterceptor + Send + Sync>);
	delegate_interceptor!(
		namespace_post_create_interceptors,
		&mut Chain<dyn NamespacePostCreateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		namespace_pre_update_interceptors,
		&mut Chain<dyn NamespacePreUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		namespace_post_update_interceptors,
		&mut Chain<dyn NamespacePostUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		namespace_pre_delete_interceptors,
		&mut Chain<dyn NamespacePreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(table_post_create_interceptors, &mut Chain<dyn TablePostCreateInterceptor + Send + Sync>);
	delegate_interceptor!(table_pre_update_interceptors, &mut Chain<dyn TablePreUpdateInterceptor + Send + Sync>);
	delegate_interceptor!(table_post_update_interceptors, &mut Chain<dyn TablePostUpdateInterceptor + Send + Sync>);
	delegate_interceptor!(table_pre_delete_interceptors, &mut Chain<dyn TablePreDeleteInterceptor + Send + Sync>);
	delegate_interceptor!(
		view_row_pre_insert_interceptors,
		&mut Chain<dyn ViewRowPreInsertInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		view_row_post_insert_interceptors,
		&mut Chain<dyn ViewRowPostInsertInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		view_row_pre_update_interceptors,
		&mut Chain<dyn ViewRowPreUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		view_row_post_update_interceptors,
		&mut Chain<dyn ViewRowPostUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		view_row_pre_delete_interceptors,
		&mut Chain<dyn ViewRowPreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		view_row_post_delete_interceptors,
		&mut Chain<dyn ViewRowPostDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(view_post_create_interceptors, &mut Chain<dyn ViewPostCreateInterceptor + Send + Sync>);
	delegate_interceptor!(view_pre_update_interceptors, &mut Chain<dyn ViewPreUpdateInterceptor + Send + Sync>);
	delegate_interceptor!(view_post_update_interceptors, &mut Chain<dyn ViewPostUpdateInterceptor + Send + Sync>);
	delegate_interceptor!(view_pre_delete_interceptors, &mut Chain<dyn ViewPreDeleteInterceptor + Send + Sync>);
	delegate_interceptor!(
		ringbuffer_post_create_interceptors,
		&mut Chain<dyn RingBufferPostCreateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		ringbuffer_pre_update_interceptors,
		&mut Chain<dyn RingBufferPreUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		ringbuffer_post_update_interceptors,
		&mut Chain<dyn RingBufferPostUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		ringbuffer_pre_delete_interceptors,
		&mut Chain<dyn RingBufferPreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		dictionary_row_pre_insert_interceptors,
		&mut Chain<dyn DictionaryRowPreInsertInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		dictionary_row_post_insert_interceptors,
		&mut Chain<dyn DictionaryRowPostInsertInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		dictionary_row_pre_update_interceptors,
		&mut Chain<dyn DictionaryRowPreUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		dictionary_row_post_update_interceptors,
		&mut Chain<dyn DictionaryRowPostUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		dictionary_row_pre_delete_interceptors,
		&mut Chain<dyn DictionaryRowPreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		dictionary_row_post_delete_interceptors,
		&mut Chain<dyn DictionaryRowPostDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		dictionary_post_create_interceptors,
		&mut Chain<dyn DictionaryPostCreateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		dictionary_pre_update_interceptors,
		&mut Chain<dyn DictionaryPreUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		dictionary_post_update_interceptors,
		&mut Chain<dyn DictionaryPostUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		dictionary_pre_delete_interceptors,
		&mut Chain<dyn DictionaryPreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		series_row_pre_insert_interceptors,
		&mut Chain<dyn SeriesRowPreInsertInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		series_row_post_insert_interceptors,
		&mut Chain<dyn SeriesRowPostInsertInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		series_row_pre_update_interceptors,
		&mut Chain<dyn SeriesRowPreUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		series_row_post_update_interceptors,
		&mut Chain<dyn SeriesRowPostUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		series_row_pre_delete_interceptors,
		&mut Chain<dyn SeriesRowPreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		series_row_post_delete_interceptors,
		&mut Chain<dyn SeriesRowPostDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		series_post_create_interceptors,
		&mut Chain<dyn SeriesPostCreateInterceptor + Send + Sync>
	);
	delegate_interceptor!(series_pre_update_interceptors, &mut Chain<dyn SeriesPreUpdateInterceptor + Send + Sync>);
	delegate_interceptor!(
		series_post_update_interceptors,
		&mut Chain<dyn SeriesPostUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(series_pre_delete_interceptors, &mut Chain<dyn SeriesPreDeleteInterceptor + Send + Sync>);
	delegate_interceptor!(
		identity_post_create_interceptors,
		&mut Chain<dyn IdentityPostCreateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		identity_pre_update_interceptors,
		&mut Chain<dyn IdentityPreUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		identity_post_update_interceptors,
		&mut Chain<dyn IdentityPostUpdateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		identity_pre_delete_interceptors,
		&mut Chain<dyn IdentityPreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(role_post_create_interceptors, &mut Chain<dyn RolePostCreateInterceptor + Send + Sync>);
	delegate_interceptor!(role_pre_update_interceptors, &mut Chain<dyn RolePreUpdateInterceptor + Send + Sync>);
	delegate_interceptor!(role_post_update_interceptors, &mut Chain<dyn RolePostUpdateInterceptor + Send + Sync>);
	delegate_interceptor!(role_pre_delete_interceptors, &mut Chain<dyn RolePreDeleteInterceptor + Send + Sync>);
	delegate_interceptor!(
		granted_role_post_create_interceptors,
		&mut Chain<dyn GrantedRolePostCreateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		granted_role_pre_delete_interceptors,
		&mut Chain<dyn GrantedRolePreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		authentication_post_create_interceptors,
		&mut Chain<dyn AuthenticationPostCreateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		authentication_pre_delete_interceptors,
		&mut Chain<dyn AuthenticationPreDeleteInterceptor + Send + Sync>
	);
}
