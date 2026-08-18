// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Public `Transaction` handle: wraps a single-version or multi-version transaction body in one shape so the
//! engine, planner, and policy layers never branch on backend.

use std::{collections::BTreeSet, sync::Arc};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::EncodedBytes,
};
use reifydb_core::{
	common::CommitVersion,
	execution::ExecutionResult,
	interface::{
		catalog::{object::ObjectId, policy::SessionOp},
		change::{Change, ChangeOrigin, Diff},
		store::{MultiVersionBatch, MultiVersionRow},
	},
	testing::{CapturedEvent, CapturedInvocation},
	value::column::columns::Columns,
};
use reifydb_value::{
	Result, error::Diagnostic, params::Params, reifydb_assertions, value::datetime::DateTime,
	value::identity::IdentityId,
};

use crate::{
	TransactionId,
	change::{CatalogChangesSavepoint, RowChange},
	change_accumulator::ChangeAccumulator,
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
		identity::{IdentityPostCreateInterceptor, IdentityPreDeleteInterceptor},
		identity_attribute::{IdentityAttributePostCreateInterceptor, IdentityAttributePreDeleteInterceptor},
		identity_attribute_value::{
			IdentityAttributeValuePostCreateInterceptor, IdentityAttributeValuePreDeleteInterceptor,
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
		role::{RolePostCreateInterceptor, RolePreDeleteInterceptor},
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
	},
	multi::{RangeScope, transaction::write::WriteSavepoint},
	single::{SingleTransaction, read::SingleReadTransaction, write::SingleWriteTransaction},
	transaction::{
		admin::AdminTransaction, command::CommandTransaction, flow::InlineFlowState, query::QueryTransaction,
		replica::ReplicaTransaction, write::Write,
	},
};

pub trait RqlExecutor: Send + Sync {
	fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> ExecutionResult;
}

pub mod admin;
pub mod catalog;
pub mod command;
pub mod flow;
pub mod query;
pub mod replica;
pub mod write;

pub struct Savepoint {
	write: WriteSavepoint,
	row_changes_len: usize,
	accumulator_len: usize,
	published_len: usize,
	changes: CatalogChangesSavepoint,
}

pub struct TestTransaction<'a> {
	pub inner: &'a mut AdminTransaction,
	pub baseline: usize,
	pub published_baseline: usize,
	pub events: &'a mut Vec<CapturedEvent>,
	pub invocations: &'a mut Vec<CapturedInvocation>,
	pub event_seq: &'a mut u64,
	pub handler_seq: &'a mut u64,
	pub savepoint: Option<Savepoint>,
	pub session_type: SessionOp,
	pub session_default_deny: bool,
}

impl<'a> TestTransaction<'a> {
	pub fn new(
		inner: &'a mut AdminTransaction,
		events: &'a mut Vec<CapturedEvent>,
		invocations: &'a mut Vec<CapturedInvocation>,
		event_seq: &'a mut u64,
		handler_seq: &'a mut u64,
		session_type: SessionOp,
		session_default_deny: bool,
	) -> Self {
		let baseline = inner.accumulator.len();
		let published_baseline = inner.inline_flow.published_len();
		let savepoint = Savepoint {
			write: inner.cmd.as_ref().unwrap().savepoint(),
			row_changes_len: inner.row_changes.len(),
			accumulator_len: inner.accumulator.len(),
			published_len: inner.inline_flow.published_len(),
			changes: inner.changes.savepoint(),
		};
		Self {
			inner,
			baseline,
			published_baseline,
			events,
			invocations,
			event_seq,
			handler_seq,
			savepoint: Some(savepoint),
			session_type,
			session_default_deny,
		}
	}

	pub fn restore(&mut self) {
		if let Some(sp) = self.savepoint.take() {
			self.inner.cmd.as_mut().unwrap().restore_savepoint(sp.write);
			self.inner.row_changes.truncate(sp.row_changes_len);
			self.inner.accumulator.truncate(sp.accumulator_len);
			self.inner.inline_flow.truncate_published(sp.published_len);
			self.inner.changes.restore_savepoint(sp.changes);
			self.inner.unpoison();
		}
	}

	pub fn reborrow(&mut self) -> TestTransaction<'_> {
		TestTransaction {
			inner: &mut *self.inner,
			baseline: self.baseline,
			published_baseline: self.published_baseline,
			events: &mut *self.events,
			invocations: &mut *self.invocations,
			event_seq: &mut *self.event_seq,
			handler_seq: &mut *self.handler_seq,
			savepoint: None,
			session_type: self.session_type,
			session_default_deny: self.session_default_deny,
		}
	}

	pub fn accumulator_entries_from(&self) -> Vec<(ObjectId, Diff)> {
		let mut entries = self.inner.accumulator.entries_from(self.baseline).to_vec();
		entries.extend_from_slice(self.inner.inline_flow.published_from(self.published_baseline));
		entries
	}

	pub fn capture_testing_pre_commit(&mut self) -> Result<()> {
		Transaction::Test(Box::new(self.reborrow())).flush_flow_changes(None)
	}
}

pub enum Transaction<'a> {
	Command(&'a mut CommandTransaction),
	Admin(&'a mut AdminTransaction),
	Query(&'a mut QueryTransaction),
	Test(Box<TestTransaction<'a>>),
	Replica(&'a mut ReplicaTransaction),
}

impl<'a> Transaction<'a> {
	pub fn version(&self) -> CommitVersion {
		match self {
			Self::Command(txn) => txn.version(),
			Self::Admin(txn) => txn.version(),
			Self::Query(txn) => txn.version(),
			Self::Test(t) => t.inner.version(),
			Self::Replica(txn) => txn.version(),
		}
	}

	pub fn id(&self) -> TransactionId {
		match self {
			Self::Command(txn) => txn.id(),
			Self::Admin(txn) => txn.id(),
			Self::Query(txn) => txn.id(),
			Self::Test(t) => t.inner.id(),
			Self::Replica(txn) => txn.id(),
		}
	}

	/// True when accumulated object changes have not yet been fed through the flow engine. Query
	/// and Replica never accumulate; Test counts only what the current test block accumulated on
	/// top of the shared admin transaction.
	pub fn has_unprocessed_flow_changes(&self) -> bool {
		match self {
			Self::Command(txn) => !txn.accumulator.is_empty(),
			Self::Admin(txn) => !txn.accumulator.is_empty(),
			Self::Test(t) => !t.inner.accumulator.entries_from(t.baseline).is_empty(),
			Self::Query(_) | Self::Replica(_) => false,
		}
	}

	/// Distinct objects with unprocessed flow changes; empty for the
	/// variants exempted by [`Self::has_unprocessed_flow_changes`].
	pub fn unprocessed_flow_change_objects(&self) -> Vec<ObjectId> {
		match self {
			Self::Command(txn) => txn.accumulator.pending_objects(),
			Self::Admin(txn) => txn.accumulator.pending_objects(),
			Self::Test(t) => t.inner.accumulator.pending_objects_from(t.baseline),
			Self::Query(_) | Self::Replica(_) => Vec::new(),
		}
	}

	/// True while a flush is already running against this transaction; a read barrier that fires
	/// underneath one must not start a nested flush.
	pub fn is_flushing_flow_changes(&self) -> bool {
		match self {
			Self::Command(txn) => txn.inline_flow.is_running(),
			Self::Admin(txn) => txn.inline_flow.is_running(),
			Self::Test(t) => t.inner.inline_flow.is_running(),
			Self::Query(_) | Self::Replica(_) => false,
		}
	}

	/// Runs the pre-commit chain against this live transaction, consuming the unprocessed changes
	/// it feeds in. Consumed input and produced view entries move to the published buffer, which
	/// reaches the committed change stream but is never fed to the flow engine a second time.
	pub fn flush_flow_changes(&mut self, objects: Option<&BTreeSet<ObjectId>>) -> Result<()> {
		let Some((offset, changed_at)) = self.flush_start() else {
			return Ok(());
		};
		let Some(accumulator) = self.accumulator_mut() else {
			return Ok(());
		};
		let flow_changes = match objects {
			Some(objects) => {
				accumulator.take_changes_matching(offset, objects, CommitVersion(0), changed_at)?
			}
			None => accumulator.take_changes_from(offset, CommitVersion(0), changed_at)?,
		};
		if flow_changes.is_empty() {
			return Ok(());
		}

		let mut ctx = PreCommitContext {
			flow_changes,
			published_entries: Vec::new(),
		};
		let chain = self.pre_commit_interceptors().clone();
		self.set_flushing_flow_changes(true);
		let outcome = chain.execute(self, &mut ctx);
		self.set_flushing_flow_changes(false);
		outcome?;

		reifydb_assertions! {
			let fed: BTreeSet<ObjectId> = ctx
				.flow_changes
				.iter()
				.filter_map(|change| match change.origin {
					ChangeOrigin::Object(object) => Some(object),
					_ => None,
				})
				.collect();
			let still_pending: Vec<ObjectId> = self
				.unprocessed_flow_change_objects()
				.into_iter()
				.filter(|object| fed.contains(object))
				.collect();
			assert!(
				still_pending.is_empty(),
				"flush fed {} object(s) to the flow engine but {:?} are still marked unprocessed, so the \
				 next read barrier would replay the same changes and apply operator state twice",
				fed.len(),
				still_pending
			);
		}

		let mut published: Vec<(ObjectId, Diff)> = Vec::new();
		for change in ctx.flow_changes {
			if let ChangeOrigin::Object(object) = change.origin {
				for diff in change.diffs {
					published.push((object, diff));
				}
			}
		}
		published.extend(ctx.published_entries);

		let Some(state) = self.inline_flow_mut() else {
			return Ok(());
		};
		state.publish(published);
		Ok(())
	}

	fn flush_start(&self) -> Option<(usize, DateTime)> {
		match self {
			Self::Command(txn) => (!txn.inline_flow.is_running()).then(|| (0, txn.clock.now())),
			Self::Admin(txn) => (!txn.inline_flow.is_running()).then(|| (0, txn.clock.now())),
			Self::Test(t) => (!t.inner.inline_flow.is_running()).then(|| (t.baseline, t.inner.clock.now())),
			Self::Query(_) | Self::Replica(_) => None,
		}
	}

	fn accumulator_mut(&mut self) -> Option<&mut ChangeAccumulator> {
		match self {
			Self::Command(txn) => Some(&mut txn.accumulator),
			Self::Admin(txn) => Some(&mut txn.accumulator),
			Self::Test(t) => Some(&mut t.inner.accumulator),
			Self::Query(_) | Self::Replica(_) => None,
		}
	}

	fn inline_flow_mut(&mut self) -> Option<&mut InlineFlowState> {
		match self {
			Self::Command(txn) => Some(&mut txn.inline_flow),
			Self::Admin(txn) => Some(&mut txn.inline_flow),
			Self::Test(t) => Some(&mut t.inner.inline_flow),
			Self::Query(_) | Self::Replica(_) => None,
		}
	}

	fn set_flushing_flow_changes(&mut self, running: bool) {
		if let Some(state) = self.inline_flow_mut() {
			state.set_running(running);
		}
	}

	pub fn get(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionRow>> {
		match self {
			Self::Command(txn) => txn.get(key),
			Self::Admin(txn) => txn.get(key),
			Self::Query(txn) => txn.get(key),
			Self::Test(t) => t.inner.get(key),
			Self::Replica(txn) => txn.get(key),
		}
	}

	pub fn get_committed(&mut self, key: &EncodedKey) -> Result<Option<MultiVersionRow>> {
		match self {
			Self::Command(txn) => txn.get_committed(key),
			Self::Admin(txn) => txn.get_committed(key),
			Self::Query(txn) => txn.get(key),
			Self::Test(t) => t.inner.get_committed(key),
			Self::Replica(txn) => txn.get(key),
		}
	}

	pub fn contains_key(&mut self, key: &EncodedKey) -> Result<bool> {
		match self {
			Self::Command(txn) => txn.contains_key(key),
			Self::Admin(txn) => txn.contains_key(key),
			Self::Query(txn) => txn.contains_key(key),
			Self::Test(t) => t.inner.contains_key(key),
			Self::Replica(txn) => txn.contains_key(key),
		}
	}

	pub fn prefix(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix(prefix),
			Self::Admin(txn) => txn.prefix(prefix),
			Self::Query(txn) => txn.prefix(prefix),
			Self::Test(t) => t.inner.prefix(prefix),
			Self::Replica(txn) => txn.prefix(prefix),
		}
	}

	pub fn prefix_rev(&mut self, prefix: &EncodedKey) -> Result<MultiVersionBatch> {
		match self {
			Self::Command(txn) => txn.prefix_rev(prefix),
			Self::Admin(txn) => txn.prefix_rev(prefix),
			Self::Query(txn) => txn.prefix_rev(prefix),
			Self::Test(t) => t.inner.prefix_rev(prefix),
			Self::Replica(txn) => txn.prefix_rev(prefix),
		}
	}

	pub fn read_as_of_version_exclusive(&mut self, version: CommitVersion) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.read_as_of_version_exclusive(version),
			Transaction::Admin(txn) => txn.read_as_of_version_exclusive(version),
			Transaction::Query(txn) => txn.read_as_of_version_exclusive(version),
			Transaction::Test(t) => t.inner.read_as_of_version_exclusive(version),
			Transaction::Replica(_) => {
				panic!("read_as_of_version_exclusive not supported on Replica transaction")
			}
		}
	}

	pub fn range(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		match self {
			Transaction::Command(txn) => txn.range(range, scope, batch_size),
			Transaction::Admin(txn) => txn.range(range, scope, batch_size),
			Transaction::Query(txn) => Ok(txn.range(range, scope, batch_size)),
			Transaction::Test(t) => t.inner.range(range, scope, batch_size),
			Transaction::Replica(txn) => txn.range(range, scope, batch_size),
		}
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		scope: RangeScope,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		match self {
			Transaction::Command(txn) => txn.range_rev(range, scope, batch_size),
			Transaction::Admin(txn) => txn.range_rev(range, scope, batch_size),
			Transaction::Query(txn) => Ok(txn.range_rev(range, scope, batch_size)),
			Transaction::Test(t) => t.inner.range_rev(range, scope, batch_size),
			Transaction::Replica(txn) => txn.range_rev(range, scope, batch_size),
		}
	}

	pub fn remove_silent(&mut self, key: &EncodedKey) -> Result<()> {
		match self {
			Transaction::Command(txn) => txn.remove_silent(key),
			Transaction::Admin(txn) => txn.remove_silent(key),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Test(t) => t.inner.remove_silent(key),
			Transaction::Replica(_) => panic!("Silent removes not supported on Replica transaction"),
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

impl<'a> From<&'a mut ReplicaTransaction> for Transaction<'a> {
	fn from(txn: &'a mut ReplicaTransaction) -> Self {
		Self::Replica(txn)
	}
}

impl<'a> Transaction<'a> {
	pub fn identity(&self) -> IdentityId {
		match self {
			Self::Command(txn) => txn.identity,
			Self::Admin(txn) => txn.identity,
			Self::Query(txn) => txn.identity,
			Self::Test(t) => t.inner.identity,
			Self::Replica(_) => IdentityId::system(),
		}
	}

	pub fn set_identity(&mut self, identity: IdentityId) {
		match self {
			Self::Command(txn) => txn.identity = identity,
			Self::Admin(txn) => txn.identity = identity,
			Self::Query(txn) => txn.identity = identity,
			Self::Test(t) => t.inner.identity = identity,
			Self::Replica(_) => {}
		}
	}

	fn executor_clone(&self) -> Option<Arc<dyn RqlExecutor>> {
		match self {
			Self::Command(txn) => txn.executor.clone(),
			Self::Admin(txn) => txn.executor.clone(),
			Self::Query(txn) => txn.executor.clone(),
			Self::Test(t) => t.inner.executor.clone(),
			Self::Replica(_) => None,
		}
	}

	pub fn rql(&mut self, rql: &str, params: Params) -> ExecutionResult {
		let executor = self.executor_clone().expect("RqlExecutor not set");
		let mut tx = self.reborrow();
		let result = executor.rql(&mut tx, rql, params);
		if let Some(ref e) = result.error {
			self.poison(*e.0.clone());
		}
		result
	}

	fn poison(&mut self, cause: Diagnostic) {
		match self {
			Transaction::Command(txn) => txn.poison(cause),
			Transaction::Admin(txn) => txn.poison(cause),
			Transaction::Query(_) => {}
			Transaction::Test(t) => t.inner.poison(cause),
			Transaction::Replica(_) => {}
		}
	}

	pub fn reborrow(&mut self) -> Transaction<'_> {
		match self {
			Transaction::Command(cmd) => Transaction::Command(cmd),
			Transaction::Admin(admin) => Transaction::Admin(admin),
			Transaction::Query(qry) => Transaction::Query(qry),
			Transaction::Test(t) => Transaction::Test(Box::new(TestTransaction {
				inner: t.inner,
				baseline: t.baseline,
				published_baseline: t.published_baseline,
				events: t.events,
				invocations: t.invocations,
				event_seq: t.event_seq,
				handler_seq: t.handler_seq,
				savepoint: None,
				session_type: t.session_type,
				session_default_deny: t.session_default_deny,
			})),
			Transaction::Replica(rep) => Transaction::Replica(rep),
		}
	}

	pub fn command(self) -> &'a mut CommandTransaction {
		match self {
			Self::Command(txn) => txn,
			_ => panic!("Expected Command transaction"),
		}
	}

	pub fn admin(self) -> &'a mut AdminTransaction {
		match self {
			Self::Admin(txn) => txn,
			Self::Test(t) => t.inner,
			_ => panic!("Expected Admin transaction"),
		}
	}

	pub fn query(self) -> &'a mut QueryTransaction {
		match self {
			Self::Query(txn) => txn,
			_ => panic!("Expected Query transaction"),
		}
	}

	pub fn replica(self) -> &'a mut ReplicaTransaction {
		match self {
			Self::Replica(txn) => txn,
			_ => panic!("Expected Replica transaction"),
		}
	}

	pub fn admin_mut(&mut self) -> &mut AdminTransaction {
		match self {
			Self::Admin(txn) => txn,
			Self::Test(t) => t.inner,
			_ => panic!("Expected Admin transaction"),
		}
	}

	pub fn begin_single_query<'b, I>(&self, keys: I) -> Result<SingleReadTransaction<'_>>
	where
		I: IntoIterator<Item = &'b EncodedKey>,
	{
		match self {
			Transaction::Command(txn) => txn.begin_single_query(keys),
			Transaction::Admin(txn) => txn.begin_single_query(keys),
			Transaction::Query(txn) => txn.begin_single_query(keys),
			Transaction::Test(t) => t.inner.begin_single_query(keys),
			Transaction::Replica(_) => panic!("Single queries not supported on Replica transaction"),
		}
	}

	pub fn begin_single_command<'b, I>(&self, keys: I) -> Result<SingleWriteTransaction<'_>>
	where
		I: IntoIterator<Item = &'b EncodedKey>,
	{
		match self {
			Transaction::Command(txn) => txn.begin_single_command(keys),
			Transaction::Admin(txn) => txn.begin_single_command(keys),
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Test(t) => t.inner.begin_single_command(keys),
			Transaction::Replica(_) => panic!("Single commands not supported on Replica transaction"),
		}
	}

	pub fn single(&self) -> Option<&SingleTransaction> {
		match self {
			Transaction::Command(txn) => Some(&txn.single),
			Transaction::Admin(txn) => Some(&txn.single),
			Transaction::Query(txn) => txn.single.as_ref(),
			Transaction::Test(t) => Some(&t.inner.single),
			Transaction::Replica(_) => None,
		}
	}

	fn write_ops(&mut self) -> &mut dyn Write {
		match self {
			Transaction::Command(txn) => &mut **txn,
			Transaction::Admin(txn) => &mut **txn,
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Test(t) => &mut *t.inner,
			Transaction::Replica(txn) => &mut **txn,
		}
	}

	pub fn set(&mut self, key: &EncodedKey, bytes: impl Into<EncodedBytes>) -> Result<()> {
		Write::set(self.write_ops(), key, bytes.into())
	}

	pub fn remove_with_pre(&mut self, key: &EncodedKey, pre: EncodedBytes) -> Result<()> {
		Write::remove_with_pre(self.write_ops(), key, pre)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		Write::remove(self.write_ops(), key)
	}

	pub fn mark_preexisting(&mut self, key: &EncodedKey) -> Result<()> {
		Write::mark_preexisting(self.write_ops(), key)
	}

	pub fn track_row_change(&mut self, changes: &[RowChange]) {
		Write::track_row_change(self.write_ops(), changes)
	}

	pub fn track_flow_change(&mut self, change: Change) {
		Write::track_flow_change(self.write_ops(), change)
	}

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
		identity_pre_delete_interceptors,
		&mut Chain<dyn IdentityPreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(role_post_create_interceptors, &mut Chain<dyn RolePostCreateInterceptor + Send + Sync>);
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
		identity_attribute_post_create_interceptors,
		&mut Chain<dyn IdentityAttributePostCreateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		identity_attribute_pre_delete_interceptors,
		&mut Chain<dyn IdentityAttributePreDeleteInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		identity_attribute_value_post_create_interceptors,
		&mut Chain<dyn IdentityAttributeValuePostCreateInterceptor + Send + Sync>
	);
	delegate_interceptor!(
		identity_attribute_value_pre_delete_interceptors,
		&mut Chain<dyn IdentityAttributeValuePreDeleteInterceptor + Send + Sync>
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
