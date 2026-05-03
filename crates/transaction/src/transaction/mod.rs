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
	execution::ExecutionResult,
	interface::{
		catalog::{policy::SessionOp, shape::ShapeId},
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
	value::{datetime::DateTime, identity::IdentityId},
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
		replica::ReplicaTransaction, write::Write,
	},
};

pub trait RqlExecutor: Send + Sync {
	fn rql(&self, tx: &mut Transaction<'_>, rql: &str, params: Params) -> ExecutionResult;
}

pub mod admin;
pub mod catalog;
pub mod command;
pub mod query;
pub mod replica;
pub mod write;

use crate::multi::{pending::PendingWrites, transaction::write::MultiWriteTransaction};

#[inline]
pub(super) fn collect_transaction_writes(pending: &PendingWrites) -> Vec<(EncodedKey, Option<EncodedRow>)> {
	pending.iter()
		.map(|(key, p)| match &p.delta {
			Delta::Set {
				row,
				..
			} => (key.clone(), Some(row.clone())),
			_ => (key.clone(), None),
		})
		.collect()
}

#[inline]
pub(super) fn apply_pre_commit_writes(
	multi: &mut MultiWriteTransaction,
	pending_writes: &[(EncodedKey, Option<EncodedRow>)],
) -> Result<()> {
	for (key, value) in pending_writes {
		match value {
			Some(v) => multi.set(key, v.clone())?,
			None => multi.remove(key)?,
		}
	}
	Ok(())
}

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
			session_type,
			session_default_deny,
		}
	}

	pub fn restore(&mut self) {
		if let Some(sp) = self.savepoint.take() {
			self.inner.cmd.as_mut().unwrap().restore_savepoint(sp.write);
			self.inner.row_changes.truncate(sp.row_changes_len);
			self.inner.accumulator.truncate(sp.accumulator_len);
			self.inner.changes.restore_savepoint(sp.changes);
			self.inner.unpoison();
		}
	}

	pub fn reborrow(&mut self) -> TestTransaction<'_> {
		TestTransaction {
			inner: &mut *self.inner,
			baseline: self.baseline,
			events: &mut *self.events,
			invocations: &mut *self.invocations,
			event_seq: &mut *self.event_seq,
			handler_seq: &mut *self.handler_seq,
			savepoint: None,
			session_type: self.session_type,
			session_default_deny: self.session_default_deny,
		}
	}

	pub fn accumulator_entries_from(&self) -> &[(ShapeId, Diff)] {
		self.inner.accumulator.entries_from(self.baseline)
	}

	pub fn capture_testing_pre_commit(&mut self) -> Result<()> {
		let has_source_changes = self
			.inner
			.accumulator
			.entries_from(self.baseline)
			.iter()
			.any(|(id, _)| !matches!(id, ShapeId::View(_)));

		if !has_source_changes {
			return Ok(());
		}

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
			flow_changes: self.inner.accumulator.take_changes_from(
				offset,
				CommitVersion(0),
				DateTime::from_nanos(self.inner.clock.now_nanos()),
			),
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
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		match self {
			Transaction::Command(txn) => txn.range(range, batch_size),
			Transaction::Admin(txn) => txn.range(range, batch_size),
			Transaction::Query(txn) => Ok(txn.range(range, batch_size)),
			Transaction::Test(t) => t.inner.range(range, batch_size),
			Transaction::Replica(txn) => txn.range(range, batch_size),
		}
	}

	pub fn range_rev(
		&mut self,
		range: EncodedKeyRange,
		batch_size: usize,
	) -> Result<Box<dyn Iterator<Item = Result<MultiVersionRow>> + Send + '_>> {
		match self {
			Transaction::Command(txn) => txn.range_rev(range, batch_size),
			Transaction::Admin(txn) => txn.range_rev(range, batch_size),
			Transaction::Query(txn) => Ok(txn.range_rev(range, batch_size)),
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

	pub fn command_mut(&mut self) -> &mut CommandTransaction {
		match self {
			Self::Command(txn) => txn,
			_ => panic!("Expected Command transaction"),
		}
	}

	pub fn admin_mut(&mut self) -> &mut AdminTransaction {
		match self {
			Self::Admin(txn) => txn,
			Self::Test(t) => t.inner,
			_ => panic!("Expected Admin transaction"),
		}
	}

	pub fn query_mut(&mut self) -> &mut QueryTransaction {
		match self {
			Self::Query(txn) => txn,
			_ => panic!("Expected Query transaction"),
		}
	}

	pub fn replica_mut(&mut self) -> &mut ReplicaTransaction {
		match self {
			Self::Replica(txn) => txn,
			_ => panic!("Expected Replica transaction"),
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

	fn write_ops(&mut self) -> &mut dyn Write {
		match self {
			Transaction::Command(txn) => &mut **txn,
			Transaction::Admin(txn) => &mut **txn,
			Transaction::Query(_) => panic!("Write operations not supported on Query transaction"),
			Transaction::Test(t) => &mut *t.inner,
			Transaction::Replica(txn) => &mut **txn,
		}
	}

	pub fn set(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		Write::set(self.write_ops(), key, row)
	}

	pub fn unset(&mut self, key: &EncodedKey, row: EncodedRow) -> Result<()> {
		Write::unset(self.write_ops(), key, row)
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Result<()> {
		Write::remove(self.write_ops(), key)
	}

	pub fn mark_preexisting(&mut self, key: &EncodedKey) -> Result<()> {
		Write::mark_preexisting(self.write_ops(), key)
	}

	pub fn track_row_change(&mut self, change: RowChange) {
		Write::track_row_change(self.write_ops(), change)
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
