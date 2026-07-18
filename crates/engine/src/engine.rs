// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ops::Deref,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
};

use reifydb_auth::service::AuthEngine;
use reifydb_catalog::{
	catalog::Catalog,
	interceptor::CatalogCacheInterceptor,
	vtable::{
		system::flow_operator_store::{SystemFlowOperatorEventListener, SystemFlowOperatorStore},
		tables::UserVTableDataFunction,
		user::{UserVTable, UserVTableColumn, registry::UserVTableEntry},
	},
};
use reifydb_cdc::{
	consume::{host::CdcHost, wake::CdcWakeRegistry, watermark::CdcConsumerWatermark},
	produce::watermark::CdcProducerWatermark,
	storage::CdcStore,
};
use reifydb_core::{
	common::CommitVersion,
	error::diagnostic::engine::read_only_rejection,
	event::{Event, EventBus},
	execution::ExecutionResult,
	interface::{
		WithEventBus,
		catalog::{
			column::{Column, ColumnIndex},
			flow::FlowNodeId,
			id::{ColumnId, NamespaceId},
			vtable::{VTable, VTableId},
		},
	},
	util::ioc::IocContainer,
};
use reifydb_metric::storage::metric::MetricReader;
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSpawner},
	context::{clock::Clock, rng::Rng},
	shutdown::Shutdown,
	version_epoch::VersionEpoch,
};
use reifydb_store_multi::tier::read::ReadBufferOperatorMetrics;
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	dictionary::{DictionaryAllocatorRegistry, store::SingleDictionaryStore},
	error::TransactionError,
	interceptor::{factory::InterceptorFactory, interceptors::Interceptors},
	multi::{lease::VersionLeaseGuard, transaction::MultiTransaction},
	single::SingleTransaction,
	transaction::{admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction},
};
use reifydb_value::{
	byte_size::ByteSize,
	error::Error,
	fragment::Fragment,
	params::Params,
	reifydb_assertions,
	value::{constraint::TypeConstraint, duration::Duration, identity::IdentityId},
};
use tracing::instrument;

use crate::{
	Result,
	bulk_insert::builder::{BulkInsertBuilder, Unchecked, Validated},
	vm::{
		Admin, Command, Query, Subscription,
		executor::Executor,
		flow_lineage::ViewLineage,
		services::{EngineConfig, Services},
	},
};

pub struct StandardEngine(Arc<Inner>);

impl WithEventBus for StandardEngine {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

impl AuthEngine for StandardEngine {
	fn begin_admin(&self) -> Result<AdminTransaction> {
		StandardEngine::begin_admin(self, IdentityId::system())
	}

	fn begin_query(&self) -> Result<QueryTransaction> {
		StandardEngine::begin_query(self, IdentityId::system())
	}

	fn catalog(&self) -> Catalog {
		StandardEngine::catalog(self)
	}
}

impl StandardEngine {
	#[instrument(name = "engine::transaction::begin_command", level = "debug", skip(self))]
	pub fn begin_command(&self, identity: IdentityId) -> Result<CommandTransaction> {
		reifydb_assertions! {
			assert!(
				!self.is_read_only(),
				"begin_command called on a read-only engine: writes are permanently disabled after set_read_only(), so any caller reaching this point has bypassed the reject_if_read_only guard (identity={:?})",
				identity
			);
		}
		let interceptors = self.interceptors.create();
		let mut txn = CommandTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.event_bus.clone(),
			interceptors,
			identity,
			self.executor.runtime_context.clock.clone(),
		)?;
		txn.set_executor(Arc::new(self.executor.clone()));
		txn.set_dictionary_allocators(self.dictionary_allocators.clone());
		Ok(txn)
	}

	#[instrument(name = "engine::transaction::begin_admin", level = "debug", skip(self))]
	pub fn begin_admin(&self, identity: IdentityId) -> Result<AdminTransaction> {
		let interceptors = self.interceptors.create();
		let mut txn = AdminTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.event_bus.clone(),
			interceptors,
			identity,
			self.executor.runtime_context.clock.clone(),
		)?;
		txn.set_executor(Arc::new(self.executor.clone()));
		txn.set_dictionary_allocators(self.dictionary_allocators.clone());
		Ok(txn)
	}

	#[instrument(name = "engine::transaction::begin_query", level = "trace", skip(self))]
	pub fn begin_query(&self, identity: IdentityId) -> Result<QueryTransaction> {
		let mut txn = QueryTransaction::new(self.multi.begin_query()?, self.single.clone(), identity);
		txn.set_executor(Arc::new(self.executor.clone()));
		Ok(txn)
	}

	pub fn clock(&self) -> &Clock {
		&self.executor.runtime_context.clock
	}

	pub fn rng(&self) -> &Rng {
		&self.executor.runtime_context.rng
	}

	pub fn version_epoch(&self) -> &VersionEpoch {
		&self.executor.runtime_context.version_epoch
	}

	#[instrument(name = "engine::admin_as", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn admin_as(&self, identity: IdentityId, rql: &str, params: Params) -> ExecutionResult {
		if let Some(e) = self.reject_request(identity) {
			return ExecutionResult::from_error(e);
		}
		let mut txn = match self.begin_admin(identity) {
			Ok(t) => t,
			Err(mut e) => {
				e.with_rql(rql.to_string());
				return ExecutionResult::from_error(e);
			}
		};
		let mut outcome = self.executor.admin(
			&mut txn,
			Admin {
				rql,
				params,
			},
		);
		self.commit_admin(&mut txn, &mut outcome, rql);
		self.annotate_rql(&mut outcome, rql);
		outcome
	}

	fn reject_request(&self, identity: IdentityId) -> Option<Error> {
		if let Err(e) = self.reject_if_read_only() {
			return Some(e);
		}
		if let Err(e) = self.reject_if_shutting_down(identity) {
			return Some(e);
		}
		None
	}

	#[inline]
	fn commit_admin(&self, txn: &mut AdminTransaction, outcome: &mut ExecutionResult, rql: &str) {
		if outcome.is_ok()
			&& let Err(mut e) = txn.commit()
		{
			e.with_rql(rql.to_string());
			outcome.error = Some(e);
		}
	}

	fn annotate_rql(&self, outcome: &mut ExecutionResult, rql: &str) {
		if let Some(ref mut e) = outcome.error {
			e.with_rql(rql.to_string());
		}
		reifydb_assertions! {
			let annotated = outcome.error.as_ref().map(|e| e.rql.is_some());
			assert!(
				annotated != Some(false),
				"annotate_rql is the single catch-all that attaches the originating query to every error leaving admin_as/command_as; an error reaching the user with rql=None (annotated={:?}) would render a diagnostic with no source query, defeating user-facing error reporting",
				annotated
			);
		}
	}

	#[instrument(name = "engine::command_as", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn command_as(&self, identity: IdentityId, rql: &str, params: Params) -> ExecutionResult {
		if let Some(e) = self.reject_request(identity) {
			return ExecutionResult::from_error(e);
		}
		let mut txn = match self.begin_command(identity) {
			Ok(t) => t,
			Err(mut e) => {
				e.with_rql(rql.to_string());
				return ExecutionResult::from_error(e);
			}
		};
		let mut outcome = self.executor.command(
			&mut txn,
			Command {
				rql,
				params,
			},
		);
		self.commit_command(&mut txn, &mut outcome, rql);
		self.annotate_rql(&mut outcome, rql);
		outcome
	}

	#[inline]
	fn commit_command(&self, txn: &mut CommandTransaction, outcome: &mut ExecutionResult, rql: &str) {
		if outcome.is_ok()
			&& let Err(mut e) = txn.commit()
		{
			e.with_rql(rql.to_string());
			outcome.error = Some(e);
		}
	}

	#[instrument(name = "engine::query_as", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn query_as(&self, identity: IdentityId, rql: &str, params: Params) -> ExecutionResult {
		let mut txn = match self.begin_query(identity) {
			Ok(t) => t,
			Err(mut e) => {
				e.with_rql(rql.to_string());
				return ExecutionResult::from_error(e);
			}
		};
		let mut outcome = self.executor.query(
			&mut txn,
			Query {
				rql,
				params,
			},
		);
		if let Some(ref mut e) = outcome.error {
			e.with_rql(rql.to_string());
		}
		outcome
	}

	#[instrument(name = "engine::query_as_at_version", level = "debug", skip(self, params, lease), fields(rql = %rql, version = %lease.version().0))]
	pub fn query_as_at_version(
		&self,
		identity: IdentityId,
		rql: &str,
		params: Params,
		lease: &VersionLeaseGuard,
	) -> ExecutionResult {
		let mut txn = match self.begin_query_at_version(lease, identity) {
			Ok(t) => t,
			Err(mut e) => {
				e.with_rql(rql.to_string());
				return ExecutionResult::from_error(e);
			}
		};
		let mut outcome = self.executor.query(
			&mut txn,
			Query {
				rql,
				params,
			},
		);
		if let Some(ref mut e) = outcome.error {
			e.with_rql(rql.to_string());
		}
		outcome
	}

	#[instrument(name = "engine::query_in_txn", level = "debug", skip(self, txn, params), fields(rql = %rql))]
	pub fn query_in_txn(&self, txn: &mut QueryTransaction, rql: &str, params: Params) -> ExecutionResult {
		let mut outcome = self.executor.query(
			txn,
			Query {
				rql,
				params,
			},
		);
		if let Some(ref mut e) = outcome.error {
			e.with_rql(rql.to_string());
		}
		outcome
	}

	#[instrument(name = "engine::subscribe_as", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn subscribe_as(&self, identity: IdentityId, rql: &str, params: Params) -> ExecutionResult {
		let mut txn = match self.begin_query(identity) {
			Ok(t) => t,
			Err(mut e) => {
				e.with_rql(rql.to_string());
				return ExecutionResult::from_error(e);
			}
		};
		let mut outcome = self.executor.subscription(
			&mut txn,
			Subscription {
				rql,
				params,
			},
		);
		if let Some(ref mut e) = outcome.error {
			e.with_rql(rql.to_string());
		}
		outcome
	}

	#[instrument(name = "engine::procedure_as", level = "debug", skip(self, params), fields(name = %name))]
	pub fn procedure_as(&self, identity: IdentityId, name: &str, params: Params) -> ExecutionResult {
		if let Err(e) = self.reject_if_read_only() {
			return ExecutionResult::from_error(e);
		}
		if let Err(e) = self.reject_if_shutting_down(identity) {
			return ExecutionResult::from_error(e);
		}
		let mut txn = match self.begin_command(identity) {
			Ok(t) => t,
			Err(e) => {
				return ExecutionResult::from_error(e);
			}
		};
		let mut outcome = self.executor.call_procedure(&mut txn, name, &params);
		if outcome.is_ok()
			&& let Err(e) = txn.commit()
		{
			outcome.error = Some(e);
		}
		outcome
	}

	pub fn register_virtual_table<T: UserVTable>(
		&self,
		namespace_id: NamespaceId,
		name: &str,
		table: T,
	) -> Result<VTableId> {
		let catalog = self.catalog();
		let table_id = self.executor.virtual_table_registry.allocate_id();

		let table_columns = table.vtable();
		let columns = convert_vtable_user_columns_to_columns(&table_columns);

		let def = Arc::new(VTable {
			id: table_id,
			namespace: namespace_id,
			name: name.to_string(),
			columns,
		});

		catalog.register_vtable_user(def.clone())?;

		let data_fn: UserVTableDataFunction = Arc::new(move |_params| table.get());

		let entry = UserVTableEntry {
			def: def.clone(),
			data_fn,
		};
		self.executor.virtual_table_registry.register(namespace_id, name.to_string(), entry);
		Ok(table_id)
	}
}

impl CdcHost for StandardEngine {
	fn begin_command(&self) -> Result<CommandTransaction> {
		StandardEngine::begin_command(self, IdentityId::system())
	}

	fn begin_query(&self) -> Result<QueryTransaction> {
		StandardEngine::begin_query(self, IdentityId::system())
	}

	fn current_version(&self) -> Result<CommitVersion> {
		StandardEngine::current_version(self)
	}

	fn done_until(&self) -> CommitVersion {
		StandardEngine::done_until(self)
	}

	fn cdc_producer_watermark(&self) -> CommitVersion {
		StandardEngine::cdc_producer_watermark(self)
	}

	fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool {
		StandardEngine::wait_for_mark_timeout(self, version, timeout)
	}

	fn notify_on_mark(&self, version: CommitVersion, callback: Box<dyn FnOnce() + Send>) {
		StandardEngine::notify_on_mark(self, version, callback);
	}

	fn catalog(&self) -> &Catalog {
		&self.catalog
	}
}

impl Clone for StandardEngine {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl Deref for StandardEngine {
	type Target = Inner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

pub struct Inner {
	multi: MultiTransaction,
	single: SingleTransaction,
	event_bus: EventBus,
	executor: Executor,
	interceptors: Arc<InterceptorFactory>,
	catalog: Catalog,
	flow_operator_store: SystemFlowOperatorStore,
	dictionary_allocators: DictionaryAllocatorRegistry,
	read_only: AtomicBool,
	shutting_down: AtomicBool,
}

impl StandardEngine {
	pub fn new(
		multi: MultiTransaction,
		single: SingleTransaction,
		event_bus: EventBus,
		interceptors: InterceptorFactory,
		catalog: Catalog,
		config: EngineConfig,
	) -> Self {
		let flow_operator_store = SystemFlowOperatorStore::new();
		let listener = SystemFlowOperatorEventListener::new(flow_operator_store.clone());
		event_bus.register(listener);

		let metrics_store = config
			.ioc
			.resolve::<SingleStore>()
			.expect("SingleStore must be registered in IocContainer for metrics");
		let stats_reader = MetricReader::new(metrics_store);

		let catalog_for_interceptor = catalog.clone();
		interceptors.add_late(Arc::new(move |interceptors: &mut Interceptors| {
			interceptors.post_commit.add(Arc::new(CatalogCacheInterceptor::new(&catalog_for_interceptor)));
		}));

		let interceptors = Arc::new(interceptors);

		let dictionary_allocators =
			DictionaryAllocatorRegistry::new(Arc::new(SingleDictionaryStore::new(single.clone())));

		Self(Arc::new(Inner {
			multi,
			single,
			event_bus,
			executor: Executor::new(catalog.clone(), config, flow_operator_store.clone(), stats_reader),
			interceptors,
			catalog,
			flow_operator_store,
			dictionary_allocators,
			read_only: AtomicBool::new(false),
			shutting_down: AtomicBool::new(false),
		}))
	}

	pub fn create_interceptors(&self) -> Interceptors {
		self.interceptors.create()
	}

	pub fn dictionary_allocators(&self) -> DictionaryAllocatorRegistry {
		self.dictionary_allocators.clone()
	}

	pub fn add_interceptor_factory(&self, factory: Arc<dyn Fn(&mut Interceptors) + Send + Sync>) {
		self.interceptors.add_late(factory);
	}

	#[instrument(name = "engine::transaction::begin_query_at_version", level = "trace", skip(self, lease), fields(version = %lease.version().0
    ))]
	pub fn begin_query_at_version(
		&self,
		lease: &VersionLeaseGuard,
		identity: IdentityId,
	) -> Result<QueryTransaction> {
		let mut txn =
			QueryTransaction::new(self.multi.begin_query_at_version(lease)?, self.single.clone(), identity);
		txn.set_executor(Arc::new(self.executor.clone()));
		Ok(txn)
	}

	#[instrument(name = "engine::acquire_version_lease", level = "trace", skip(self), fields(version = %version.0))]
	pub fn acquire_version_lease(&self, version: CommitVersion) -> Result<VersionLeaseGuard> {
		self.multi.acquire_version_lease(version)
	}

	#[instrument(name = "engine::acquire_current_snapshot_lease", level = "trace", skip(self))]
	pub fn acquire_current_snapshot_lease(&self) -> Result<(CommitVersion, VersionLeaseGuard)> {
		self.multi.acquire_current_snapshot_lease()
	}

	#[inline]
	pub fn multi(&self) -> &MultiTransaction {
		&self.multi
	}

	#[inline]
	pub fn multi_owned(&self) -> MultiTransaction {
		self.multi.clone()
	}

	#[inline]
	pub fn spawner(&self) -> ActorSpawner {
		self.multi.spawner()
	}

	#[inline]
	pub fn single(&self) -> &SingleTransaction {
		&self.single
	}

	#[inline]
	pub fn single_owned(&self) -> SingleTransaction {
		self.single.clone()
	}

	#[inline]
	pub fn emit<E: Event>(&self, event: E) {
		self.event_bus.emit(event)
	}

	#[inline]
	pub fn catalog(&self) -> Catalog {
		self.catalog.clone()
	}

	#[inline]
	pub fn services(&self) -> Arc<Services> {
		self.executor.services().clone()
	}

	#[inline]
	pub fn flow_operator_store(&self) -> &SystemFlowOperatorStore {
		&self.flow_operator_store
	}

	#[inline]
	pub fn current_version(&self) -> Result<CommitVersion> {
		self.multi.current_version()
	}

	#[inline]
	pub fn done_until(&self) -> CommitVersion {
		self.multi.done_until()
	}

	#[inline]
	pub fn query_done_until(&self) -> CommitVersion {
		self.multi.query_done_until()
	}

	#[inline]
	pub fn oracle_window_count(&self) -> usize {
		self.multi.oracle_window_count()
	}

	#[inline]
	pub fn read_buffer_operator_metrics(&self) -> Vec<ReadBufferOperatorMetrics> {
		self.multi.store().read_buffer_operator_metrics()
	}

	#[inline]
	pub fn operator_disk_payload_bytes(&self) -> Vec<(FlowNodeId, ByteSize)> {
		self.multi.store().operator_disk_payload_bytes()
	}

	#[inline]
	pub fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool {
		self.multi.wait_for_mark_timeout(version, timeout)
	}

	#[inline]
	pub fn notify_on_mark(&self, version: CommitVersion, callback: Box<dyn FnOnce() + Send>) {
		self.multi.notify_on_mark(version, callback);
	}

	#[inline]
	pub fn executor(&self) -> Executor {
		self.executor.clone()
	}

	#[inline]
	pub fn view_lineage(&self) -> ViewLineage {
		self.executor.view_lineage.clone()
	}

	#[inline]
	pub fn ioc(&self) -> &IocContainer {
		&self.executor.ioc
	}

	#[inline]
	pub fn cdc_store(&self) -> CdcStore {
		self.executor.ioc.resolve::<CdcStore>().expect("CdcStore must be registered")
	}

	#[inline]
	pub fn actor<M: 'static>(&self) -> Option<ActorRef<M>>
	where
		ActorRef<M>: Send + Sync,
	{
		self.executor.ioc.try_resolve::<ActorRef<M>>()
	}

	#[inline]
	pub fn cdc_producer_watermark(&self) -> CommitVersion {
		self.executor.ioc.try_resolve::<CdcProducerWatermark>().map(|w| w.get()).unwrap_or(CommitVersion(0))
	}

	#[inline]
	pub fn cdc_consumer_watermark(&self) -> CommitVersion {
		self.executor.ioc.try_resolve::<CdcConsumerWatermark>().map(|w| w.get()).unwrap_or(CommitVersion(0))
	}

	#[inline]
	pub fn notify_cdc_consumers(&self) {
		if let Some(registry) = self.executor.ioc.try_resolve::<CdcWakeRegistry>() {
			registry.notify_all();
		}
	}

	pub fn set_read_only(&self) {
		self.read_only.store(true, Ordering::SeqCst);
	}

	pub fn is_read_only(&self) -> bool {
		self.read_only.load(Ordering::SeqCst)
	}

	pub(crate) fn reject_if_read_only(&self) -> Result<()> {
		if self.is_read_only() {
			return Err(Error(Box::new(read_only_rejection(Fragment::None))));
		}
		Ok(())
	}

	pub fn set_shutting_down(&self) {
		self.shutting_down.store(true, Ordering::SeqCst);
	}

	pub fn is_shutting_down(&self) -> bool {
		self.shutting_down.load(Ordering::SeqCst)
	}

	pub(crate) fn reject_if_shutting_down(&self, identity: IdentityId) -> Result<()> {
		if self.is_shutting_down() && !identity.is_system() {
			return Err(TransactionError::ShuttingDown.into());
		}
		Ok(())
	}

	pub fn bulk_insert<'e>(&'e self, identity: IdentityId) -> BulkInsertBuilder<'e, Validated> {
		BulkInsertBuilder::new(self, identity)
	}

	pub fn bulk_insert_unchecked<'e>(&'e self, identity: IdentityId) -> BulkInsertBuilder<'e, Unchecked> {
		BulkInsertBuilder::new_unchecked(self, identity)
	}
}

impl Shutdown for StandardEngine {
	fn shutdown(&self) {
		self.interceptors.clear_late();
		self.executor.ioc.clear();
		self.executor.virtual_table_registry.clear();
		self.multi().store().clear_eviction_watermark();
		#[cfg(not(reifydb_single_threaded))]
		if let Some(registry) = self.executor.remote_registry.as_ref() {
			registry.shutdown();
		}
	}
}

fn convert_vtable_user_columns_to_columns(columns: &[UserVTableColumn]) -> Vec<Column> {
	columns.iter()
		.enumerate()
		.map(|(idx, col)| {
			let constraint = TypeConstraint::unconstrained(col.data_type.clone());
			Column {
				id: ColumnId(idx as u64),
				name: col.name.clone(),
				constraint,
				properties: vec![],
				index: ColumnIndex(idx as u8),
				auto_increment: false,
				dictionary_id: None,
			}
		})
		.collect()
}
