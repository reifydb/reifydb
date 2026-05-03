// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	ops::Deref,
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

use reifydb_auth::service::AuthEngine;
use reifydb_catalog::{
	catalog::Catalog,
	vtable::{
		system::flow_operator_store::{SystemFlowOperatorEventListener, SystemFlowOperatorStore},
		tables::UserVTableDataFunction,
		user::{UserVTable, UserVTableColumn, registry::UserVTableEntry},
	},
};
use reifydb_cdc::{consume::host::CdcHost, produce::watermark::CdcProducerWatermark, storage::CdcStore};
use reifydb_core::{
	common::CommitVersion,
	error::diagnostic::{catalog::namespace_not_found, engine::read_only_rejection},
	event::{Event, EventBus},
	execution::ExecutionResult,
	interface::{
		WithEventBus,
		catalog::{
			column::{Column, ColumnIndex},
			id::ColumnId,
			vtable::{VTable, VTableId},
		},
	},
	metric::ExecutionMetrics,
	util::ioc::IocContainer,
};
use reifydb_metric::storage::metric::MetricReader;
use reifydb_runtime::{
	actor::{mailbox::ActorRef, system::ActorSystem},
	context::{clock::Clock, rng::Rng},
};
use reifydb_store_single::SingleStore;
use reifydb_transaction::{
	interceptor::{factory::InterceptorFactory, interceptors::Interceptors},
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction},
};
use reifydb_type::{
	error::Error,
	fragment::Fragment,
	params::Params,
	value::{constraint::TypeConstraint, identity::IdentityId},
};
use tracing::instrument;

use crate::{
	Result,
	bulk_insert::builder::{BulkInsertBuilder, Unchecked, Validated},
	vm::{
		Admin, Command, Query, Subscription,
		executor::Executor,
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
		Ok(txn)
	}

	#[instrument(name = "engine::transaction::begin_query", level = "debug", skip(self))]
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

	#[instrument(name = "engine::admin_as", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn admin_as(&self, identity: IdentityId, rql: &str, params: Params) -> ExecutionResult {
		if let Err(e) = self.reject_if_read_only() {
			return ExecutionResult {
				frames: vec![],
				error: Some(e),
				metrics: ExecutionMetrics::default(),
			};
		}
		let mut txn = match self.begin_admin(identity) {
			Ok(t) => t,
			Err(mut e) => {
				e.with_rql(rql.to_string());
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};
		let mut outcome = self.executor.admin(
			&mut txn,
			Admin {
				rql,
				params,
			},
		);
		if outcome.is_ok()
			&& let Err(mut e) = txn.commit()
		{
			e.with_rql(rql.to_string());
			outcome.error = Some(e);
		}
		if let Some(ref mut e) = outcome.error {
			e.with_rql(rql.to_string());
		}
		outcome
	}

	#[instrument(name = "engine::command_as", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn command_as(&self, identity: IdentityId, rql: &str, params: Params) -> ExecutionResult {
		if let Err(e) = self.reject_if_read_only() {
			return ExecutionResult {
				frames: vec![],
				error: Some(e),
				metrics: ExecutionMetrics::default(),
			};
		}
		let mut txn = match self.begin_command(identity) {
			Ok(t) => t,
			Err(mut e) => {
				e.with_rql(rql.to_string());
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
			}
		};
		let mut outcome = self.executor.command(
			&mut txn,
			Command {
				rql,
				params,
			},
		);
		if outcome.is_ok()
			&& let Err(mut e) = txn.commit()
		{
			e.with_rql(rql.to_string());
			outcome.error = Some(e);
		}
		if let Some(ref mut e) = outcome.error {
			e.with_rql(rql.to_string());
		}
		outcome
	}

	#[instrument(name = "engine::query_as", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn query_as(&self, identity: IdentityId, rql: &str, params: Params) -> ExecutionResult {
		let mut txn = match self.begin_query(identity) {
			Ok(t) => t,
			Err(mut e) => {
				e.with_rql(rql.to_string());
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
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

	#[instrument(name = "engine::subscribe_as", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn subscribe_as(&self, identity: IdentityId, rql: &str, params: Params) -> ExecutionResult {
		let mut txn = match self.begin_query(identity) {
			Ok(t) => t,
			Err(mut e) => {
				e.with_rql(rql.to_string());
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
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
			return ExecutionResult {
				frames: vec![],
				error: Some(e),
				metrics: ExecutionMetrics::default(),
			};
		}
		let mut txn = match self.begin_command(identity) {
			Ok(t) => t,
			Err(e) => {
				return ExecutionResult {
					frames: vec![],
					error: Some(e),
					metrics: ExecutionMetrics::default(),
				};
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

	pub fn register_virtual_table<T: UserVTable>(&self, namespace: &str, name: &str, table: T) -> Result<VTableId> {
		let catalog = self.catalog();

		let ns_def = catalog
			.materialized()
			.find_namespace_by_name(namespace)
			.ok_or_else(|| Error(Box::new(namespace_not_found(Fragment::None, namespace))))?;

		let table_id = self.executor.virtual_table_registry.allocate_id();

		let table_columns = table.vtable();
		let columns = convert_vtable_user_columns_to_columns(&table_columns);

		let def = Arc::new(VTable {
			id: table_id,
			namespace: ns_def.id(),
			name: name.to_string(),
			columns,
		});

		catalog.register_vtable_user(def.clone())?;

		let data_fn: UserVTableDataFunction = Arc::new(move |_params| table.get());

		let entry = UserVTableEntry {
			def: def.clone(),
			data_fn,
		};
		self.executor.virtual_table_registry.register(ns_def.id(), name.to_string(), entry);
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

	fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool {
		StandardEngine::wait_for_mark_timeout(self, version, timeout)
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
	read_only: AtomicBool,
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
			interceptors.post_commit.add(catalog_for_interceptor.post_commit_interceptor());
		}));

		let interceptors = Arc::new(interceptors);

		Self(Arc::new(Inner {
			multi,
			single,
			event_bus,
			executor: Executor::new(catalog.clone(), config, flow_operator_store.clone(), stats_reader),
			interceptors,
			catalog,
			flow_operator_store,
			read_only: AtomicBool::new(false),
		}))
	}

	pub fn create_interceptors(&self) -> Interceptors {
		self.interceptors.create()
	}

	pub fn add_interceptor_factory(&self, factory: Arc<dyn Fn(&mut Interceptors) + Send + Sync>) {
		self.interceptors.add_late(factory);
	}

	#[instrument(name = "engine::transaction::begin_query_at_version", level = "debug", skip(self), fields(version = %version.0
    ))]
	pub fn begin_query_at_version(&self, version: CommitVersion, identity: IdentityId) -> Result<QueryTransaction> {
		let mut txn = QueryTransaction::new(
			self.multi.begin_query_at_version(version)?,
			self.single.clone(),
			identity,
		);
		txn.set_executor(Arc::new(self.executor.clone()));
		Ok(txn)
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
	pub fn actor_system(&self) -> ActorSystem {
		self.multi.actor_system()
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
	pub fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool {
		self.multi.wait_for_mark_timeout(version, timeout)
	}

	#[inline]
	pub fn executor(&self) -> Executor {
		self.executor.clone()
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
		self.executor
			.ioc
			.resolve::<CdcProducerWatermark>()
			.expect("CdcProducerWatermark must be registered")
			.get()
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

	pub fn shutdown(&self) {
		self.interceptors.clear_late();
		self.executor.ioc.clear();
	}

	pub fn bulk_insert<'e>(&'e self, identity: IdentityId) -> BulkInsertBuilder<'e, Validated> {
		BulkInsertBuilder::new(self, identity)
	}

	pub fn bulk_insert_unchecked<'e>(&'e self, identity: IdentityId) -> BulkInsertBuilder<'e, Unchecked> {
		BulkInsertBuilder::new_unchecked(self, identity)
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
