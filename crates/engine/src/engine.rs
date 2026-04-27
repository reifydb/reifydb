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
	materialized::MaterializedCatalog,
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
	interceptor::catalog::MaterializedCatalogInterceptor,
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

// Engine methods (formerly from Engine trait in reifydb-core)
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

	/// Get the runtime clock for timestamp operations.
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

	/// Call a procedure by fully-qualified name.
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

	/// Register a user-defined virtual table.
	///
	/// The virtual table will be available for queries using the given namespace and name.
	///
	/// # Arguments
	///
	/// * `namespace` - The namespace name (e.g., "default", "my_namespace")
	/// * `name` - The table name
	/// * `table` - The virtual table implementation
	///
	/// # Returns
	///
	/// The assigned `VTableId` on success.
	///
	/// # Example
	///
	/// ```ignore
	/// use reifydb_engine::vtable::{UserVTable, UserVTableColumn};
	/// use reifydb_type::value::r#type::Type;
	/// use reifydb_core::value::Columns;
	///
	/// #[derive(Clone)]
	/// struct MyTable;
	///
	/// impl UserVTable for MyTable {
	///     fn definition(&self) -> Vec<UserVTableColumn> {
	///         vec![UserVTableColumn::new("id", Type::Uint8)]
	///     }
	///     fn get(&self) -> Columns {
	///         // Return column-oriented data
	///         Columns::empty()
	///     }
	/// }
	///
	/// let id = engine.register_virtual_table("default", "my_table", MyTable)?;
	/// ```
	pub fn register_virtual_table<T: UserVTable>(&self, namespace: &str, name: &str, table: T) -> Result<VTableId> {
		let catalog = self.materialized_catalog();

		// Look up namespace by name (use max u64 to get latest version)
		let ns_def = catalog
			.find_namespace_by_name(namespace)
			.ok_or_else(|| Error(Box::new(namespace_not_found(Fragment::None, namespace))))?;

		// Allocate a new table ID
		let table_id = self.executor.virtual_table_registry.allocate_id();
		// Convert user column definitions to internal column definitions
		let table_columns = table.vtable();
		let columns = convert_vtable_user_columns_to_columns(&table_columns);

		// Create the table definition
		let def = Arc::new(VTable {
			id: table_id,
			namespace: ns_def.id(),
			name: name.to_string(),
			columns,
		});

		// Register in catalog (for resolver lookups)
		catalog.register_vtable_user(def.clone())?;
		// Create the data function from the UserVTable trait
		let data_fn: UserVTableDataFunction = Arc::new(move |_params| table.get());
		// Create and register the entry
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

	fn materialized_catalog(&self) -> &MaterializedCatalog {
		&self.catalog.materialized
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

		// Get the metrics store from IoC to create the stats reader
		let metrics_store = config
			.ioc
			.resolve::<SingleStore>()
			.expect("SingleStore must be registered in IocContainer for metrics");
		let stats_reader = MetricReader::new(metrics_store);

		// Register MaterializedCatalogInterceptor as a factory function.
		let materialized = catalog.materialized.clone();
		interceptors.add_late(Arc::new(move |interceptors: &mut Interceptors| {
			interceptors
				.post_commit
				.add(Arc::new(MaterializedCatalogInterceptor::new(materialized.clone())));
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

	/// Create a new set of interceptors from the factory.
	pub fn create_interceptors(&self) -> Interceptors {
		self.interceptors.create()
	}

	/// Register an additional interceptor factory function.
	///
	/// The function will be called on every `create()` to augment the base interceptors.
	/// This is thread-safe and can be called after the engine is constructed (e.g. by subsystems).
	pub fn add_interceptor_factory(&self, factory: Arc<dyn Fn(&mut Interceptors) + Send + Sync>) {
		self.interceptors.add_late(factory);
	}

	/// Begin a query transaction at a specific version.
	///
	/// This is used for parallel query execution where multiple tasks need to
	/// read from the same snapshot (same CommitVersion) for consistency.
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

	/// Get the actor system
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
	pub fn materialized_catalog(&self) -> &MaterializedCatalog {
		&self.catalog.materialized
	}

	/// Returns a `Catalog` instance for catalog lookups.
	/// The Catalog provides three-tier lookup methods that check transactional changes,
	/// then MaterializedCatalog, then fall back to storage.
	#[inline]
	pub fn catalog(&self) -> Catalog {
		self.catalog.clone()
	}

	/// Returns the shared `Services` instance used by this engine's executor.
	/// External consumers that want to drive volcano operators directly (e.g.
	/// subsystems that build a `QueryContext`) read from the same `Services`
	/// the engine already initialised - avoids duplicating the `Services::new`
	/// wiring path.
	#[inline]
	pub fn services(&self) -> Arc<Services> {
		self.executor.services().clone()
	}

	#[inline]
	pub fn flow_operator_store(&self) -> &SystemFlowOperatorStore {
		&self.flow_operator_store
	}

	/// Get the current version from the transaction manager
	#[inline]
	pub fn current_version(&self) -> Result<CommitVersion> {
		self.multi.current_version()
	}

	/// Returns the highest version where ALL prior versions have completed.
	/// This is useful for CDC polling to know the safe upper bound for fetching
	/// CDC events - all events up to this version are guaranteed to be in storage.
	#[inline]
	pub fn done_until(&self) -> CommitVersion {
		self.multi.done_until()
	}

	/// Wait for the watermark to reach the given version with a timeout.
	/// Returns true if the watermark reached the target, false if timeout occurred.
	#[inline]
	pub fn wait_for_mark_timeout(&self, version: CommitVersion, timeout: Duration) -> bool {
		self.multi.wait_for_mark_timeout(version, timeout)
	}

	#[inline]
	pub fn executor(&self) -> Executor {
		self.executor.clone()
	}

	/// Borrow the IoC container backing this engine. Used by callers that need
	/// to resolve services registered during construction (e.g. observability
	/// providers).
	#[inline]
	pub fn ioc(&self) -> &IocContainer {
		&self.executor.ioc
	}

	/// Get the CDC store from the IoC container.
	///
	/// Returns the CdcStore that was registered during engine construction.
	/// Panics if CdcStore was not registered.
	#[inline]
	pub fn cdc_store(&self) -> CdcStore {
		self.executor.ioc.resolve::<CdcStore>().expect("CdcStore must be registered")
	}

	/// Resolve an actor handle by message type.
	///
	/// Returns `None` if no actor for `M` was registered during engine
	/// construction (e.g. the CDC compact actor is only registered for
	/// persistent backends).
	#[inline]
	pub fn actor<M: 'static>(&self) -> Option<ActorRef<M>>
	where
		ActorRef<M>: Send + Sync,
	{
		self.executor.ioc.try_resolve::<ActorRef<M>>()
	}

	/// Highest commit version processed by the CDC producer actor.
	///
	/// Once this returns `>= V`, every `PostCommitEvent` for versions `<= V`
	/// has been fully handled by the producer, so any CDC row it was going
	/// to write is in storage. Unlike `cdc_store().max_version()`, this
	/// advances even for commits whose deltas were entirely filtered out by
	/// `should_exclude_from_cdc` (e.g. ConfigStorage-only commits), so it is
	/// the correct frontier for "producer is caught up to the engine".
	#[inline]
	pub fn cdc_producer_watermark(&self) -> CommitVersion {
		self.executor
			.ioc
			.resolve::<CdcProducerWatermark>()
			.expect("CdcProducerWatermark must be registered")
			.get()
	}

	/// Mark this engine as read-only (replica mode).
	/// Once set, all write-path methods will return ENG_007 immediately.
	pub fn set_read_only(&self) {
		self.read_only.store(true, Ordering::SeqCst);
	}

	/// Whether this engine is in read-only (replica) mode.
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

	/// Start a bulk insert operation with full validation.
	///
	/// This provides a fluent API for fast bulk inserts that bypasses RQL parsing.
	/// All inserts within a single builder execute in one transaction.
	///
	/// # Example
	///
	/// ```ignore
	/// use reifydb_type::params;
	///
	/// engine.bulk_insert(&identity)
	///     .table("namespace.users")
	///         .row(params!{ id: 1, name: "Alice" })
	///         .row(params!{ id: 2, name: "Bob" })
	///         .done()
	///     .execute()?;
	/// ```
	pub fn bulk_insert<'e>(&'e self, identity: IdentityId) -> BulkInsertBuilder<'e, Validated> {
		BulkInsertBuilder::new(self, identity)
	}

	/// Start a bulk insert that bypasses BOTH constraint validation AND the
	/// oracle's per-key conflict-detection index ("unchecked" mode).
	///
	/// # What this skips beyond `bulk_insert`
	///
	/// `bulk_insert` (the validated default) performs full type/constraint
	/// validation and registers the commit's write set in the oracle's
	/// conflict-detection time-windows so that any concurrent OCC transaction
	/// whose read set overlaps these writes will be aborted at its own commit
	/// time.
	///
	/// `bulk_insert_unchecked` skips both. The commit version still advances
	/// and the watermark still progresses, so any transaction that reads at
	/// version >= this commit will observe the new rows. But concurrent OCC
	/// transactions that already started reading at an older version will
	/// NOT detect that this commit happened underneath them.
	///
	/// # Safety contract - when this is sound
	///
	/// Use this method ONLY when ALL of the following hold for the calling
	/// context:
	///
	/// 1. **Single writer.** This commit is the only writer touching the rows it inserts. No other thread / process
	///    / connection is writing to the same keys concurrently. (For chain ingest: the block-stream consumer is
	///    the only writer, and blocks arrive in monotonic order.)
	///
	/// 2. **No concurrent OCC reader needs to be invalidated.** Any OCC transaction reading at an older version
	///    will silently miss this commit's writes when computing its own conflict set. If your workload has
	///    concurrent user transactions that read these rows, they will commit successfully despite a logical
	///    conflict, and they will see stale data on retry. For trusted ingest where "downstream" readers are
	///    streaming-view operators that consume each new commit on its own merits (not via OCC retry), this is
	///    fine.
	///
	/// 3. **Caller-side well-formedness.** Validation is skipped, so primary key violations or constraint failures
	///    will surface as storage errors at insert time rather than as transaction-level conflicts. The caller must
	///    already ensure the data conforms to the table/ringbuffer shape.
	///
	/// 4. **No need to abort on overlap.** OCC normally aborts a writer whose read set was modified by a more
	///    recent committer. Skipping the index means a concurrent OCC writer with an overlapping read set will
	///    commit through. For trusted ingest where there is no competing OCC writer, this is irrelevant.
	///
	/// In short: safe for sequential, single-writer, append-mostly trusted
	/// ingest where downstream readers don't rely on OCC abort-on-overlap.
	/// Unsafe (silently incorrect) for any workload with concurrent OCC
	/// transactions that read these keys and rely on conflict detection
	/// for correctness.
	pub fn bulk_insert_unchecked<'e>(&'e self, identity: IdentityId) -> BulkInsertBuilder<'e, Unchecked> {
		BulkInsertBuilder::new_unchecked(self, identity)
	}
}

/// Convert user column definitions to internal Column format.
fn convert_vtable_user_columns_to_columns(columns: &[UserVTableColumn]) -> Vec<Column> {
	columns.iter()
		.enumerate()
		.map(|(idx, col)| {
			// Note: For virtual tables, we use unconstrained for all types.
			// The nullable field is still available for documentation purposes.
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
