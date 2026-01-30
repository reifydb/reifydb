// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc, time::Duration};

use reifydb_catalog::{
	catalog::Catalog,
	materialized::MaterializedCatalog,
	vtable::{
		system::flow_operator_store::{FlowOperatorEventListener, FlowOperatorStore},
		tables::UserVTableDataFunction,
		user::{UserVTable, UserVTableColumnDef, registry::UserVTableEntry},
	},
};
use reifydb_cdc::{consume::host::CdcHost, storage::CdcStore};
use reifydb_core::{
	common::CommitVersion,
	error::diagnostic::catalog::namespace_not_found,
	event::{Event, EventBus},
	interface::{
		WithEventBus,
		auth::Identity,
		catalog::{
			column::{ColumnDef, ColumnIndex},
			id::ColumnId,
			vtable::{VTableDef, VTableId},
		},
	},
	util::ioc::IocContainer,
	value::column::columns::Columns,
};
use reifydb_function::{math, registry::Functions, series, subscription};
use reifydb_metric::metric::MetricReader;
use reifydb_rqlv2::compiler::Compiler;
use reifydb_runtime::actor::system::ActorSystem;
use reifydb_transaction::{
	interceptor::factory::InterceptorFactory,
	multi::transaction::MultiTransaction,
	single::SingleTransaction,
	transaction::{admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction},
};
use reifydb_type::{
	error::{Error, diagnostic},
	fragment::Fragment,
	params::Params,
	value::{constraint::TypeConstraint, frame::frame::Frame},
};
use reifydb_vm::{
	pipeline::collect,
	runtime::{context::VmContext, state::VmState},
};
use tracing::instrument;

use crate::{
	bulk_insert::builder::BulkInsertBuilder,
	execute::{Admin, Command, ExecuteAdmin, ExecuteCommand, ExecuteQuery, Executor, Query},
	interceptor::catalog::MaterializedCatalogInterceptor,
};

pub struct StandardEngine(Arc<Inner>);

impl WithEventBus for StandardEngine {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

// Engine methods (formerly from Engine trait in reifydb-core)
impl StandardEngine {
	#[instrument(name = "engine::transaction::begin_command", level = "debug", skip(self))]
	pub fn begin_command(&self) -> crate::Result<CommandTransaction> {
		let mut interceptors = self.interceptors.create();

		interceptors
			.post_commit
			.add(Arc::new(MaterializedCatalogInterceptor::new(self.catalog.materialized.clone())));

		CommandTransaction::new(self.multi.clone(), self.single.clone(), self.event_bus.clone(), interceptors)
	}

	#[instrument(name = "engine::transaction::begin_admin", level = "debug", skip(self))]
	pub fn begin_admin(&self) -> crate::Result<AdminTransaction> {
		let mut interceptors = self.interceptors.create();

		interceptors
			.post_commit
			.add(Arc::new(MaterializedCatalogInterceptor::new(self.catalog.materialized.clone())));

		AdminTransaction::new(self.multi.clone(), self.single.clone(), self.event_bus.clone(), interceptors)
	}

	#[instrument(name = "engine::transaction::begin_query", level = "debug", skip(self))]
	pub fn begin_query(&self) -> crate::Result<QueryTransaction> {
		Ok(QueryTransaction::new(self.multi.begin_query()?, self.single.clone()))
	}

	#[instrument(name = "engine::admin", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn admin_as(&self, identity: &Identity, rql: &str, params: Params) -> Result<Vec<Frame>, Error> {
		(|| {
			let mut txn = self.begin_admin()?;
			let frames = self.executor.execute_admin(
				&mut txn,
				Admin {
					rql,
					params,
					identity,
				},
			)?;
			txn.commit()?;
			Ok(frames)
		})()
		.map_err(|mut err: Error| {
			err.with_statement(rql.to_string());
			err
		})
	}

	#[instrument(name = "engine::command", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn command_as(&self, identity: &Identity, rql: &str, params: Params) -> Result<Vec<Frame>, Error> {
		(|| {
			let mut txn = self.begin_command()?;
			let frames = self.executor.execute_command(
				&mut txn,
				Command {
					rql,
					params,
					identity,
				},
			)?;
			txn.commit()?;
			Ok(frames)
		})()
		.map_err(|mut err: Error| {
			err.with_statement(rql.to_string());
			err
		})
	}

	#[instrument(name = "engine::query", level = "debug", skip(self, params), fields(rql = %rql))]
	pub fn query_as(&self, identity: &Identity, rql: &str, params: Params) -> Result<Vec<Frame>, Error> {
		(|| {
			let mut txn = self.begin_query()?;
			self.executor.execute_query(
				&mut txn,
				Query {
					rql,
					params,
					identity,
				},
			)
		})()
		.map_err(|mut err: Error| {
			err.with_statement(rql.to_string());
			err
		})
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
	/// use reifydb_engine::vtable::{UserVTable, UserVTableColumnDef};
	/// use reifydb_type::value::r#type::Type;
	/// use reifydb_core::value::Columns;
	///
	/// #[derive(Clone)]
	/// struct MyTable;
	///
	/// impl UserVTable for MyTable {
	///     fn definition(&self) -> Vec<UserVTableColumnDef> {
	///         vec![UserVTableColumnDef::new("id", Type::Uint8)]
	///     }
	///     fn get(&self) -> Columns {
	///         // Return column-oriented data
	///         Columns::empty()
	///     }
	/// }
	///
	/// let id = engine.register_virtual_table("default", "my_table", MyTable)?;
	/// ```
	pub fn register_virtual_table<T: UserVTable>(
		&self,
		namespace: &str,
		name: &str,
		table: T,
	) -> crate::Result<VTableId> {
		let catalog = self.materialized_catalog();

		// Look up namespace by name (use max u64 to get latest version)
		let ns_def = catalog
			.find_namespace_by_name(namespace)
			.ok_or_else(|| Error(namespace_not_found(Fragment::None, namespace)))?;

		// Allocate a new table ID
		let table_id = self.executor.virtual_table_registry.allocate_id();
		// Convert user column definitions to internal column definitions
		let table_columns = table.definition();
		let columns = convert_vtable_user_columns_to_column_defs(&table_columns);

		// Create the table definition
		let def = Arc::new(VTableDef {
			id: table_id,
			namespace: ns_def.id,
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
		self.executor.virtual_table_registry.register(ns_def.id, name.to_string(), entry);
		Ok(table_id)
	}

	#[instrument(name = "engine::query_new", level = "debug", skip(self, _params), fields(rql = %rql))]
	pub fn query_new_as(&self, _identity: &Identity, rql: &str, _params: Params) -> Result<Vec<Frame>, Error> {
		let catalog = self.catalog();
		let rql_for_errors = rql.to_string();

		let program = self.compiler.compile(rql)?;

		// Step 2: Create a new transaction for execution
		let mut exec_tx = self.begin_query().map_err(|e| {
			let diagnostic = diagnostic::Diagnostic {
				code: "VM_ERROR".to_string(),
				statement: Some(rql_for_errors.clone()),
				message: format!("Failed to begin transaction for execution: {}", e),
				column: None,
				fragment: Fragment::default(),
				label: None,
				help: None,
				notes: Vec::new(),
				cause: None,
				operator_chain: None,
			};
			Error(diagnostic)
		})?;

		// Step 3: Execute the bytecode
		let context = Arc::new(VmContext::with_catalog(catalog));
		let mut vm = VmState::new(program, context);

		let pipeline = vm
			.execute(&mut exec_tx)
			.map_err(|e| {
				let diagnostic = diagnostic::Diagnostic {
					code: "VM_ERROR".to_string(),
					statement: Some(rql_for_errors.clone()),
					message: format!("Execution failed: {}", e),
					column: None,
					fragment: Fragment::default(),
					label: None,
					help: None,
					notes: Vec::new(),
					cause: None,
					operator_chain: None,
				};
				Error(diagnostic)
			})?
			.ok_or_else(|| {
				let diagnostic = diagnostic::Diagnostic {
					code: "VM_ERROR".to_string(),
					statement: Some(rql_for_errors.clone()),
					message: "Query produced no result".to_string(),
					column: None,
					fragment: Fragment::default(),
					label: None,
					help: None,
					notes: Vec::new(),
					cause: None,
					operator_chain: None,
				};
				Error(diagnostic)
			})?;

		// Step 4: Collect results
		let columns = collect(pipeline).map_err(|e| {
			let diagnostic = diagnostic::Diagnostic {
				code: "VM_ERROR".to_string(),
				statement: Some(rql_for_errors.clone()),
				message: format!("Collection failed: {}", e),
				column: None,
				fragment: Fragment::default(),
				label: None,
				help: None,
				notes: Vec::new(),
				cause: None,
				operator_chain: None,
			};
			Error(diagnostic)
		})?;

		Ok(vec![Frame::from(columns)])
	}

	/// Execute a DDL/DML command using the new RQLv2/VM stack.
	///
	/// This is similar to `command_as()` but uses the new bytecode-based execution.
	#[instrument(name = "engine::command_new", level = "debug", skip(self, _params), fields(rql = %rql))]
	pub fn command_new_as(&self, _identity: &Identity, rql: &str, _params: Params) -> Result<Vec<Frame>, Error> {
		let catalog = self.catalog();
		let rql_for_errors = rql.to_string();

		// Step 1: Compile to bytecode
		let program = self.compiler.compile(rql)?;

		// Step 2: Begin a command transaction
		let mut exec_tx = self.begin_command().map_err(|e| {
			let diagnostic = diagnostic::Diagnostic {
				code: "VM_ERROR".to_string(),
				statement: Some(rql_for_errors.clone()),
				message: format!("Failed to begin command transaction: {}", e),
				column: None,
				fragment: Fragment::default(),
				label: None,
				help: None,
				notes: Vec::new(),
				cause: None,
				operator_chain: None,
			};
			Error(diagnostic)
		})?;

		// Step 3: Execute the bytecode
		let context = Arc::new(VmContext::with_catalog(catalog));
		let mut vm = VmState::new(program, context);

		let pipeline = vm.execute(&mut exec_tx).map_err(|e| {
			let diagnostic = diagnostic::Diagnostic {
				code: "VM_ERROR".to_string(),
				statement: Some(rql_for_errors.clone()),
				message: format!("Command execution failed: {}", e),
				column: None,
				fragment: Fragment::default(),
				label: None,
				help: None,
				notes: Vec::new(),
				cause: None,
				operator_chain: None,
			};
			Error(diagnostic)
		})?;

		// Step 4: Collect results
		let columns = if let Some(pipeline) = pipeline {
			collect(pipeline).map_err(|e| {
				let diagnostic = diagnostic::Diagnostic {
					code: "VM_ERROR".to_string(),
					statement: Some(rql_for_errors.clone()),
					message: format!("Collection failed: {}", e),
					column: None,
					fragment: Fragment::default(),
					label: None,
					help: None,
					notes: Vec::new(),
					cause: None,
					operator_chain: None,
				};
				Error(diagnostic)
			})?
		} else {
			// Commands may not produce output (e.g., CREATE TABLE)
			Columns::empty()
		};

		// Step 5: Commit the transaction
		exec_tx.commit().map_err(|e| {
			let diagnostic = diagnostic::Diagnostic {
				code: "VM_ERROR".to_string(),
				statement: Some(rql_for_errors.clone()),
				message: format!("Failed to commit transaction: {}", e),
				column: None,
				fragment: Fragment::default(),
				label: None,
				help: None,
				notes: Vec::new(),
				cause: None,
				operator_chain: None,
			};
			Error(diagnostic)
		})?;

		Ok(vec![Frame::from(columns)])
	}
}

impl ExecuteAdmin for StandardEngine {
	#[inline]
	fn execute_admin(&self, txn: &mut AdminTransaction, cmd: Admin<'_>) -> crate::Result<Vec<Frame>> {
		self.executor.execute_admin(txn, cmd)
	}
}

impl ExecuteCommand for StandardEngine {
	#[inline]
	fn execute_command(&self, txn: &mut CommandTransaction, cmd: Command<'_>) -> crate::Result<Vec<Frame>> {
		self.executor.execute_command(txn, cmd)
	}
}

impl CdcHost for StandardEngine {
	fn begin_command(&self) -> reifydb_type::Result<CommandTransaction> {
		StandardEngine::begin_command(self)
	}

	fn begin_query(&self) -> reifydb_type::Result<QueryTransaction> {
		StandardEngine::begin_query(self)
	}

	fn current_version(&self) -> reifydb_type::Result<CommitVersion> {
		StandardEngine::current_version(self)
	}

	fn done_until(&self) -> CommitVersion {
		StandardEngine::done_until(self)
	}
}

impl ExecuteQuery for StandardEngine {
	#[inline]
	fn execute_query(&self, txn: &mut QueryTransaction, qry: Query<'_>) -> crate::Result<Vec<Frame>> {
		self.executor.execute_query(txn, qry)
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
	interceptors: Box<dyn InterceptorFactory>,
	catalog: Catalog,
	flow_operator_store: FlowOperatorStore,
	compiler: Compiler,
}

impl StandardEngine {
	pub fn new(
		multi: MultiTransaction,
		single: SingleTransaction,
		event_bus: EventBus,
		interceptors: Box<dyn InterceptorFactory>,
		catalog: Catalog,
		custom_functions: Option<Functions>,
		ioc: IocContainer,
	) -> Self {
		let functions = custom_functions.unwrap_or_else(|| {
			Functions::builder()
				.register_aggregate("math::sum", math::aggregate::sum::Sum::new)
				.register_aggregate("math::min", math::aggregate::min::Min::new)
				.register_aggregate("math::max", math::aggregate::max::Max::new)
				.register_aggregate("math::avg", math::aggregate::avg::Avg::new)
				.register_aggregate("math::count", math::aggregate::count::Count::new)
				.register_scalar("math::abs", math::scalar::abs::Abs::new)
				.register_scalar("math::avg", math::scalar::avg::Avg::new)
				.register_generator("generate_series", series::GenerateSeries::new)
				.register_generator(
					"inspect_subscription",
					subscription::inspect::InspectSubscription::new,
				)
				.build()
		});

		// Create the flow operator store and register the event listener
		let flow_operator_store = FlowOperatorStore::new();
		let listener = FlowOperatorEventListener::new(flow_operator_store.clone());
		event_bus.register(listener);

		// Get the metrics store from IoC to create the stats reader
		let metrics_store = ioc
			.resolve::<reifydb_store_single::SingleStore>()
			.expect("SingleStore must be registered in IocContainer for metrics");
		let stats_reader = MetricReader::new(metrics_store);

		let compiler = ioc.resolve::<Compiler>().expect("Compiler must be registered in IocContainer");

		Self(Arc::new(Inner {
			multi,
			single,
			event_bus,
			executor: Executor::new(
				catalog.clone(),
				functions,
				flow_operator_store.clone(),
				stats_reader,
				ioc,
			),
			interceptors,
			catalog,
			flow_operator_store,
			compiler,
		}))
	}

	/// Begin a query transaction at a specific version.
	///
	/// This is used for parallel query execution where multiple tasks need to
	/// read from the same snapshot (same CommitVersion) for consistency.
	#[instrument(name = "engine::transaction::begin_query_at_version", level = "debug", skip(self), fields(version = %version.0
    ))]
	pub fn begin_query_at_version(&self, version: CommitVersion) -> crate::Result<QueryTransaction> {
		Ok(QueryTransaction::new(self.multi.begin_query_at_version(version)?, self.single.clone()))
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

	#[inline]
	pub fn flow_operator_store(&self) -> &FlowOperatorStore {
		&self.flow_operator_store
	}

	/// Get the current version from the transaction manager
	#[inline]
	pub fn current_version(&self) -> crate::Result<CommitVersion> {
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

	/// Get the CDC store from the IoC container.
	///
	/// Returns the CdcStore that was registered during engine construction.
	/// Panics if CdcStore was not registered.
	#[inline]
	pub fn cdc_store(&self) -> CdcStore {
		self.executor.ioc.resolve::<CdcStore>().expect("CdcStore must be registered")
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
	pub fn bulk_insert<'e>(
		&'e self,
		identity: &'e Identity,
	) -> BulkInsertBuilder<'e, crate::bulk_insert::builder::Validated> {
		BulkInsertBuilder::new(self, identity)
	}

	/// Start a bulk insert operation with validation disabled (trusted mode).
	///
	/// Use this for pre-validated internal data where constraint validation
	/// can be skipped for maximum performance.
	///
	/// # Safety
	///
	/// The caller is responsible for ensuring the data conforms to the
	/// schema constraints. Invalid data may cause undefined behavior.
	pub fn bulk_insert_trusted<'e>(
		&'e self,
		identity: &'e Identity,
	) -> BulkInsertBuilder<'e, crate::bulk_insert::builder::Trusted> {
		BulkInsertBuilder::new_trusted(self, identity)
	}
}

/// Convert user column definitions to internal ColumnDef format.
fn convert_vtable_user_columns_to_column_defs(columns: &[UserVTableColumnDef]) -> Vec<ColumnDef> {
	columns.iter()
		.enumerate()
		.map(|(idx, col)| {
			// Note: For virtual tables, we use unconstrained for all types.
			// The nullable field is still available for documentation purposes.
			let constraint = TypeConstraint::unconstrained(col.data_type);
			ColumnDef {
				id: ColumnId(idx as u64),
				name: col.name.clone(),
				constraint,
				policies: vec![],
				index: ColumnIndex(idx as u8),
				auto_increment: false,
				dictionary_id: None,
			}
		})
		.collect()
}
