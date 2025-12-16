// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{ops::Deref, rc::Rc, sync::Arc, time::Duration};

use reifydb_catalog::MaterializedCatalog;
use reifydb_core::{
	CommitVersion, Frame,
	event::{Event, EventBus},
	interceptor::InterceptorFactory,
	interface::{
		ColumnDef, ColumnId, ColumnIndex, Command, Engine as EngineInterface, ExecuteCommand, ExecuteQuery,
		Identity, Params, Query, TableVirtualDef, TableVirtualId, WithEventBus,
	},
};
use reifydb_transaction::{
	cdc::TransactionCdc,
	multi::{AwaitWatermarkError, TransactionMultiVersion},
	single::TransactionSingleVersion,
};
use reifydb_type::{OwnedFragment, TypeConstraint};
use tracing::instrument;

use crate::{
	execute::Executor,
	function::{Functions, generator, math},
	interceptor::{CatalogEventInterceptor, materialized_catalog::MaterializedCatalogInterceptor},
	table_virtual::{
		IteratorVirtualTableFactory, SimpleVirtualTableFactory, TableVirtualUser, TableVirtualUserColumnDef,
		TableVirtualUserIterator,
		system::{FlowOperatorEventListener, FlowOperatorStore},
	},
	transaction::{StandardCommandTransaction, StandardQueryTransaction},
};

pub struct StandardEngine(Arc<EngineInner>);

impl WithEventBus for StandardEngine {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

impl EngineInterface for StandardEngine {
	type Command = StandardCommandTransaction;
	type Query = StandardQueryTransaction;

	#[instrument(level = "debug", skip(self))]
	fn begin_command(&self) -> crate::Result<Self::Command> {
		let mut interceptors = self.interceptors.create();

		interceptors.post_commit.add(Rc::new(MaterializedCatalogInterceptor::new(self.catalog.clone())));
		interceptors
			.post_commit
			.add(Rc::new(CatalogEventInterceptor::new(self.event_bus.clone(), self.catalog.clone())));

		StandardCommandTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.cdc.clone(),
			self.event_bus.clone(),
			self.catalog.clone(),
			interceptors,
		)
	}

	#[instrument(level = "debug", skip(self))]
	fn begin_query(&self) -> crate::Result<Self::Query> {
		Ok(StandardQueryTransaction::new(
			self.multi.begin_query()?,
			self.single.clone(),
			self.cdc.clone(),
			self.catalog.clone(),
		))
	}

	#[instrument(level = "info", skip(self, params), fields(rql = %rql))]
	fn command_as(&self, identity: &Identity, rql: &str, params: Params) -> crate::Result<Vec<Frame>> {
		let mut txn = self.begin_command()?;
		let result = self.execute_command(
			&mut txn,
			Command {
				rql,
				params,
				identity,
			},
		)?;
		txn.commit()?;
		Ok(result)
	}

	#[instrument(level = "info", skip(self, params), fields(rql = %rql))]
	fn query_as(&self, identity: &Identity, rql: &str, params: Params) -> crate::Result<Vec<Frame>> {
		let mut txn = self.begin_query()?;
		let result = self.execute_query(
			&mut txn,
			Query {
				rql,
				params,
				identity,
			},
		)?;
		Ok(result)
	}
}

impl ExecuteCommand<StandardCommandTransaction> for StandardEngine {
	#[inline]
	fn execute_command(&self, txn: &mut StandardCommandTransaction, cmd: Command<'_>) -> crate::Result<Vec<Frame>> {
		self.executor.execute_command(txn, cmd)
	}
}

impl ExecuteQuery<StandardQueryTransaction> for StandardEngine {
	#[inline]
	fn execute_query(&self, txn: &mut StandardQueryTransaction, qry: Query<'_>) -> crate::Result<Vec<Frame>> {
		self.executor.execute_query(txn, qry)
	}
}

impl Clone for StandardEngine {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl Deref for StandardEngine {
	type Target = EngineInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

pub struct EngineInner {
	multi: TransactionMultiVersion,
	single: TransactionSingleVersion,
	cdc: TransactionCdc,
	event_bus: EventBus,
	executor: Executor,
	interceptors: Box<dyn InterceptorFactory<StandardCommandTransaction>>,
	catalog: MaterializedCatalog,
	flow_operator_store: FlowOperatorStore,
}

impl StandardEngine {
	pub fn new(
		multi: TransactionMultiVersion,
		single: TransactionSingleVersion,
		cdc: TransactionCdc,
		event_bus: EventBus,
		interceptors: Box<dyn InterceptorFactory<StandardCommandTransaction>>,
		catalog: MaterializedCatalog,
	) -> Self {
		Self::with_functions(multi, single, cdc, event_bus, interceptors, catalog, None)
	}

	pub fn with_functions(
		multi: TransactionMultiVersion,
		single: TransactionSingleVersion,
		cdc: TransactionCdc,
		event_bus: EventBus,
		interceptors: Box<dyn InterceptorFactory<StandardCommandTransaction>>,
		catalog: MaterializedCatalog,
		custom_functions: Option<Functions>,
	) -> Self {
		let functions = custom_functions.unwrap_or_else(|| {
			Functions::builder()
				.register_aggregate("math::sum", math::aggregate::Sum::new)
				.register_aggregate("math::min", math::aggregate::Min::new)
				.register_aggregate("math::max", math::aggregate::Max::new)
				.register_aggregate("math::avg", math::aggregate::Avg::new)
				.register_aggregate("math::count", math::aggregate::Count::new)
				.register_scalar("math::abs", math::scalar::Abs::new)
				.register_scalar("math::avg", math::scalar::Avg::new)
				.register_generator("generate_series", generator::GenerateSeries::new)
				.build()
		});

		// Create the flow operator store and register the event listener
		let flow_operator_store = FlowOperatorStore::new();
		let listener = FlowOperatorEventListener::new(flow_operator_store.clone());
		event_bus.register(listener);

		Self(Arc::new(EngineInner {
			multi,
			single,
			cdc,
			event_bus,
			executor: Executor::new(functions, flow_operator_store.clone()),
			interceptors,
			catalog,
			flow_operator_store,
		}))
	}

	#[inline]
	pub fn multi(&self) -> &TransactionMultiVersion {
		&self.multi
	}

	#[inline]
	pub fn multi_owned(&self) -> TransactionMultiVersion {
		self.multi.clone()
	}

	#[inline]
	pub fn single(&self) -> &TransactionSingleVersion {
		&self.single
	}

	#[inline]
	pub fn single_owned(&self) -> TransactionSingleVersion {
		self.single.clone()
	}

	#[inline]
	pub fn cdc(&self) -> &TransactionCdc {
		&self.cdc
	}

	#[inline]
	pub fn cdc_owned(&self) -> TransactionCdc {
		self.cdc.clone()
	}

	#[inline]
	pub fn emit<E: Event>(&self, event: E) {
		self.event_bus.emit(event)
	}

	#[inline]
	pub fn catalog(&self) -> &MaterializedCatalog {
		&self.catalog
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

	/// Wait for the watermark to reach the specified version.
	/// Returns Ok(()) if the watermark reaches the version within the timeout,
	/// or Err(AwaitWatermarkError) if the timeout expires.
	///
	/// This is useful for CDC polling to ensure all in-flight commits have
	/// completed their storage writes before querying for CDC events.
	#[inline]
	pub fn try_wait_for_watermark(
		&self,
		version: CommitVersion,
		timeout: Duration,
	) -> Result<(), AwaitWatermarkError> {
		self.multi.try_wait_for_watermark(version, timeout)
	}

	/// Returns the highest version where ALL prior versions have completed.
	/// This is useful for CDC polling to know the safe upper bound for fetching
	/// CDC events - all events up to this version are guaranteed to be in storage.
	#[inline]
	pub fn done_until(&self) -> CommitVersion {
		self.multi.done_until()
	}

	/// Returns (query_done_until, command_done_until) for debugging watermark state.
	#[inline]
	pub fn watermarks(&self) -> (CommitVersion, CommitVersion) {
		self.multi.watermarks()
	}

	#[inline]
	pub fn executor(&self) -> Executor {
		self.executor.clone()
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
	/// The assigned `TableVirtualId` on success.
	///
	/// # Example
	///
	/// ```ignore
	/// use reifydb_engine::table_virtual::{TableVirtualUser, TableVirtualUserColumnDef};
	/// use reifydb_type::Type;
	/// use reifydb_core::value::Value;
	///
	/// #[derive(Clone)]
	/// struct MyTable;
	///
	/// impl TableVirtualUser for MyTable {
	///     fn columns(&self) -> Vec<TableVirtualUserColumnDef> {
	///         vec![TableVirtualUserColumnDef::new("id", Type::Uint8)]
	///     }
	///     fn rows(&self) -> Vec<Vec<Value>> {
	///         vec![vec![Value::Uint8(1)], vec![Value::Uint8(2)]]
	///     }
	/// }
	///
	/// let id = engine.register_virtual_table("default", "my_table", MyTable)?;
	/// ```
	pub fn register_virtual_table<T: TableVirtualUser + Clone>(
		&self,
		namespace: &str,
		name: &str,
		table: T,
	) -> crate::Result<TableVirtualId> {
		// Look up namespace by name (use max u64 to get latest version)
		let ns_def =
			self.catalog.find_namespace_by_name(namespace, CommitVersion(u64::MAX)).ok_or_else(|| {
				reifydb_type::Error(reifydb_type::diagnostic::catalog::namespace_not_found(
					OwnedFragment::None,
					namespace,
				))
			})?;

		// Allocate a new table ID
		let table_id = self.executor.virtual_table_registry.allocate_id();

		// Convert user columns to internal column definitions
		let table_columns = table.columns();
		let columns = convert_table_virtual_user_columns_to_column_defs(&table_columns);

		// Create the table definition
		let def = Arc::new(TableVirtualDef {
			id: table_id,
			namespace: ns_def.id,
			name: name.to_string(),
			columns,
		});

		// Register in catalog (for resolver lookups)
		self.catalog.register_table_virtual_user(def.clone())?;

		// Create and register the factory (for runtime instantiation)
		let factory = Arc::new(SimpleVirtualTableFactory::new(table, def.clone()));
		self.executor.virtual_table_registry.register(ns_def.id, name.to_string(), factory);

		Ok(table_id)
	}

	/// Unregister a user-defined virtual table.
	///
	/// # Arguments
	///
	/// * `namespace` - The namespace name
	/// * `name` - The table name
	pub fn unregister_virtual_table(&self, namespace: &str, name: &str) -> crate::Result<()> {
		// Look up namespace by name (use max u64 to get latest version)
		let ns_def =
			self.catalog.find_namespace_by_name(namespace, CommitVersion(u64::MAX)).ok_or_else(|| {
				reifydb_type::Error(reifydb_type::diagnostic::catalog::namespace_not_found(
					OwnedFragment::None,
					namespace,
				))
			})?;

		// Unregister from catalog
		self.catalog.unregister_table_virtual_user(ns_def.id, name)?;

		// Unregister from executor registry
		self.executor.virtual_table_registry.unregister(ns_def.id, name);

		Ok(())
	}

	/// Register a user-defined virtual table using an iterator-based implementation.
	///
	/// This method is for tables that stream data in batches, which is more efficient
	/// for large datasets. The creator function is called once per query to create
	/// a fresh iterator instance.
	///
	/// # Arguments
	///
	/// * `namespace` - The namespace to register the table in
	/// * `name` - The table name
	/// * `creator` - A function that creates a new iterator instance for each query
	///
	/// # Returns
	///
	/// The ID of the registered virtual table
	pub fn register_virtual_table_iterator<F>(
		&self,
		namespace: &str,
		name: &str,
		creator: F,
	) -> crate::Result<TableVirtualId>
	where
		F: Fn() -> Box<dyn TableVirtualUserIterator> + Send + Sync + 'static,
	{
		// Look up namespace by name (use max u64 to get latest version)
		let ns_def =
			self.catalog.find_namespace_by_name(namespace, CommitVersion(u64::MAX)).ok_or_else(|| {
				reifydb_type::Error(reifydb_type::diagnostic::catalog::namespace_not_found(
					OwnedFragment::None,
					namespace,
				))
			})?;

		// Allocate a new table ID
		let table_id = self.executor.virtual_table_registry.allocate_id();

		// Get columns from a temporary instance
		let temp_iter = creator();
		let table_columns = temp_iter.columns();
		let columns = convert_table_virtual_user_columns_to_column_defs(&table_columns);

		// Create the table definition
		let def = Arc::new(TableVirtualDef {
			id: table_id,
			namespace: ns_def.id,
			name: name.to_string(),
			columns,
		});

		// Register in catalog (for resolver lookups)
		self.catalog.register_table_virtual_user(def.clone())?;

		// Create and register the factory (for runtime instantiation)
		let factory = Arc::new(IteratorVirtualTableFactory::new(creator, def.clone()));
		self.executor.virtual_table_registry.register(ns_def.id, name.to_string(), factory);

		Ok(table_id)
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
	) -> crate::bulk_insert::BulkInsertBuilder<'e, crate::bulk_insert::Validated> {
		crate::bulk_insert::BulkInsertBuilder::new(self, identity)
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
	) -> crate::bulk_insert::BulkInsertBuilder<'e, crate::bulk_insert::Trusted> {
		crate::bulk_insert::BulkInsertBuilder::new_trusted(self, identity)
	}
}

/// Convert user column definitions to internal ColumnDef format.
fn convert_table_virtual_user_columns_to_column_defs(columns: &[TableVirtualUserColumnDef]) -> Vec<ColumnDef> {
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
