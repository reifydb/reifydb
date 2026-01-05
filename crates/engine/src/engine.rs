// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;
use reifydb_catalog::{
	MaterializedCatalog,
	vtable::{
		UserVTable, UserVTableColumnDef, UserVTableDataFunction, UserVTableEntry,
		system::{FlowOperatorEventListener, FlowOperatorStore},
	},
};
use reifydb_core::{
	CommitVersion, Frame,
	event::{Event, EventBus},
	interface::{ColumnDef, ColumnId, ColumnIndex, Identity, Params, VTableDef, VTableId, WithEventBus},
	ioc::IocContainer,
	stream::{ChannelFrameStream, FrameSender, SendableFrameStream, StreamError},
};
use reifydb_function::{Functions, math, series, subscription};
use reifydb_rql::ast;
use reifydb_transaction::{
	StandardCommandTransaction, StandardQueryTransaction, cdc::TransactionCdc, interceptor::InterceptorFactory,
	multi::TransactionMultiVersion, single::TransactionSingle,
};
use reifydb_type::{
	Error, Fragment, TypeConstraint,
	diagnostic::{catalog::namespace_not_found, engine::parallel_execution_error},
};
use tokio::spawn;
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{
	execute::{Command, ExecuteCommand, ExecuteQuery, Executor, Query, parallel::can_parallelize},
	interceptor::{CatalogEventInterceptor, materialized_catalog::MaterializedCatalogInterceptor},
};

pub struct StandardEngine(Arc<EngineInner>);

impl WithEventBus for StandardEngine {
	fn event_bus(&self) -> &EventBus {
		&self.event_bus
	}
}

// Engine methods (formerly from Engine trait in reifydb-core)
impl StandardEngine {
	#[instrument(name = "engine::transaction::begin_command", level = "debug", skip(self))]
	pub async fn begin_command(&self) -> crate::Result<StandardCommandTransaction> {
		let mut interceptors = self.interceptors.create();

		interceptors.post_commit.add(Arc::new(MaterializedCatalogInterceptor::new(self.catalog.clone())));
		interceptors
			.post_commit
			.add(Arc::new(CatalogEventInterceptor::new(self.event_bus.clone(), self.catalog.clone())));

		StandardCommandTransaction::new(
			self.multi.clone(),
			self.single.clone(),
			self.cdc.clone(),
			self.event_bus.clone(),
			interceptors,
		)
		.await
	}

	#[instrument(name = "engine::transaction::begin_query", level = "debug", skip(self))]
	pub async fn begin_query(&self) -> crate::Result<StandardQueryTransaction> {
		Ok(StandardQueryTransaction::new(
			self.multi.begin_query().await?,
			self.single.clone(),
			self.cdc.clone(),
		))
	}

	#[instrument(name = "engine::command", level = "info", skip(self, params), fields(rql = %rql))]
	pub fn command_as(&self, identity: &Identity, rql: &str, params: Params) -> SendableFrameStream {
		let engine = self.clone();
		let identity = identity.clone();
		let rql = rql.to_string();
		let cancel_token = CancellationToken::new();

		let (sender, stream) = ChannelFrameStream::new(8, cancel_token.clone());

		spawn(execute_command(engine, identity, rql, params, sender, cancel_token));

		Box::pin(stream)
	}

	#[instrument(name = "engine::query", level = "info", skip(self, params), fields(rql = %rql))]
	pub fn query_as(&self, identity: &Identity, rql: &str, params: Params) -> SendableFrameStream {
		let engine = self.clone();
		let identity = identity.clone();
		let rql = rql.to_string();
		let cancel_token = CancellationToken::new();

		let (sender, stream) = ChannelFrameStream::new(8, cancel_token.clone());

		spawn(execute_query(engine, identity, rql, params, sender, cancel_token));

		Box::pin(stream)
	}
}

/// Execute a command and send results to the stream.
async fn execute_command(
	engine: StandardEngine,
	identity: Identity,
	rql: String,
	params: Params,
	sender: FrameSender,
	cancel_token: CancellationToken,
) {
	// Check for cancellation before starting
	if cancel_token.is_cancelled() {
		return;
	}

	// Begin transaction
	let txn_result = engine.begin_command().await;
	let mut txn = match txn_result {
		Ok(txn) => txn,
		Err(e) => {
			let _ = sender.try_send(Err(StreamError::query_with_statement(e, rql.clone())));
			return;
		}
	};

	// Execute command - call executor directly to avoid trait object indirection
	let result = engine
		.executor
		.execute_command(
			&mut txn,
			Command {
				rql: &rql,
				params,
				identity: &identity,
			},
		)
		.await;

	match result {
		Ok(frames) => {
			// Commit transaction
			if let Err(e) = txn.commit().await {
				let _ = sender.try_send(Err(StreamError::query_with_statement(e, rql)));
				return;
			}

			// Send each frame through the channel
			for frame in frames {
				if cancel_token.is_cancelled() {
					return;
				}
				if sender.send(Ok(frame)).await.is_err() {
					return; // Receiver dropped
				}
			}
		}
		Err(e) => {
			// Rollback on error (drop will handle it)
			let _ = sender.try_send(Err(StreamError::query_with_statement(e, rql)));
		}
	}
}

/// Execute a query and send results to the stream.
async fn execute_query(
	engine: StandardEngine,
	identity: Identity,
	rql: String,
	params: Params,
	sender: FrameSender,
	cancel_token: CancellationToken,
) {
	// Check for cancellation before starting
	if cancel_token.is_cancelled() {
		return;
	}

	// Parse the RQL to check for parallel execution opportunity
	let statements = match ast::parse_str(&rql) {
		Ok(stmts) => stmts,
		Err(e) => {
			let _ = sender.try_send(Err(StreamError::query_with_statement(e.into(), rql)));
			return;
		}
	};

	// Check if we can execute statements in parallel
	if can_parallelize(&statements) {
		execute_query_parallel(engine, rql, statements, params, sender, cancel_token).await;
	} else {
		execute_query_sequential(engine, identity, rql, params, sender, cancel_token).await;
	}
}

/// Execute a query sequentially (original behavior).
async fn execute_query_sequential(
	engine: StandardEngine,
	identity: Identity,
	rql: String,
	params: Params,
	sender: FrameSender,
	cancel_token: CancellationToken,
) {
	// Begin transaction
	let txn_result = engine.begin_query().await;
	let mut txn = match txn_result {
		Ok(txn) => txn,
		Err(e) => {
			let _ = sender.try_send(Err(StreamError::query_with_statement(e, rql.clone())));
			return;
		}
	};

	// Execute query - call executor directly to avoid trait object indirection
	let result = engine
		.executor
		.execute_query(
			&mut txn,
			Query {
				rql: &rql,
				params,
				identity: &identity,
			},
		)
		.await;

	match result {
		Ok(frames) => {
			// Send each frame through the channel
			for frame in frames {
				if cancel_token.is_cancelled() {
					return;
				}
				if sender.send(Ok(frame)).await.is_err() {
					return; // Receiver dropped
				}
			}
		}
		Err(e) => {
			let _ = sender.try_send(Err(StreamError::query_with_statement(e, rql)));
		}
	}
}

/// Execute independent statements in parallel.
///
/// Each statement gets its own transaction at the same snapshot version,
/// ensuring consistent reads across all parallel executions.
#[allow(dead_code)]
async fn execute_query_parallel(
	engine: StandardEngine,
	rql: String,
	statements: Vec<ast::AstStatement>,
	params: Params,
	sender: FrameSender,
	cancel_token: CancellationToken,
) {
	// Begin initial transaction to get the snapshot version
	let initial_txn = match engine.begin_query().await {
		Ok(txn) => txn,
		Err(e) => {
			let _ = sender.try_send(Err(StreamError::query_with_statement(e, rql)));
			return;
		}
	};

	// Capture the snapshot version - all parallel tasks will use this same version
	let version = initial_txn.version();
	drop(initial_txn);

	let statement_count = statements.len();

	// Spawn parallel tasks for each statement
	let handles: Vec<_> = statements
		.into_iter()
		.enumerate()
		.map(|(idx, statement)| {
			let engine = engine.clone();
			let params = params.clone();

			spawn(async move {
				// Create a new transaction at the same snapshot version
				let mut txn = engine.begin_query_at_version(version).await?;

				// Execute the single statement
				let frame =
					engine.executor.execute_single_statement(&mut txn, statement, params).await?;

				Ok::<_, Error>((idx, frame))
			})
		})
		.collect();

	// Collect results, preserving order
	let mut results: Vec<Option<Frame>> = vec![None; statement_count];
	let mut first_error: Option<Error> = None;

	for handle in handles {
		match handle.await {
			Ok(Ok((idx, frame))) => {
				results[idx] = frame;
			}
			Ok(Err(e)) => {
				// Track the first error
				if first_error.is_none() {
					first_error = Some(e);
				}
			}
			Err(join_err) => {
				// Task panicked
				if first_error.is_none() {
					first_error = Some(Error(parallel_execution_error(format!(
						"Task panicked: {}",
						join_err
					))));
				}
			}
		}
	}

	// If there was an error, report it
	if let Some(error) = first_error {
		let _ = sender.try_send(Err(StreamError::query_with_statement(error, rql)));
		return;
	}

	// Send results in order
	for frame in results.into_iter().flatten() {
		if cancel_token.is_cancelled() {
			return;
		}
		if sender.send(Ok(frame)).await.is_err() {
			return; // Receiver dropped
		}
	}
}

#[async_trait]
impl ExecuteCommand for StandardEngine {
	#[inline]
	async fn execute_command(
		&self,
		txn: &mut StandardCommandTransaction,
		cmd: Command<'_>,
	) -> crate::Result<Vec<Frame>> {
		self.executor.execute_command(txn, cmd).await
	}
}

#[async_trait]
impl ExecuteQuery for StandardEngine {
	#[inline]
	async fn execute_query(&self, txn: &mut StandardQueryTransaction, qry: Query<'_>) -> crate::Result<Vec<Frame>> {
		self.executor.execute_query(txn, qry).await
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
	single: TransactionSingle,
	cdc: TransactionCdc,
	event_bus: EventBus,
	executor: Executor,
	interceptors: Box<dyn InterceptorFactory>,
	catalog: MaterializedCatalog,
	flow_operator_store: FlowOperatorStore,
}

impl StandardEngine {
	pub async fn new(
		multi: TransactionMultiVersion,
		single: TransactionSingle,
		cdc: TransactionCdc,
		event_bus: EventBus,
		interceptors: Box<dyn InterceptorFactory>,
		catalog: MaterializedCatalog,
		custom_functions: Option<Functions>,
		ioc: IocContainer,
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
				.register_generator("generate_series", series::GenerateSeries::new)
				.register_generator("inspect_subscription", subscription::InspectSubscription::new)
				.build()
		});

		// Create the flow operator store and register the event listener
		let flow_operator_store = FlowOperatorStore::new();
		let listener = FlowOperatorEventListener::new(flow_operator_store.clone());
		event_bus.register(listener).await;

		let stats_tracker = multi.store().stats_tracker().clone();

		let catalog_wrapper = reifydb_catalog::Catalog::new(catalog.clone());

		Self(Arc::new(EngineInner {
			multi,
			single,
			cdc,
			event_bus,
			executor: Executor::new(
				catalog_wrapper,
				functions,
				flow_operator_store.clone(),
				stats_tracker,
				ioc,
			),
			interceptors,
			catalog,
			flow_operator_store,
		}))
	}

	/// Begin a query transaction at a specific version.
	///
	/// This is used for parallel query execution where multiple tasks need to
	/// read from the same snapshot (same CommitVersion) for consistency.
	#[instrument(name = "engine::transaction::begin_query_at_version", level = "debug", skip(self), fields(version = %version.0))]
	pub async fn begin_query_at_version(&self, version: CommitVersion) -> crate::Result<StandardQueryTransaction> {
		Ok(StandardQueryTransaction::new(
			self.multi.begin_query_at_version(version).await?,
			self.single.clone(),
			self.cdc.clone(),
		))
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
	pub fn single(&self) -> &TransactionSingle {
		&self.single
	}

	#[inline]
	pub fn single_owned(&self) -> TransactionSingle {
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
	pub async fn emit<E: Event>(&self, event: E) {
		self.event_bus.emit(event).await
	}

	#[inline]
	pub fn materialized_catalog(&self) -> &MaterializedCatalog {
		&self.catalog
	}

	/// Returns a `Catalog` instance for catalog lookups.
	/// The Catalog provides three-tier lookup methods that check transactional changes,
	/// then MaterializedCatalog, then fall back to storage.
	#[inline]
	pub fn catalog(&self) -> reifydb_catalog::Catalog {
		reifydb_catalog::Catalog::new(self.catalog.clone())
	}

	#[inline]
	pub fn flow_operator_store(&self) -> &FlowOperatorStore {
		&self.flow_operator_store
	}

	/// Get the current version from the transaction manager
	#[inline]
	pub async fn current_version(&self) -> crate::Result<CommitVersion> {
		self.multi.current_version().await
	}

	/// Returns the highest version where ALL prior versions have completed.
	/// This is useful for CDC polling to know the safe upper bound for fetching
	/// CDC events - all events up to this version are guaranteed to be in storage.
	#[inline]
	pub fn done_until(&self) -> CommitVersion {
		self.multi.done_until()
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
	/// The assigned `VTableId` on success.
	///
	/// # Example
	///
	/// ```ignore
	/// use reifydb_engine::vtable::{UserVTable, UserVTableColumnDef};
	/// use reifydb_type::Type;
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
	pub fn register_virtual_table<T: UserVTable + Clone + 'static>(
		&self,
		namespace: &str,
		name: &str,
		table: T,
	) -> crate::Result<VTableId> {
		// Look up namespace by name (use max u64 to get latest version)
		let ns_def = self
			.catalog
			.find_namespace_by_name(namespace, CommitVersion(u64::MAX))
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
		self.catalog.register_vtable_user(def.clone())?;

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

	/// Unregister a user-defined virtual table.
	///
	/// # Arguments
	///
	/// * `namespace` - The namespace name
	/// * `name` - The table name
	pub fn unregister_virtual_table(&self, namespace: &str, name: &str) -> crate::Result<()> {
		// Look up namespace by name (use max u64 to get latest version)
		let ns_def = self
			.catalog
			.find_namespace_by_name(namespace, CommitVersion(u64::MAX))
			.ok_or_else(|| Error(namespace_not_found(Fragment::None, namespace)))?;

		// Unregister from catalog
		self.catalog.unregister_vtable_user(ns_def.id, name)?;

		// Unregister from executor registry
		self.executor.virtual_table_registry.unregister(ns_def.id, name);

		Ok(())
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
