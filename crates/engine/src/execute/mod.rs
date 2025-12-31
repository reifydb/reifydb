// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use async_trait::async_trait;
use query::{
	aggregate::AggregateNode,
	assign::AssignNode,
	compile::compile,
	declare::DeclareNode,
	dictionary_scan::DictionaryScanNode,
	environment::EnvironmentNode,
	extend::{ExtendNode, ExtendWithoutInputNode},
	filter::FilterNode,
	generator::GeneratorNode,
	index_scan::IndexScanNode,
	inline::InlineDataNode,
	join::{InnerJoinNode, LeftJoinNode, NaturalJoinNode},
	map::{MapNode, MapWithoutInputNode},
	ringbuffer_scan::RingBufferScan,
	row_lookup::{RowListLookupNode, RowPointLookupNode, RowRangeScanNode},
	scalarize::ScalarizeNode,
	sort::SortNode,
	table_scan::TableScanNode,
	take::TakeNode,
	top_k::TopKNode,
	variable::VariableNode,
	view_scan::ViewScanNode,
	vtable_scan::VirtualScanNode,
};
use reifydb_builtin::{Functions, generator, math};
use reifydb_catalog::{
	Catalog,
	vtable::{UserVTableRegistry, system::FlowOperatorStore},
};
use reifydb_core::{
	Frame, LazyBatch,
	interface::{Identity, Params, ResolvedPrimitive},
	ioc::IocContainer,
	value::column::{Column, ColumnData, Columns, headers::ColumnHeaders},
};

// Types moved from reifydb-core (formerly in interface/execute.rs)

/// A batch of columnar data returned from query execution
#[derive(Debug)]
pub struct Batch {
	pub columns: Columns,
}

/// Command execution request
#[derive(Debug)]
pub struct Command<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: &'a Identity,
}

/// Query execution request
#[derive(Debug)]
pub struct Query<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: &'a Identity,
}

/// Trait for executing commands (write operations)
#[async_trait]
pub trait ExecuteCommand {
	async fn execute_command(
		&self,
		txn: &mut StandardCommandTransaction,
		cmd: Command<'_>,
	) -> crate::Result<Vec<Frame>>;
}

/// Trait for executing queries (read operations)
#[async_trait]
pub trait ExecuteQuery {
	async fn execute_query(&self, txn: &mut StandardQueryTransaction, qry: Query<'_>) -> crate::Result<Vec<Frame>>;
}
use reifydb_rql::{
	ast,
	ast::AstStatement,
	plan::{physical::PhysicalPlan, plan},
};
use reifydb_transaction::StorageTracker;
use tracing::instrument;

use crate::{
	StandardCommandTransaction, StandardQueryTransaction, StandardTransaction,
	stack::{Stack, Variable},
};

mod catalog;
pub(crate) mod mutate;
pub(crate) mod parallel;
mod query;

/// Unified trait for query execution nodes following the volcano iterator pattern
#[async_trait]
pub(crate) trait QueryNode {
	/// Initialize the operator with execution context
	/// Called once before iteration begins
	async fn initialize<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &ExecutionContext,
	) -> crate::Result<()>;

	/// Get the next batch of results (volcano iterator pattern)
	/// Returns None when exhausted
	async fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>>;

	/// Get the next batch as a LazyBatch for deferred materialization
	/// Returns None if this node doesn't support lazy evaluation or is exhausted
	/// Default implementation returns None (falls back to materialized evaluation)
	async fn next_lazy<'a>(
		&mut self,
		_rx: &mut StandardTransaction<'a>,
		_ctx: &mut ExecutionContext,
	) -> crate::Result<Option<LazyBatch>> {
		Ok(None)
	}

	/// Get the headers of columns this node produces
	fn headers(&self) -> Option<ColumnHeaders>;
}

#[derive(Clone)]
pub struct ExecutionContext {
	pub executor: Executor,
	pub source: Option<ResolvedPrimitive>,
	pub batch_size: u64,
	pub params: Params,
	pub stack: Stack,
}

pub(crate) enum ExecutionPlan {
	Aggregate(AggregateNode),
	DictionaryScan(DictionaryScanNode),
	Filter(FilterNode),
	IndexScan(IndexScanNode),
	InlineData(InlineDataNode),
	InnerJoin(InnerJoinNode),
	LeftJoin(LeftJoinNode),
	NaturalJoin(NaturalJoinNode),
	Map(MapNode),
	MapWithoutInput(MapWithoutInputNode),
	Extend(ExtendNode),
	ExtendWithoutInput(ExtendWithoutInputNode),
	Sort(SortNode),
	TableScan(TableScanNode),
	Take(TakeNode),
	TopK(TopKNode),
	ViewScan(ViewScanNode),
	Variable(VariableNode),
	Environment(EnvironmentNode),
	VirtualScan(VirtualScanNode),
	RingBufferScan(RingBufferScan),
	Generator(GeneratorNode),
	Declare(DeclareNode),
	Assign(AssignNode),
	Conditional(query::conditional::ConditionalNode),
	Scalarize(ScalarizeNode),
	// Row-number optimized access
	RowPointLookup(RowPointLookupNode),
	RowListLookup(RowListLookupNode),
	RowRangeScan(RowRangeScanNode),
}

// Implement QueryNode for Box<ExecutionPlan> to allow chaining
#[async_trait]
impl QueryNode for Box<ExecutionPlan> {
	async fn initialize<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		(**self).initialize(rx, ctx).await
	}

	async fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		(**self).next(rx, ctx).await
	}

	async fn next_lazy<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<LazyBatch>> {
		(**self).next_lazy(rx, ctx).await
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		(**self).headers()
	}
}

#[async_trait]
impl QueryNode for ExecutionPlan {
	async fn initialize<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &ExecutionContext,
	) -> crate::Result<()> {
		match self {
			ExecutionPlan::Aggregate(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::DictionaryScan(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Filter(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::IndexScan(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::InlineData(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::InnerJoin(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::LeftJoin(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::NaturalJoin(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Map(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::MapWithoutInput(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Extend(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::ExtendWithoutInput(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Sort(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::TableScan(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Take(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::TopK(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::ViewScan(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Variable(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Environment(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::VirtualScan(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::RingBufferScan(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Generator(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Declare(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Assign(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Conditional(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::Scalarize(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::RowPointLookup(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::RowListLookup(node) => node.initialize(rx, ctx).await,
			ExecutionPlan::RowRangeScan(node) => node.initialize(rx, ctx).await,
		}
	}

	async fn next<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<Batch>> {
		match self {
			ExecutionPlan::Aggregate(node) => node.next(rx, ctx).await,
			ExecutionPlan::DictionaryScan(node) => node.next(rx, ctx).await,
			ExecutionPlan::Filter(node) => node.next(rx, ctx).await,
			ExecutionPlan::IndexScan(node) => node.next(rx, ctx).await,
			ExecutionPlan::InlineData(node) => node.next(rx, ctx).await,
			ExecutionPlan::InnerJoin(node) => node.next(rx, ctx).await,
			ExecutionPlan::LeftJoin(node) => node.next(rx, ctx).await,
			ExecutionPlan::NaturalJoin(node) => node.next(rx, ctx).await,
			ExecutionPlan::Map(node) => node.next(rx, ctx).await,
			ExecutionPlan::MapWithoutInput(node) => node.next(rx, ctx).await,
			ExecutionPlan::Extend(node) => node.next(rx, ctx).await,
			ExecutionPlan::ExtendWithoutInput(node) => node.next(rx, ctx).await,
			ExecutionPlan::Sort(node) => node.next(rx, ctx).await,
			ExecutionPlan::TableScan(node) => node.next(rx, ctx).await,
			ExecutionPlan::Take(node) => node.next(rx, ctx).await,
			ExecutionPlan::TopK(node) => node.next(rx, ctx).await,
			ExecutionPlan::ViewScan(node) => node.next(rx, ctx).await,
			ExecutionPlan::Variable(node) => node.next(rx, ctx).await,
			ExecutionPlan::Environment(node) => node.next(rx, ctx).await,
			ExecutionPlan::VirtualScan(node) => node.next(rx, ctx).await,
			ExecutionPlan::RingBufferScan(node) => node.next(rx, ctx).await,
			ExecutionPlan::Generator(node) => node.next(rx, ctx).await,
			ExecutionPlan::Declare(node) => node.next(rx, ctx).await,
			ExecutionPlan::Assign(node) => node.next(rx, ctx).await,
			ExecutionPlan::Conditional(node) => node.next(rx, ctx).await,
			ExecutionPlan::Scalarize(node) => node.next(rx, ctx).await,
			ExecutionPlan::RowPointLookup(node) => node.next(rx, ctx).await,
			ExecutionPlan::RowListLookup(node) => node.next(rx, ctx).await,
			ExecutionPlan::RowRangeScan(node) => node.next(rx, ctx).await,
		}
	}

	async fn next_lazy<'a>(
		&mut self,
		rx: &mut StandardTransaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<LazyBatch>> {
		match self {
			// Only TableScan supports lazy evaluation for now
			ExecutionPlan::TableScan(node) => node.next_lazy(rx, ctx).await,
			// All other nodes return None (use default materialized path)
			_ => Ok(None),
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		match self {
			ExecutionPlan::Aggregate(node) => node.headers(),
			ExecutionPlan::DictionaryScan(node) => node.headers(),
			ExecutionPlan::Filter(node) => node.headers(),
			ExecutionPlan::IndexScan(node) => node.headers(),
			ExecutionPlan::InlineData(node) => node.headers(),
			ExecutionPlan::InnerJoin(node) => node.headers(),
			ExecutionPlan::LeftJoin(node) => node.headers(),
			ExecutionPlan::NaturalJoin(node) => node.headers(),
			ExecutionPlan::Map(node) => node.headers(),
			ExecutionPlan::MapWithoutInput(node) => node.headers(),
			ExecutionPlan::Extend(node) => node.headers(),
			ExecutionPlan::ExtendWithoutInput(node) => node.headers(),
			ExecutionPlan::Sort(node) => node.headers(),
			ExecutionPlan::TableScan(node) => node.headers(),
			ExecutionPlan::Take(node) => node.headers(),
			ExecutionPlan::TopK(node) => node.headers(),
			ExecutionPlan::ViewScan(node) => node.headers(),
			ExecutionPlan::Variable(node) => node.headers(),
			ExecutionPlan::Environment(node) => node.headers(),
			ExecutionPlan::VirtualScan(node) => node.headers(),
			ExecutionPlan::RingBufferScan(node) => node.headers(),
			ExecutionPlan::Generator(node) => node.headers(),
			ExecutionPlan::Declare(node) => node.headers(),
			ExecutionPlan::Assign(node) => node.headers(),
			ExecutionPlan::Conditional(node) => node.headers(),
			ExecutionPlan::Scalarize(node) => node.headers(),
			ExecutionPlan::RowPointLookup(node) => node.headers(),
			ExecutionPlan::RowListLookup(node) => node.headers(),
			ExecutionPlan::RowRangeScan(node) => node.headers(),
		}
	}
}

pub struct Executor(Arc<ExecutorInner>);

pub struct ExecutorInner {
	pub catalog: Catalog,
	pub functions: Functions,
	pub flow_operator_store: FlowOperatorStore,
	pub virtual_table_registry: UserVTableRegistry,
	pub stats_tracker: StorageTracker,
	pub ioc: IocContainer,
}

impl Clone for Executor {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl std::ops::Deref for Executor {
	type Target = ExecutorInner;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Executor {
	pub fn new(
		catalog: Catalog,
		functions: Functions,
		flow_operator_store: FlowOperatorStore,
		stats_tracker: StorageTracker,
		ioc: IocContainer,
	) -> Self {
		Self(Arc::new(ExecutorInner {
			catalog,
			functions,
			flow_operator_store,
			virtual_table_registry: UserVTableRegistry::new(),
			stats_tracker,
			ioc,
		}))
	}

	#[allow(dead_code)]
	pub fn testing() -> Self {
		Self::new(
			Catalog::new(reifydb_catalog::MaterializedCatalog::new()),
			Functions::builder()
				.register_aggregate("math::sum", math::aggregate::Sum::new)
				.register_aggregate("math::min", math::aggregate::Min::new)
				.register_aggregate("math::max", math::aggregate::Max::new)
				.register_aggregate("math::avg", math::aggregate::Avg::new)
				.register_aggregate("math::count", math::aggregate::Count::new)
				.register_scalar("math::abs", math::scalar::Abs::new)
				.register_scalar("math::avg", math::scalar::Avg::new)
				.register_generator("generate_series", generator::GenerateSeries::new)
				.build(),
			FlowOperatorStore::new(),
			StorageTracker::with_defaults(),
			IocContainer::new(),
		)
	}
}

#[async_trait]
impl ExecuteCommand for Executor {
	#[instrument(name = "executor::execute_command", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	async fn execute_command(
		&self,
		txn: &mut StandardCommandTransaction,
		cmd: Command<'_>,
	) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = ast::parse_str(cmd.rql)?;

		// Create a single persistent Stack for all statements in this command
		let mut persistent_stack = Stack::new();

		// Populate the stack with parameters so they can be accessed as variables
		match &cmd.params {
			Params::Positional(values) => {
				// For positional parameters, use $1, $2, $3, etc.
				for (index, value) in values.iter().enumerate() {
					let param_name = (index + 1).to_string(); // 1-based indexing
					persistent_stack.set(param_name, Variable::Scalar(value.clone()), false)?;
				}
			}
			Params::Named(map) => {
				// For named parameters, use the parameter name directly
				for (name, value) in map {
					persistent_stack.set(name.clone(), Variable::Scalar(value.clone()), false)?;
				}
			}
			Params::None => {
				// No parameters to populate
			}
		}

		for statement in statements {
			if let Some(plan) = plan(&self.catalog, txn, statement).await? {
				if let Some(er) = self
					.execute_command_plan(txn, plan, cmd.params.clone(), &mut persistent_stack)
					.await?
				{
					result.push(Frame::from(er));
				}
			}
		}

		Ok(result)
	}
}

#[async_trait]
impl ExecuteQuery for Executor {
	#[instrument(name = "executor::execute_query", level = "debug", skip(self, txn, qry), fields(rql = %qry.rql))]
	async fn execute_query(&self, txn: &mut StandardQueryTransaction, qry: Query<'_>) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = ast::parse_str(qry.rql)?;

		// Create a single persistent Stack for all statements in this query
		let mut persistent_stack = Stack::new();

		// Populate the stack with parameters so they can be accessed as variables
		match &qry.params {
			Params::Positional(values) => {
				// For positional parameters, use $1, $2, $3, etc.
				for (index, value) in values.iter().enumerate() {
					let param_name = (index + 1).to_string(); // 1-based indexing
					persistent_stack.set(param_name, Variable::Scalar(value.clone()), false)?;
				}
			}
			Params::Named(map) => {
				// For named parameters, use the parameter name directly
				for (name, value) in map {
					persistent_stack.set(name.clone(), Variable::Scalar(value.clone()), false)?;
				}
			}
			Params::None => {
				// No parameters to populate
			}
		}

		for statement in statements {
			if let Some(plan) = plan(&self.catalog, txn, statement).await? {
				if let Some(er) = self
					.execute_query_plan(txn, plan, qry.params.clone(), &mut persistent_stack)
					.await?
				{
					result.push(Frame::from(er));
				}
			}
		}

		Ok(result)
	}
}

impl Executor {
	/// Execute a single statement without any scripting context.
	///
	/// This is used for parallel query execution where each statement
	/// runs independently in its own task with its own transaction.
	#[instrument(name = "executor::execute_single_statement", level = "debug", skip(self, txn, statement, params))]
	pub(crate) async fn execute_single_statement(
		&self,
		txn: &mut StandardQueryTransaction,
		statement: AstStatement,
		params: Params,
	) -> crate::Result<Option<Frame>> {
		// Create an empty stack (no persistent variables for parallel execution)
		let mut stack = Stack::new();

		// Populate the stack with parameters
		match &params {
			Params::Positional(values) => {
				for (index, value) in values.iter().enumerate() {
					let param_name = (index + 1).to_string();
					stack.set(param_name, Variable::Scalar(value.clone()), false)?;
				}
			}
			Params::Named(map) => {
				for (name, value) in map {
					stack.set(name.clone(), Variable::Scalar(value.clone()), false)?;
				}
			}
			Params::None => {}
		}

		// Plan and execute
		if let Some(plan) = plan(&self.catalog, txn, statement).await? {
			if let Some(columns) = self.execute_query_plan(txn, plan, params, &mut stack).await? {
				return Ok(Some(Frame::from(columns)));
			}
		}

		Ok(None)
	}
}

impl Executor {
	#[instrument(name = "executor::plan::query", level = "debug", skip(self, rx, plan, params, stack))]
	pub(crate) async fn execute_query_plan<'a>(
		&self,
		rx: &'a mut StandardQueryTransaction,
		plan: PhysicalPlan,
		params: Params,
		stack: &mut Stack,
	) -> crate::Result<Option<Columns>> {
		match plan {
			// Query
			PhysicalPlan::Aggregate(_)
			| PhysicalPlan::DictionaryScan(_)
			| PhysicalPlan::Filter(_)
			| PhysicalPlan::IndexScan(_)
			| PhysicalPlan::JoinInner(_)
			| PhysicalPlan::JoinLeft(_)
			| PhysicalPlan::JoinNatural(_)
			| PhysicalPlan::Take(_)
			| PhysicalPlan::Sort(_)
			| PhysicalPlan::Map(_)
			| PhysicalPlan::Extend(_)
			| PhysicalPlan::InlineData(_)
			| PhysicalPlan::Generator(_)
			| PhysicalPlan::Delete(_)
			| PhysicalPlan::DeleteRingBuffer(_)
			| PhysicalPlan::InsertTable(_)
			| PhysicalPlan::InsertRingBuffer(_)
			| PhysicalPlan::InsertDictionary(_)
			| PhysicalPlan::Update(_)
			| PhysicalPlan::UpdateRingBuffer(_)
			| PhysicalPlan::TableScan(_)
			| PhysicalPlan::ViewScan(_)
			| PhysicalPlan::FlowScan(_)
			| PhysicalPlan::TableVirtualScan(_)
			| PhysicalPlan::RingBufferScan(_)
			| PhysicalPlan::Variable(_)
			| PhysicalPlan::Environment(_)
			| PhysicalPlan::Conditional(_)
			| PhysicalPlan::Scalarize(_)
			| PhysicalPlan::RowPointLookup(_)
			| PhysicalPlan::RowListLookup(_)
			| PhysicalPlan::RowRangeScan(_) => {
				let mut std_txn = StandardTransaction::from(rx);
				self.query(&mut std_txn, plan, params, stack).await
			}
			PhysicalPlan::Declare(_) | PhysicalPlan::Assign(_) => {
				let mut std_txn = StandardTransaction::from(rx);
				self.query(&mut std_txn, plan, params, stack).await?;
				Ok(None)
			}
			PhysicalPlan::AlterSequence(_)
			| PhysicalPlan::AlterTable(_)
			| PhysicalPlan::AlterView(_)
			| PhysicalPlan::AlterFlow(_)
			| PhysicalPlan::CreateDeferredView(_)
			| PhysicalPlan::CreateTransactionalView(_)
			| PhysicalPlan::CreateNamespace(_)
			| PhysicalPlan::CreateTable(_)
			| PhysicalPlan::CreateRingBuffer(_)
			| PhysicalPlan::CreateFlow(_)
			| PhysicalPlan::CreateDictionary(_)
			| PhysicalPlan::Distinct(_)
			| PhysicalPlan::Apply(_) => {
				// Apply operator requires flow engine for mod
				// execution
				unimplemented!(
					"Apply operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
				)
			}
			PhysicalPlan::Window(_) => {
				// Window operator requires flow engine for mod
				// execution
				unimplemented!(
					"Window operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
				)
			}
			PhysicalPlan::Merge(_) => {
				// Merge operator requires flow engine
				unimplemented!(
					"Merge operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
				)
			}
		}
	}

	#[instrument(name = "executor::plan::command", level = "debug", skip(self, txn, plan, params, stack))]
	pub async fn execute_command_plan<'a>(
		&self,
		txn: &'a mut StandardCommandTransaction,
		plan: PhysicalPlan,
		params: Params,
		stack: &mut Stack,
	) -> crate::Result<Option<Columns>> {
		match plan {
			PhysicalPlan::AlterSequence(plan) => Ok(Some(self.alter_table_sequence(txn, plan).await?)),
			PhysicalPlan::CreateDeferredView(plan) => {
				Ok(Some(self.create_deferred_view(txn, plan).await?))
			}
			PhysicalPlan::CreateTransactionalView(plan) => {
				Ok(Some(self.create_transactional_view(txn, plan).await?))
			}
			PhysicalPlan::CreateNamespace(plan) => Ok(Some(self.create_namespace(txn, plan).await?)),
			PhysicalPlan::CreateTable(plan) => Ok(Some(self.create_table(txn, plan).await?)),
			PhysicalPlan::CreateRingBuffer(plan) => Ok(Some(self.create_ringbuffer(txn, plan).await?)),
			PhysicalPlan::CreateFlow(plan) => Ok(Some(self.create_flow(txn, plan).await?)),
			PhysicalPlan::CreateDictionary(plan) => Ok(Some(self.create_dictionary(txn, plan).await?)),
			PhysicalPlan::Delete(plan) => Ok(Some(self.delete(txn, plan, params).await?)),
			PhysicalPlan::DeleteRingBuffer(plan) => {
				Ok(Some(self.delete_ringbuffer(txn, plan, params).await?))
			}
			PhysicalPlan::InsertTable(plan) => Ok(Some(self.insert_table(txn, plan, stack).await?)),
			PhysicalPlan::InsertRingBuffer(plan) => {
				Ok(Some(self.insert_ringbuffer(txn, plan, params).await?))
			}
			PhysicalPlan::InsertDictionary(plan) => {
				Ok(Some(self.insert_dictionary(txn, plan, stack).await?))
			}
			PhysicalPlan::Update(plan) => Ok(Some(self.update_table(txn, plan, params).await?)),
			PhysicalPlan::UpdateRingBuffer(plan) => {
				Ok(Some(self.update_ringbuffer(txn, plan, params).await?))
			}

			PhysicalPlan::Aggregate(_)
			| PhysicalPlan::DictionaryScan(_)
			| PhysicalPlan::Filter(_)
			| PhysicalPlan::IndexScan(_)
			| PhysicalPlan::JoinInner(_)
			| PhysicalPlan::JoinLeft(_)
			| PhysicalPlan::JoinNatural(_)
			| PhysicalPlan::Take(_)
			| PhysicalPlan::Sort(_)
			| PhysicalPlan::Map(_)
			| PhysicalPlan::Extend(_)
			| PhysicalPlan::InlineData(_)
			| PhysicalPlan::Generator(_)
			| PhysicalPlan::TableScan(_)
			| PhysicalPlan::ViewScan(_)
			| PhysicalPlan::FlowScan(_)
			| PhysicalPlan::TableVirtualScan(_)
			| PhysicalPlan::RingBufferScan(_)
			| PhysicalPlan::Distinct(_)
			| PhysicalPlan::Variable(_)
			| PhysicalPlan::Environment(_)
			| PhysicalPlan::Apply(_)
			| PhysicalPlan::Conditional(_)
			| PhysicalPlan::Scalarize(_)
			| PhysicalPlan::RowPointLookup(_)
			| PhysicalPlan::RowListLookup(_)
			| PhysicalPlan::RowRangeScan(_) => {
				let mut std_txn = StandardTransaction::from(txn);
				self.query(&mut std_txn, plan, params, stack).await
			}
			PhysicalPlan::Declare(_) | PhysicalPlan::Assign(_) => {
				let mut std_txn = StandardTransaction::from(txn);
				self.query(&mut std_txn, plan, params, stack).await?;
				Ok(None)
			}
			PhysicalPlan::Window(_) => {
				let mut std_txn = StandardTransaction::from(txn);
				self.query(&mut std_txn, plan, params, stack).await
			}
			PhysicalPlan::Merge(_) => {
				let mut std_txn = StandardTransaction::from(txn);
				self.query(&mut std_txn, plan, params, stack).await
			}

			PhysicalPlan::AlterTable(plan) => Ok(Some(self.alter_table(txn, plan).await?)),
			PhysicalPlan::AlterView(plan) => Ok(Some(self.execute_alter_view(txn, plan).await?)),
			PhysicalPlan::AlterFlow(plan) => Ok(Some(self.execute_alter_flow(txn, plan).await?)),
		}
	}

	#[instrument(name = "executor::query", level = "debug", skip(self, rx, plan, params, stack))]
	async fn query<'a>(
		&self,
		rx: &mut StandardTransaction<'a>,
		plan: PhysicalPlan,
		params: Params,
		stack: &mut Stack,
	) -> crate::Result<Option<Columns>> {
		let mut context = ExecutionContext {
			executor: self.clone(),
			source: None,
			batch_size: 1024,
			params: params.clone(),
			stack: stack.clone(),
		};
		let mut node = compile(plan, rx, Arc::new(context.clone())).await;

		// Initialize the operator before execution
		node.initialize(rx, &context).await?;

		let mut result: Option<Columns> = None;

		while let Some(Batch {
			columns,
		}) = node.next(rx, &mut context).await?
		{
			if let Some(mut result_columns) = result.take() {
				result_columns.append_columns(columns)?;
				result = Some(result_columns);
			} else {
				result = Some(columns);
			}
		}

		// Copy stack changes back to persistent stack
		*stack = context.stack;

		let headers = node.headers();

		if let Some(mut columns) = result {
			if let Some(headers) = headers {
				columns.apply_headers(&headers);
			}

			Ok(columns.into())
		} else {
			// empty columns - reconstruct table,
			// for better UX
			let columns: Vec<Column> = node
				.headers()
				.unwrap_or(ColumnHeaders {
					columns: vec![],
				})
				.columns
				.into_iter()
				.map(|name| Column {
					name,
					data: ColumnData::undefined(0),
				})
				.collect();

			Ok(Some(Columns::new(columns)))
		}
	}
}
