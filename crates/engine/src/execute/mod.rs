// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

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
use reifydb_catalog::{
	catalog::Catalog,
	vtable::{system::flow_operator_store::FlowOperatorStore, user::registry::UserVTableRegistry},
};
use reifydb_core::{
	interface::{auth::Identity, resolved::ResolvedPrimitive},
	util::ioc::IocContainer,
	value::{
		batch::lazy::LazyBatch,
		column::{Column, columns::Columns, data::ColumnData, headers::ColumnHeaders},
	},
};
use reifydb_function::{math, registry::Functions, series, subscription};
use reifydb_type::{params::Params, value::frame::frame::Frame};

// Types moved from reifydb-core (formerly in interface/execute.rs)

/// A batch of columnar data returned from query execution
#[derive(Debug)]
pub struct Batch {
	pub columns: Columns,
}

/// Admin execution request (DDL + DML + Query)
#[derive(Debug)]
pub struct Admin<'a> {
	pub rql: &'a str,
	pub params: Params,
	pub identity: &'a Identity,
}

/// Command execution request (DML + Query)
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

use ast::parse_str;
use reifydb_metric::metric::MetricReader;
use reifydb_rql::{
	ast,
	plan::{physical::PhysicalPlan, plan},
};
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::{
	Transaction, admin::AdminTransaction, command::CommandTransaction, query::QueryTransaction,
};
use tracing::instrument;

use crate::{
	execute::query::join::{inner::InnerJoinNode, left::LeftJoinNode, natural::NaturalJoinNode},
	stack::{Stack, Variable},
};

pub mod ddl;
pub(crate) mod dispatch;
pub(crate) mod dml;
pub mod query;

/// Unified trait for query execution nodes following the volcano iterator pattern

pub(crate) trait QueryNode {
	/// Initialize the operator with execution context
	/// Called once before iteration begins
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &ExecutionContext) -> crate::Result<()>;

	/// Get the next batch of results (volcano iterator pattern)
	/// Returns None when exhausted
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut ExecutionContext) -> crate::Result<Option<Batch>>;

	/// Get the next batch as a LazyBatch for deferred materialization
	/// Returns None if this node doesn't support lazy evaluation or is exhausted
	/// Default implementation returns None (falls back to materialized evaluation)
	fn next_lazy<'a>(
		&mut self,
		_rx: &mut Transaction<'a>,
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

impl QueryNode for Box<ExecutionPlan> {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		(**self).initialize(rx, ctx)
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut ExecutionContext) -> crate::Result<Option<Batch>> {
		(**self).next(rx, ctx)
	}

	fn next_lazy<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<LazyBatch>> {
		(**self).next_lazy(rx, ctx)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		(**self).headers()
	}
}

impl QueryNode for ExecutionPlan {
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &ExecutionContext) -> crate::Result<()> {
		match self {
			ExecutionPlan::Aggregate(node) => node.initialize(rx, ctx),
			ExecutionPlan::DictionaryScan(node) => node.initialize(rx, ctx),
			ExecutionPlan::Filter(node) => node.initialize(rx, ctx),
			ExecutionPlan::IndexScan(node) => node.initialize(rx, ctx),
			ExecutionPlan::InlineData(node) => node.initialize(rx, ctx),
			ExecutionPlan::InnerJoin(node) => node.initialize(rx, ctx),
			ExecutionPlan::LeftJoin(node) => node.initialize(rx, ctx),
			ExecutionPlan::NaturalJoin(node) => node.initialize(rx, ctx),
			ExecutionPlan::Map(node) => node.initialize(rx, ctx),
			ExecutionPlan::MapWithoutInput(node) => node.initialize(rx, ctx),
			ExecutionPlan::Extend(node) => node.initialize(rx, ctx),
			ExecutionPlan::ExtendWithoutInput(node) => node.initialize(rx, ctx),
			ExecutionPlan::Sort(node) => node.initialize(rx, ctx),
			ExecutionPlan::TableScan(node) => node.initialize(rx, ctx),
			ExecutionPlan::Take(node) => node.initialize(rx, ctx),
			ExecutionPlan::TopK(node) => node.initialize(rx, ctx),
			ExecutionPlan::ViewScan(node) => node.initialize(rx, ctx),
			ExecutionPlan::Variable(node) => node.initialize(rx, ctx),
			ExecutionPlan::Environment(node) => node.initialize(rx, ctx),
			ExecutionPlan::VirtualScan(node) => node.initialize(rx, ctx),
			ExecutionPlan::RingBufferScan(node) => node.initialize(rx, ctx),
			ExecutionPlan::Generator(node) => node.initialize(rx, ctx),
			ExecutionPlan::Declare(node) => node.initialize(rx, ctx),
			ExecutionPlan::Assign(node) => node.initialize(rx, ctx),
			ExecutionPlan::Conditional(node) => node.initialize(rx, ctx),
			ExecutionPlan::Scalarize(node) => node.initialize(rx, ctx),
			ExecutionPlan::RowPointLookup(node) => node.initialize(rx, ctx),
			ExecutionPlan::RowListLookup(node) => node.initialize(rx, ctx),
			ExecutionPlan::RowRangeScan(node) => node.initialize(rx, ctx),
		}
	}

	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut ExecutionContext) -> crate::Result<Option<Batch>> {
		match self {
			ExecutionPlan::Aggregate(node) => node.next(rx, ctx),
			ExecutionPlan::DictionaryScan(node) => node.next(rx, ctx),
			ExecutionPlan::Filter(node) => node.next(rx, ctx),
			ExecutionPlan::IndexScan(node) => node.next(rx, ctx),
			ExecutionPlan::InlineData(node) => node.next(rx, ctx),
			ExecutionPlan::InnerJoin(node) => node.next(rx, ctx),
			ExecutionPlan::LeftJoin(node) => node.next(rx, ctx),
			ExecutionPlan::NaturalJoin(node) => node.next(rx, ctx),
			ExecutionPlan::Map(node) => node.next(rx, ctx),
			ExecutionPlan::MapWithoutInput(node) => node.next(rx, ctx),
			ExecutionPlan::Extend(node) => node.next(rx, ctx),
			ExecutionPlan::ExtendWithoutInput(node) => node.next(rx, ctx),
			ExecutionPlan::Sort(node) => node.next(rx, ctx),
			ExecutionPlan::TableScan(node) => node.next(rx, ctx),
			ExecutionPlan::Take(node) => node.next(rx, ctx),
			ExecutionPlan::TopK(node) => node.next(rx, ctx),
			ExecutionPlan::ViewScan(node) => node.next(rx, ctx),
			ExecutionPlan::Variable(node) => node.next(rx, ctx),
			ExecutionPlan::Environment(node) => node.next(rx, ctx),
			ExecutionPlan::VirtualScan(node) => node.next(rx, ctx),
			ExecutionPlan::RingBufferScan(node) => node.next(rx, ctx),
			ExecutionPlan::Generator(node) => node.next(rx, ctx),
			ExecutionPlan::Declare(node) => node.next(rx, ctx),
			ExecutionPlan::Assign(node) => node.next(rx, ctx),
			ExecutionPlan::Conditional(node) => node.next(rx, ctx),
			ExecutionPlan::Scalarize(node) => node.next(rx, ctx),
			ExecutionPlan::RowPointLookup(node) => node.next(rx, ctx),
			ExecutionPlan::RowListLookup(node) => node.next(rx, ctx),
			ExecutionPlan::RowRangeScan(node) => node.next(rx, ctx),
		}
	}

	fn next_lazy<'a>(
		&mut self,
		rx: &mut Transaction<'a>,
		ctx: &mut ExecutionContext,
	) -> crate::Result<Option<LazyBatch>> {
		match self {
			// Only TableScan supports lazy evaluation for now
			ExecutionPlan::TableScan(node) => node.next_lazy(rx, ctx),
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
	pub stats_reader: MetricReader<SingleStore>,
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
		stats_reader: MetricReader<SingleStore>,
		ioc: IocContainer,
	) -> Self {
		Self(Arc::new(ExecutorInner {
			catalog,
			functions,
			flow_operator_store,
			virtual_table_registry: UserVTableRegistry::new(),
			stats_reader,
			ioc,
		}))
	}

	#[allow(dead_code)]
	pub fn testing() -> Self {
		let store = SingleStore::testing_memory();
		Self::new(
			Catalog::testing(),
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
				.build(),
			FlowOperatorStore::new(),
			MetricReader::new(store),
			IocContainer::new(),
		)
	}
}

/// Populate a stack with parameters so they can be accessed as variables.
fn populate_stack(stack: &mut Stack, params: &Params) -> crate::Result<()> {
	match params {
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
	Ok(())
}

impl Executor {
	#[instrument(name = "executor::execute_admin_statements", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn execute_admin_statements(
		&self,
		txn: &mut AdminTransaction,
		cmd: Admin<'_>,
	) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = parse_str(cmd.rql)?;
		let mut stack = Stack::new();
		populate_stack(&mut stack, &cmd.params)?;

		for statement in statements {
			if let Some(physical_plan) = plan(&self.catalog, txn, statement)? {
				if let Some(columns) =
					self.dispatch_admin(txn, physical_plan, cmd.params.clone(), &mut stack)?
				{
					result.push(Frame::from(columns));
				}
			}
		}

		Ok(result)
	}

	#[instrument(name = "executor::execute_command_statements", level = "debug", skip(self, txn, cmd), fields(rql = %cmd.rql))]
	pub fn execute_command_statements(
		&self,
		txn: &mut CommandTransaction,
		cmd: Command<'_>,
	) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = parse_str(cmd.rql)?;
		let mut stack = Stack::new();
		populate_stack(&mut stack, &cmd.params)?;

		for statement in statements {
			if let Some(physical_plan) = plan(&self.catalog, txn, statement)? {
				if let Some(columns) =
					self.dispatch_command(txn, physical_plan, cmd.params.clone(), &mut stack)?
				{
					result.push(Frame::from(columns));
				}
			}
		}

		Ok(result)
	}

	#[instrument(name = "executor::execute_query_statements", level = "debug", skip(self, txn, qry), fields(rql = %qry.rql))]
	pub fn execute_query_statements(
		&self,
		txn: &mut QueryTransaction,
		qry: Query<'_>,
	) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = parse_str(qry.rql)?;
		let mut stack = Stack::new();
		populate_stack(&mut stack, &qry.params)?;

		for statement in statements {
			if let Some(physical_plan) = plan(&self.catalog, txn, statement)? {
				if let Some(columns) =
					self.dispatch_query(txn, physical_plan, qry.params.clone(), &mut stack)?
				{
					result.push(Frame::from(columns));
				}
			}
		}

		Ok(result)
	}

	#[instrument(name = "executor::query", level = "debug", skip(self, rx, plan, params, stack))]
	fn query<'a>(
		&self,
		rx: &mut Transaction<'a>,
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
		let mut node = compile(plan, rx, Arc::new(context.clone()));

		// Initialize the operator before execution
		node.initialize(rx, &context)?;

		let mut result: Option<Columns> = None;

		while let Some(Batch {
			columns,
		}) = node.next(rx, &mut context)?
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
