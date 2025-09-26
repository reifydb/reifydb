// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use query::{
	aggregate::AggregateNode,
	compile::compile,
	extend::{ExtendNode, ExtendWithoutInputNode},
	filter::FilterNode,
	index_scan::IndexScanNode,
	inline::InlineDataNode,
	join::{InnerJoinNode, LeftJoinNode, NaturalJoinNode},
	map::{MapNode, MapWithoutInputNode},
	ring_buffer_scan::RingBufferScan,
	sort::SortNode,
	table_scan::TableScanNode,
	table_virtual_scan::VirtualScanNode,
	take::TakeNode,
	view_scan::ViewScanNode,
};
use reifydb_core::{
	Frame,
	interface::{Command, Execute, ExecuteCommand, ExecuteQuery, Params, Query, ResolvedSource, Transaction},
	value::column::{Column, ColumnData, Columns, headers::ColumnHeaders},
};
use reifydb_rql::{
	ast,
	plan::{physical::PhysicalPlan, plan},
};

use crate::{
	StandardCommandTransaction, StandardQueryTransaction, StandardTransaction,
	function::{Functions, math},
};

mod catalog;
mod mutate;
mod query;

/// Unified trait for query execution nodes following the volcano iterator
/// pattern
pub(crate) trait QueryNode<'a, T: Transaction> {
	/// Initialize the operator with execution context
	/// Called once before iteration begins
	fn initialize(&mut self, rx: &mut StandardTransaction<'a, T>, ctx: &ExecutionContext<'a>) -> crate::Result<()>;

	/// Get the next batch of results (volcano iterator pattern)
	/// Returns None when exhausted
	fn next(&mut self, rx: &mut StandardTransaction<'a, T>) -> crate::Result<Option<Batch<'a>>>;

	/// Get the headers of columns this node produces
	fn headers(&self) -> Option<ColumnHeaders<'a>>;
}

#[derive(Clone)]
pub struct ExecutionContext<'a> {
	pub functions: Functions,
	pub source: Option<ResolvedSource<'a>>,
	pub batch_size: usize,
	pub preserve_row_numbers: bool,
	pub params: Params,
}

#[derive(Debug)]
pub struct Batch<'a> {
	pub columns: Columns<'a>,
}

pub(crate) enum ExecutionPlan<'a, T: Transaction> {
	Aggregate(AggregateNode<'a, T>),
	Filter(FilterNode<'a, T>),
	IndexScan(IndexScanNode<'a, T>),
	InlineData(InlineDataNode<'a, T>),
	InnerJoin(InnerJoinNode<'a, T>),
	LeftJoin(LeftJoinNode<'a, T>),
	NaturalJoin(NaturalJoinNode<'a, T>),
	Map(MapNode<'a, T>),
	MapWithoutInput(MapWithoutInputNode<'a, T>),
	Extend(ExtendNode<'a, T>),
	ExtendWithoutInput(ExtendWithoutInputNode<'a, T>),
	Sort(SortNode<'a, T>),
	TableScan(TableScanNode<'a, T>),
	Take(TakeNode<'a, T>),
	ViewScan(ViewScanNode<'a, T>),
	VirtualScan(VirtualScanNode<'a, T>),
	RingBufferScan(RingBufferScan<'a, T>),
}

// Implement QueryNode for Box<ExecutionPlan> to allow chaining
impl<'a, T: Transaction> QueryNode<'a, T> for Box<ExecutionPlan<'a, T>> {
	fn initialize(&mut self, rx: &mut StandardTransaction<'a, T>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		(**self).initialize(rx, ctx)
	}

	fn next(&mut self, rx: &mut StandardTransaction<'a, T>) -> crate::Result<Option<Batch<'a>>> {
		(**self).next(rx)
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		(**self).headers()
	}
}

impl<'a, T: Transaction> QueryNode<'a, T> for ExecutionPlan<'a, T> {
	fn initialize(&mut self, rx: &mut StandardTransaction<'a, T>, ctx: &ExecutionContext<'a>) -> crate::Result<()> {
		match self {
			ExecutionPlan::Aggregate(node) => node.initialize(rx, ctx),
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
			ExecutionPlan::ViewScan(node) => node.initialize(rx, ctx),
			ExecutionPlan::VirtualScan(node) => node.initialize(rx, ctx),
			ExecutionPlan::RingBufferScan(node) => node.initialize(rx, ctx),
		}
	}

	fn next(&mut self, rx: &mut StandardTransaction<'a, T>) -> crate::Result<Option<Batch<'a>>> {
		match self {
			ExecutionPlan::Aggregate(node) => node.next(rx),
			ExecutionPlan::Filter(node) => node.next(rx),
			ExecutionPlan::IndexScan(node) => node.next(rx),
			ExecutionPlan::InlineData(node) => node.next(rx),
			ExecutionPlan::InnerJoin(node) => node.next(rx),
			ExecutionPlan::LeftJoin(node) => node.next(rx),
			ExecutionPlan::NaturalJoin(node) => node.next(rx),
			ExecutionPlan::Map(node) => node.next(rx),
			ExecutionPlan::MapWithoutInput(node) => node.next(rx),
			ExecutionPlan::Extend(node) => node.next(rx),
			ExecutionPlan::ExtendWithoutInput(node) => node.next(rx),
			ExecutionPlan::Sort(node) => node.next(rx),
			ExecutionPlan::TableScan(node) => node.next(rx),
			ExecutionPlan::Take(node) => node.next(rx),
			ExecutionPlan::ViewScan(node) => node.next(rx),
			ExecutionPlan::VirtualScan(node) => node.next(rx),
			ExecutionPlan::RingBufferScan(node) => node.next(rx),
		}
	}

	fn headers(&self) -> Option<ColumnHeaders<'a>> {
		match self {
			ExecutionPlan::Aggregate(node) => node.headers(),
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
			ExecutionPlan::ViewScan(node) => node.headers(),
			ExecutionPlan::VirtualScan(node) => node.headers(),
			ExecutionPlan::RingBufferScan(node) => node.headers(),
		}
	}
}

pub struct Executor {
	pub functions: Functions,
}

impl Executor {
	#[allow(dead_code)]
	pub fn testing() -> Self {
		Self {
			functions: Functions::builder()
				.register_aggregate("sum", math::aggregate::Sum::new)
				.register_aggregate("min", math::aggregate::Min::new)
				.register_aggregate("max", math::aggregate::Max::new)
				.register_aggregate("avg", math::aggregate::Avg::new)
				.register_aggregate("count", math::aggregate::Count::new)
				.register_scalar("abs", math::scalar::Abs::new)
				.register_scalar("avg", math::scalar::Avg::new)
				.build(),
		}
	}
}

impl<T: Transaction> ExecuteCommand<StandardCommandTransaction<T>> for Executor {
	fn execute_command(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		cmd: Command<'_>,
	) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = ast::parse_str(cmd.rql)?;

		for statement in statements {
			if let Some(plan) = plan(txn, statement)? {
				let er = self.execute_command_plan(txn, plan, cmd.params.clone())?;
				result.push(Frame::from(er));
			}
		}

		Ok(result)
	}
}

impl<T: Transaction> ExecuteQuery<StandardQueryTransaction<T>> for Executor {
	fn execute_query(&self, txn: &mut StandardQueryTransaction<T>, qry: Query<'_>) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = ast::parse_str(qry.rql)?;

		for statement in statements {
			if let Some(plan) = plan(txn, statement)? {
				let er = self.execute_query_plan(txn, plan, qry.params.clone())?;
				result.push(Frame::from(er));
			}
		}

		Ok(result)
	}
}

impl<T: Transaction> Execute<StandardCommandTransaction<T>, StandardQueryTransaction<T>> for Executor {}

impl Executor {
	pub(crate) fn execute_query_plan<'a, T: Transaction>(
		&self,
		rx: &'a mut StandardQueryTransaction<T>,
		plan: PhysicalPlan<'a>,
		params: Params,
	) -> crate::Result<Columns<'a>> {
		match plan {
			// Query
			PhysicalPlan::Aggregate(_)
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
			| PhysicalPlan::Delete(_)
			| PhysicalPlan::DeleteRingBuffer(_)
			| PhysicalPlan::InsertTable(_)
			| PhysicalPlan::InsertRingBuffer(_)
			| PhysicalPlan::Update(_)
			| PhysicalPlan::UpdateRingBuffer(_)
			| PhysicalPlan::TableScan(_)
			| PhysicalPlan::ViewScan(_)
			| PhysicalPlan::TableVirtualScan(_)
			| PhysicalPlan::RingBufferScan(_) => {
				let mut std_txn = StandardTransaction::from(rx);
				self.query(&mut std_txn, plan, params)
			}

			PhysicalPlan::AlterSequence(_)
			| PhysicalPlan::AlterTable(_)
			| PhysicalPlan::AlterView(_)
			| PhysicalPlan::CreateDeferredView(_)
			| PhysicalPlan::CreateTransactionalView(_)
			| PhysicalPlan::CreateNamespace(_)
			| PhysicalPlan::CreateTable(_)
			| PhysicalPlan::CreateRingBuffer(_)
			| PhysicalPlan::Distinct(_) => unreachable!(), // FIXME return explanatory diagnostic
			PhysicalPlan::Apply(_) => {
				// Apply operator requires flow engine for mod
				// execution
				unimplemented!(
					"Apply operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
				)
			}
		}
	}

	pub fn execute_command_plan<'a, T: Transaction>(
		&self,
		txn: &'a mut StandardCommandTransaction<T>,
		plan: PhysicalPlan<'a>,
		params: Params,
	) -> crate::Result<Columns<'a>> {
		match plan {
			PhysicalPlan::AlterSequence(plan) => self.alter_table_sequence(txn, plan),
			PhysicalPlan::CreateDeferredView(plan) => self.create_deferred_view(txn, plan),
			PhysicalPlan::CreateTransactionalView(plan) => self.create_transactional_view(txn, plan),
			PhysicalPlan::CreateNamespace(plan) => self.create_namespace(txn, plan),
			PhysicalPlan::CreateTable(plan) => self.create_table(txn, plan),
			PhysicalPlan::CreateRingBuffer(plan) => self.create_ring_buffer(txn, plan),
			PhysicalPlan::Delete(plan) => self.delete(txn, plan, params),
			PhysicalPlan::DeleteRingBuffer(plan) => self.delete_ring_buffer(txn, plan, params),
			PhysicalPlan::InsertTable(plan) => self.insert_table(txn, plan, params),
			PhysicalPlan::InsertRingBuffer(plan) => self.insert_ring_buffer(txn, plan, params),
			PhysicalPlan::Update(plan) => self.update_table(txn, plan, params),
			PhysicalPlan::UpdateRingBuffer(plan) => self.update_ring_buffer(txn, plan, params),

			PhysicalPlan::Aggregate(_)
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
			| PhysicalPlan::TableScan(_)
			| PhysicalPlan::ViewScan(_)
			| PhysicalPlan::TableVirtualScan(_)
			| PhysicalPlan::RingBufferScan(_)
			| PhysicalPlan::Distinct(_) => {
				let mut std_txn = StandardTransaction::from(txn);
				self.query(&mut std_txn, plan, params)
			}
			PhysicalPlan::Apply(_) => {
				let mut std_txn = StandardTransaction::from(txn);
				self.query(&mut std_txn, plan, params)
			}

			PhysicalPlan::AlterTable(plan) => self.alter_table(txn, plan),
			PhysicalPlan::AlterView(plan) => self.execute_alter_view(txn, plan),
		}
	}

	fn query<'a, T: Transaction>(
		&self,
		rx: &mut StandardTransaction<'a, T>,
		plan: PhysicalPlan<'a>,
		params: Params,
	) -> crate::Result<Columns<'a>> {
		match plan {
			// PhysicalPlan::Describe { plan } => {
			//     // FIXME evaluating the entire columns is quite
			// wasteful but good enough to write some tests
			//     let result = self.execute_query_plan(rx, *plan)?;
			//     let ExecutionResult::Query { columns, .. } =
			// result else { panic!() };
			//     Ok(ExecutionResult::DescribeQuery { columns })
			// }
			_ => {
				let context = Arc::new(ExecutionContext {
					functions: self.functions.clone(),
					source: None,
					batch_size: 1024,
					preserve_row_numbers: false,
					params: params.clone(),
				});
				let mut node = compile(plan, rx, context.clone());

				// Initialize the operator before execution
				node.initialize(rx, &context)?;

				let mut result: Option<Columns> = None;

				while let Some(Batch {
					columns,
				}) = node.next(rx)?
				{
					if let Some(mut result_columns) = result.take() {
						result_columns.append_columns(columns)?;
						result = Some(result_columns);
					} else {
						result = Some(columns);
					}
				}

				let headers = node.headers();

				if let Some(mut columns) = result {
					if let Some(headers) = headers {
						columns.apply_headers(&headers);
					}

					Ok(columns.into())
				} else {
					// empty columns - reconstruct table,
					// for better UX
					let columns: Vec<Column<'a>> = node
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

					Ok(Columns::new(columns))
				}
			}
		}
	}
}
