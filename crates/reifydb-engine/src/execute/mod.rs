// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::Arc;

use query::{
	aggregate::AggregateNode,
	compile::compile,
	extend::{ExtendNode, ExtendWithoutInputNode},
	filter::FilterNode,
	inline::InlineDataNode,
	join_inner::InnerJoinNode,
	join_left::LeftJoinNode,
	join_natural::NaturalJoinNode,
	map::{MapNode, MapWithoutInputNode},
	sort::SortNode,
	table_scan::TableScanNode,
	take::TakeNode,
	view_scan::ViewScanNode,
	virtual_table_scan::VirtualScanNode,
};
use reifydb_core::{
	Frame,
	interface::{
		Command, Execute, ExecuteCommand, ExecuteQuery, Params, Query,
		TableDef, Transaction,
	},
};
use reifydb_rql::{
	ast,
	plan::{physical::PhysicalPlan, plan},
};

use crate::{
	StandardCommandTransaction, StandardQueryTransaction,
	StandardTransaction,
	columnar::{
		Column, ColumnData, ColumnQualified, Columns, SourceQualified,
		layout::ColumnsLayout,
	},
	function::{Functions, math},
};

mod catalog;
mod mutate;
mod query;

#[derive(Clone)]
pub struct ExecutionContext {
	pub functions: Functions,
	pub table: Option<TableDef>,
	pub batch_size: usize,
	pub preserve_row_numbers: bool,
	pub params: Params,
}

#[derive(Debug)]
pub(crate) struct Batch {
	pub columns: Columns,
}

pub(crate) enum ExecutionPlan<T: Transaction> {
	Aggregate(AggregateNode<T>),
	Filter(FilterNode<T>),
	InlineData(InlineDataNode<T>),
	InnerJoin(InnerJoinNode<T>),
	LeftJoin(LeftJoinNode<T>),
	NaturalJoin(NaturalJoinNode<T>),
	Map(MapNode<T>),
	MapWithoutInput(MapWithoutInputNode<T>),
	Extend(ExtendNode<T>),
	ExtendWithoutInput(ExtendWithoutInputNode<T>),
	Sort(SortNode<T>),
	TableScan(TableScanNode<T>),
	Take(TakeNode<T>),
	ViewScan(ViewScanNode<T>),
	VirtualScan(VirtualScanNode<T>),
}

impl<T: Transaction> ExecutionPlan<T> {
	pub(crate) fn next(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut StandardTransaction<T>,
	) -> crate::Result<Option<Batch>> {
		match self {
			ExecutionPlan::Aggregate(node) => node.next(ctx, rx),
			ExecutionPlan::Filter(node) => node.next(ctx, rx),
			ExecutionPlan::InlineData(node) => node.next(ctx, rx),
			ExecutionPlan::InnerJoin(node) => node.next(ctx, rx),
			ExecutionPlan::LeftJoin(node) => node.next(ctx, rx),
			ExecutionPlan::NaturalJoin(node) => node.next(ctx, rx),
			ExecutionPlan::Map(node) => node.next(ctx, rx),
			ExecutionPlan::MapWithoutInput(node) => {
				node.next(ctx, rx)
			}
			ExecutionPlan::Extend(node) => node.next(ctx, rx),
			ExecutionPlan::ExtendWithoutInput(node) => {
				node.next(ctx, rx)
			}
			ExecutionPlan::Sort(node) => node.next(ctx, rx),
			ExecutionPlan::TableScan(node) => node.next(ctx, rx),
			ExecutionPlan::Take(node) => node.next(ctx, rx),
			ExecutionPlan::ViewScan(node) => node.next(ctx, rx),
			ExecutionPlan::VirtualScan(node) => node.next(ctx, rx),
		}
	}

	pub(crate) fn layout(&self) -> Option<ColumnsLayout> {
		match self {
			ExecutionPlan::Aggregate(node) => node.layout(),
			ExecutionPlan::Filter(node) => node.layout(),
			ExecutionPlan::InlineData(node) => node.layout(),
			ExecutionPlan::InnerJoin(node) => node.layout(),
			ExecutionPlan::LeftJoin(node) => node.layout(),
			ExecutionPlan::NaturalJoin(node) => node.layout(),
			ExecutionPlan::Map(node) => node.layout(),
			ExecutionPlan::MapWithoutInput(node) => node.layout(),
			ExecutionPlan::Extend(node) => node.layout(),
			ExecutionPlan::ExtendWithoutInput(node) => {
				node.layout()
			}
			ExecutionPlan::Sort(node) => node.layout(),
			ExecutionPlan::TableScan(node) => node.layout(),
			ExecutionPlan::Take(node) => node.layout(),
			ExecutionPlan::ViewScan(node) => node.layout(),
			ExecutionPlan::VirtualScan(node) => node.layout(),
		}
	}
}

pub(crate) struct Executor {
	pub functions: Functions,
}

impl Executor {
	#[allow(dead_code)]
	pub(crate) fn testing() -> Self {
		Self {
			functions: Functions::builder()
				.register_aggregate(
					"sum",
					math::aggregate::Sum::new,
				)
				.register_aggregate(
					"min",
					math::aggregate::Min::new,
				)
				.register_aggregate(
					"max",
					math::aggregate::Max::new,
				)
				.register_aggregate(
					"avg",
					math::aggregate::Avg::new,
				)
				.register_aggregate(
					"count",
					math::aggregate::Count::new,
				)
				.register_scalar("abs", math::scalar::Abs::new)
				.register_scalar("avg", math::scalar::Avg::new)
				.build(),
		}
	}
}

impl<T: Transaction> ExecuteCommand<StandardCommandTransaction<T>>
	for Executor
{
	fn execute_command(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		cmd: Command<'_>,
	) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = ast::parse_str(cmd.rql)?;

		for statement in statements {
			if let Some(plan) = plan(txn, statement)? {
				let er = self.execute_command_plan(
					txn,
					plan,
					cmd.params.clone(),
				)?;
				result.push(er);
			}
		}

		Ok(result.into_iter().map(Frame::from).collect())
	}
}

impl<T: Transaction> ExecuteQuery<StandardQueryTransaction<T>> for Executor {
	fn execute_query(
		&self,
		txn: &mut StandardQueryTransaction<T>,
		qry: Query<'_>,
	) -> crate::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = ast::parse_str(qry.rql)?;

		for statement in statements {
			if let Some(plan) = plan(txn, statement)? {
				let er = self.execute_query_plan(
					txn,
					plan,
					qry.params.clone(),
				)?;
				result.push(er);
			}
		}

		Ok(result.into_iter().map(Frame::from).collect())
	}
}

impl<T: Transaction>
	Execute<StandardCommandTransaction<T>, StandardQueryTransaction<T>>
	for Executor
{
}

impl Executor {
	pub(crate) fn execute_query_plan<T: Transaction>(
		&self,
		rx: &mut StandardQueryTransaction<T>,
		plan: PhysicalPlan,
		params: Params,
	) -> crate::Result<Columns> {
		match plan {
			// Query
			PhysicalPlan::Aggregate(_)
			| PhysicalPlan::Filter(_)
			| PhysicalPlan::JoinInner(_)
			| PhysicalPlan::JoinLeft(_)
			| PhysicalPlan::JoinNatural(_)
			| PhysicalPlan::Take(_)
			| PhysicalPlan::Sort(_)
			| PhysicalPlan::Map(_)
			| PhysicalPlan::Extend(_)
			| PhysicalPlan::InlineData(_)
			| PhysicalPlan::Delete(_)
			| PhysicalPlan::Insert(_)
			| PhysicalPlan::Update(_)
			| PhysicalPlan::TableScan(_)
			| PhysicalPlan::ViewScan(_)
			| PhysicalPlan::VirtualScan(_) => {
				let mut std_txn = StandardTransaction::from(rx);
				self.query(&mut std_txn, plan, params)
			}

			PhysicalPlan::AlterSequence(_)
			| PhysicalPlan::CreateDeferredView(_)
			| PhysicalPlan::CreateTransactionalView(_)
			| PhysicalPlan::CreateSchema(_)
			| PhysicalPlan::CreateTable(_)
			| PhysicalPlan::Distinct(_) => unreachable!(), /* FIXME return explanatory diagnostic */
		}
	}

	pub(crate) fn execute_command_plan<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		plan: PhysicalPlan,
		params: Params,
	) -> crate::Result<Columns> {
		match plan {
			PhysicalPlan::AlterSequence(plan) => {
				self.alter_table_sequence(txn, plan)
			}
			PhysicalPlan::CreateDeferredView(plan) => {
				self.create_deferred_view(txn, plan)
			}
			PhysicalPlan::CreateTransactionalView(plan) => {
				self.create_transactional_view(txn, plan)
			}
			PhysicalPlan::CreateSchema(plan) => {
				self.create_schema(txn, plan)
			}
			PhysicalPlan::CreateTable(plan) => {
				self.create_table(txn, plan)
			}
			PhysicalPlan::Delete(plan) => {
				self.delete(txn, plan, params)
			}
			PhysicalPlan::Insert(plan) => {
				self.insert(txn, plan, params)
			}
			PhysicalPlan::Update(plan) => {
				self.update(txn, plan, params)
			}

			PhysicalPlan::Aggregate(_)
			| PhysicalPlan::Filter(_)
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
			| PhysicalPlan::VirtualScan(_)
			| PhysicalPlan::Distinct(_) => {
				let mut std_txn =
					StandardTransaction::from(txn);
				self.query(&mut std_txn, plan, params)
			}
		}
	}

	fn query<T: Transaction>(
		&self,
		rx: &mut StandardTransaction<'_, T>,
		plan: PhysicalPlan,
		params: Params,
	) -> crate::Result<Columns> {
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
					table: None,
					batch_size: 1024,
					preserve_row_numbers: false,
					params: params.clone(),
				});
				let mut node =
					compile(plan, rx, context.clone());
				let mut result: Option<Columns> = None;

				while let Some(Batch {
					columns,
				}) = node.next(&context, rx)?
				{
					if let Some(mut result_columns) =
						result.take()
					{
						result_columns.append_columns(
							columns,
						)?;
						result = Some(result_columns);
					} else {
						result = Some(columns);
					}
				}

				let layout = node.layout();

				if let Some(mut columns) = result {
					if let Some(layout) = layout {
						columns.apply_layout(&layout);
					}

					Ok(columns.into())
				} else {
					// empty columns - reconstruct table,
					// for better UX
					let columns: Vec<Column> = node
                        .layout()
                        .unwrap_or(ColumnsLayout { columns: vec![] })
                        .columns
                        .into_iter()
                        .map(|layout| match layout.source {
                            Some(source) => Column::SourceQualified(SourceQualified {
								source: source,
                                name: layout.name,
                                data: ColumnData::undefined(0),
                            }),
                            None => Column::ColumnQualified(ColumnQualified {
                                name: layout.name,
                                data: ColumnData::undefined(0),
                            }),
                        })
                        .collect();

					Ok(Columns::new(columns))
				}
			}
		}
	}
}
