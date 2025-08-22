// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{marker::PhantomData, sync::Arc};

use query::compile::compile;
use reifydb_core::interface::{
	StandardCdcTransaction, UnderlyingQueryTransaction,
};
use reifydb_core::{
	interface::{
		Command, Execute, ExecuteCommand,
		ExecuteQuery, Params, Query,
		StandardTransaction, TableDef, Transaction,
		VersionedQueryTransaction,
	},
	transaction::{CommandTransaction, QueryTransaction},
	Frame,
};
use reifydb_rql::{
	ast,
	plan::{physical::PhysicalPlan, plan},
};
use reifydb_storage::memory::Memory;
use reifydb_transaction::{
	mvcc::transaction::serializable::Serializable, svl::SingleVersionLock,
};

use crate::{
	columnar::{
		layout::ColumnsLayout, Column, ColumnData, ColumnQualified, Columns,
		SourceQualified,
	},
	function::{math, Functions},
};

mod catalog;
mod mutate;
mod query;

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

pub(crate) trait ExecutionPlan {
	fn next(
		&mut self,
		ctx: &ExecutionContext,
		rx: &mut dyn VersionedQueryTransaction,
	) -> crate::Result<Option<Batch>>;
	fn layout(&self) -> Option<ColumnsLayout>;
}

pub(crate) struct Executor<T: Transaction> {
	pub functions: Functions,
	pub _phantom: PhantomData<T>,
}

impl
	Executor<
		StandardTransaction<
			Serializable<Memory, SingleVersionLock<Memory>>,
			SingleVersionLock<Memory>,
			StandardCdcTransaction<Memory>,
		>,
	>
{
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
			_phantom: PhantomData,
		}
	}
}

impl<T: Transaction> ExecuteCommand<T> for Executor<T> {
	fn execute_command<'a>(
		&'a self,
		txn: &mut CommandTransaction<T>,
		cmd: Command<'a>,
	) -> reifydb_core::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = ast::parse(cmd.rql)?;

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

impl<T: Transaction> ExecuteQuery<T> for Executor<T> {
	fn execute_query<'a>(
		&'a self,
		txn: &mut QueryTransaction<T>,
		qry: Query<'a>,
	) -> reifydb_core::Result<Vec<Frame>> {
		let mut result = vec![];
		let statements = ast::parse(qry.rql)?;

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

impl<T: Transaction> Execute<T> for Executor<T> {}

impl<T: Transaction> Executor<T> {
	pub(crate) fn execute_query_plan(
		&self,
		rx: &mut impl UnderlyingQueryTransaction,
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
			| PhysicalPlan::InlineData(_)
			| PhysicalPlan::Delete(_)
			| PhysicalPlan::Insert(_)
			| PhysicalPlan::Update(_)
			| PhysicalPlan::TableScan(_)
			| PhysicalPlan::ViewScan(_) => self.query(rx, plan, params),

			PhysicalPlan::AlterSequence(_)
			| PhysicalPlan::CreateDeferredView(_)
			| PhysicalPlan::CreateTransactionalView(_)
			| PhysicalPlan::CreateSchema(_)
			| PhysicalPlan::CreateTable(_) => unreachable!(), /* FIXME return explanatory diagnostic */
		}
	}

	pub(crate) fn execute_command_plan(
		&self,
		txn: &mut CommandTransaction<T>,
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
			| PhysicalPlan::InlineData(_)
			| PhysicalPlan::TableScan(_)
			| PhysicalPlan::ViewScan(_) => self.query(txn, plan, params),
		}
	}

	fn query(
		&self,
		rx: &mut impl UnderlyingQueryTransaction,
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
