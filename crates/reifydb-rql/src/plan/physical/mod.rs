// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod alter;
mod create;

use std::sync::Arc;

use reifydb_catalog::{
	CatalogStore, system::SystemCatalog, table::TableColumnToCreate,
	view::ViewColumnToCreate,
};
use reifydb_core::{
	Fragment, JoinType, SortKey,
	interface::{
		QueryTransaction, SchemaDef, TableDef, TableVirtualDef,
		ViewDef,
		evaluate::expression::{AliasExpression, Expression},
	},
	result::error::diagnostic::catalog::{
		schema_not_found, table_not_found,
	},
	return_error,
};

use crate::plan::{
	logical::LogicalPlan,
	physical::PhysicalPlan::{TableScan, ViewScan},
};

struct Compiler {}

pub fn compile_physical<'a>(
	rx: &mut impl QueryTransaction,
	logical: Vec<LogicalPlan<'a>>,
) -> crate::Result<Option<PhysicalPlan<'a>>> {
	Compiler::compile(rx, logical)
}

impl Compiler {
	fn compile<'a>(
		rx: &mut impl QueryTransaction,
		logical: Vec<LogicalPlan<'a>>,
	) -> crate::Result<Option<PhysicalPlan<'a>>> {
		if logical.is_empty() {
			return Ok(None);
		}

		let mut stack: Vec<PhysicalPlan<'a>> = Vec::new();
		for plan in logical {
			match plan {
				LogicalPlan::Aggregate(aggregate) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Aggregate(
						AggregateNode {
							by: aggregate.by,
							map: aggregate.map,
							input: Box::new(input),
						},
					));
				}

				LogicalPlan::CreateSchema(create) => {
					stack.push(
						Self::compile_create_schema(
							rx, create,
						)?,
					);
				}

				LogicalPlan::CreateTable(create) => {
					stack.push(Self::compile_create_table(
						rx, create,
					)?);
				}

				LogicalPlan::CreateDeferredView(create) => {
					stack.push(
						Self::compile_create_deferred(
							rx, create,
						)?,
					);
				}

				LogicalPlan::CreateTransactionalView(
					create,
				) => {
					stack.push(
						Self::compile_create_transactional(
							rx, create,
						)?,
					);
				}

				LogicalPlan::AlterSequence(alter) => {
					stack.push(
						Self::compile_alter_sequence(
							rx, alter,
						)?,
					);
				}

				LogicalPlan::Filter(filter) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Filter(
						FilterNode {
							conditions: vec![filter
									.condition],
							input: Box::new(input),
						},
					));
				}

				LogicalPlan::InlineData(inline) => {
					stack.push(PhysicalPlan::InlineData(
						InlineDataNode {
							rows: inline.rows,
						},
					));
				}

				LogicalPlan::Delete(delete) => {
					// If delete has its own input, compile
					// it first Otherwise, try to pop
					// from stack (for pipeline operations)
					let input =
						if let Some(delete_input) =
							delete.input
						{
							// Recursively compile
							// the input pipeline
							let sub_plan = Self::compile(rx, vec![*delete_input])?
							.expect("Delete input must produce a plan");
							Some(Box::new(sub_plan))
						} else {
							stack.pop().map(|i| {
								Box::new(i)
							})
						};

					stack.push(PhysicalPlan::Delete(
						DeletePlan {
							input,
							schema: delete.schema,
							table: delete.table,
						},
					))
				}

				LogicalPlan::Insert(insert) => {
					let input = stack.pop().unwrap();
					stack.push(PhysicalPlan::Insert(
						InsertPlan {
							input: Box::new(input),
							schema: insert.schema,
							table: insert.table,
						},
					))
				}

				LogicalPlan::Update(update) => {
					// If update has its own input, compile
					// it first Otherwise, pop from
					// stack (for pipeline operations)
					let input =
						if let Some(update_input) =
							update.input
						{
							// Recursively compile
							// the input pipeline
							let sub_plan = Self::compile(rx, vec![*update_input])?
							.expect("Update input must produce a plan");
							Box::new(sub_plan)
						} else {
							Box::new(
								stack.pop()
									.expect(
										"Update requires input",
									),
							)
						};

					stack.push(PhysicalPlan::Update(
						UpdatePlan {
							input,
							schema: update.schema,
							table: update.table,
						},
					))
				}

				LogicalPlan::JoinInner(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right =
						Self::compile(rx, join.with)?
							.unwrap();
					stack.push(PhysicalPlan::JoinInner(
						JoinInnerNode {
							left: Box::new(left),
							right: Box::new(right),
							on: join.on,
						},
					));
				}

				LogicalPlan::JoinLeft(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right =
						Self::compile(rx, join.with)?
							.unwrap();
					stack.push(PhysicalPlan::JoinLeft(
						JoinLeftNode {
							left: Box::new(left),
							right: Box::new(right),
							on: join.on,
						},
					));
				}

				LogicalPlan::JoinNatural(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right =
						Self::compile(rx, join.with)?
							.unwrap();
					stack.push(PhysicalPlan::JoinNatural(
						JoinNaturalNode {
							left: Box::new(left),
							right: Box::new(right),
							join_type: join
								.join_type,
						},
					));
				}

				LogicalPlan::Order(order) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Sort(
						SortNode {
							by: order.by,
							input: Box::new(input),
						},
					));
				}

				LogicalPlan::Distinct(distinct) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Distinct(
						DistinctNode {
							columns: distinct
								.columns,
							input: Box::new(input),
						},
					));
				}

				LogicalPlan::Map(map) => {
					let input = stack.pop().map(Box::new);
					stack.push(PhysicalPlan::Map(
						MapNode {
							map: map.map,
							input,
						},
					));
				}

				LogicalPlan::Extend(extend) => {
					let input = stack.pop().map(Box::new);
					stack.push(PhysicalPlan::Extend(
						ExtendNode {
							extend: extend.extend,
							input,
						},
					));
				}

				LogicalPlan::SourceScan(scan) => {
					let Some(schema) = CatalogStore::find_schema_by_name(
							rx,
							scan.schema.fragment(),
						)?
					else {
						return_error!(
							schema_not_found(
								scan.schema.clone(),
								scan.schema.fragment()
							)
						);
					};

					if let Some(table) = CatalogStore::find_table_by_name(
							rx,
							schema.id,
							scan.source.fragment(),
						)? {
						stack.push(TableScan(
							TableScanNode {
								schema,
								table,
							},
						));
					} else if let Some(view) = CatalogStore::find_view_by_name(
							rx,
							schema.id,
							scan.source.fragment(),
						)? {
						stack.push(ViewScan(
							ViewScanNode {
								schema,
								view,
							},
						));
					} else if schema.name == "system" && scan.source.fragment() == "sequences" {
						stack.push(PhysicalPlan::TableVirtualScan(
							TableVirtualScanNode {
								schema,
								table: SystemCatalog::sequences(),
								pushdown_context: None, // TODO: Detect pushdown opportunities
							},
						));
					} else {
						return_error!(
							table_not_found(
								scan.source.clone(),
								scan.schema.fragment(),
								scan.source.fragment()
							)
						);
					}
				}

				LogicalPlan::Take(take) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Take(
						TakeNode {
							take: take.take,
							input: Box::new(input),
						},
					));
				}

				LogicalPlan::Chain(chain) => {
					// Compile the chain of operations
					// This ensures they all share the same
					// stack
					let chain_result =
						Self::compile(rx, chain.steps)?;
					if let Some(result) = chain_result {
						stack.push(result);
					}
				}

				_ => unimplemented!(),
			}
		}

		if stack.len() != 1 {
			// return Err("Logical plan did not reduce to a single
			// physical plan".into());
			dbg!(&stack);
			panic!(
				"logical plan did not reduce to a single physical plan"
			); // FIXME
		}

		Ok(Some(stack.pop().unwrap()))
	}
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan<'a> {
	CreateDeferredView(CreateDeferredViewPlan<'a>),
	CreateTransactionalView(CreateTransactionalViewPlan<'a>),
	CreateSchema(CreateSchemaPlan<'a>),
	CreateTable(CreateTablePlan<'a>),
	// Alter
	AlterSequence(AlterSequencePlan<'a>),
	// Mutate
	Delete(DeletePlan<'a>),
	Insert(InsertPlan<'a>),
	Update(UpdatePlan<'a>),

	// Query
	Aggregate(AggregateNode<'a>),
	Distinct(DistinctNode<'a>),
	Filter(FilterNode<'a>),
	JoinInner(JoinInnerNode<'a>),
	JoinLeft(JoinLeftNode<'a>),
	JoinNatural(JoinNaturalNode<'a>),
	Take(TakeNode<'a>),
	Sort(SortNode<'a>),
	Map(MapNode<'a>),
	Extend(ExtendNode<'a>),
	InlineData(InlineDataNode<'a>),
	TableScan(TableScanNode),
	TableVirtualScan(TableVirtualScanNode<'a>),
	ViewScan(ViewScanNode),
}

#[derive(Debug, Clone)]
pub struct CreateDeferredViewPlan<'a> {
	pub schema: SchemaDef,
	pub view: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Box<PhysicalPlan<'a>>,
}

#[derive(Debug, Clone)]
pub struct CreateTransactionalViewPlan<'a> {
	pub schema: SchemaDef,
	pub view: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Box<PhysicalPlan<'a>>,
}

#[derive(Debug, Clone)]
pub struct CreateSchemaPlan<'a> {
	pub schema: Fragment<'a>,
	pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan<'a> {
	pub schema: SchemaDef,
	pub table: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
}

#[derive(Debug, Clone)]
pub struct AlterSequencePlan<'a> {
	pub schema: Option<Fragment<'a>>,
	pub table: Fragment<'a>,
	pub column: Fragment<'a>,
	pub value: Expression<'a>,
}

#[derive(Debug, Clone)]
pub struct AggregateNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub by: Vec<Expression<'a>>,
	pub map: Vec<Expression<'a>>,
}

#[derive(Debug, Clone)]
pub struct DistinctNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub columns: Vec<Fragment<'a>>,
}

#[derive(Debug, Clone)]
pub struct FilterNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub conditions: Vec<Expression<'a>>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan<'a> {
	pub input: Option<Box<PhysicalPlan<'a>>>,
	pub schema: Option<Fragment<'a>>,
	pub table: Option<Fragment<'a>>,
}

#[derive(Debug, Clone)]
pub struct InsertPlan<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub schema: Option<Fragment<'a>>,
	pub table: Fragment<'a>,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub schema: Option<Fragment<'a>>,
	pub table: Option<Fragment<'a>>,
}

#[derive(Debug, Clone)]
pub struct JoinInnerNode<'a> {
	pub left: Box<PhysicalPlan<'a>>,
	pub right: Box<PhysicalPlan<'a>>,
	pub on: Vec<Expression<'a>>,
}

#[derive(Debug, Clone)]
pub struct JoinLeftNode<'a> {
	pub left: Box<PhysicalPlan<'a>>,
	pub right: Box<PhysicalPlan<'a>>,
	pub on: Vec<Expression<'a>>,
}

#[derive(Debug, Clone)]
pub struct JoinNaturalNode<'a> {
	pub left: Box<PhysicalPlan<'a>>,
	pub right: Box<PhysicalPlan<'a>>,
	pub join_type: JoinType,
}

#[derive(Debug, Clone)]
pub struct SortNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub by: Vec<SortKey>,
}

#[derive(Debug, Clone)]
pub struct MapNode<'a> {
	pub input: Option<Box<PhysicalPlan<'a>>>,
	pub map: Vec<Expression<'a>>,
}

#[derive(Debug, Clone)]
pub struct ExtendNode<'a> {
	pub input: Option<Box<PhysicalPlan<'a>>>,
	pub extend: Vec<Expression<'a>>,
}

#[derive(Debug, Clone)]
pub struct InlineDataNode<'a> {
	pub rows: Vec<Vec<AliasExpression<'a>>>,
}

#[derive(Debug, Clone)]
pub struct TableScanNode {
	pub schema: SchemaDef,
	pub table: TableDef,
}

#[derive(Debug, Clone)]
pub struct ViewScanNode {
	pub schema: SchemaDef,
	pub view: ViewDef,
}

#[derive(Debug, Clone)]
pub struct TableVirtualScanNode<'a> {
	pub schema: SchemaDef,
	pub table: Arc<TableVirtualDef>,
	pub pushdown_context: Option<TableVirtualPushdownContext<'a>>,
}

#[derive(Debug, Clone)]
pub struct TableVirtualPushdownContext<'a> {
	pub filters: Vec<Expression<'a>>,
	pub projections: Vec<Expression<'a>>,
	pub order_by: Vec<SortKey>,
	pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct TakeNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub take: usize,
}
