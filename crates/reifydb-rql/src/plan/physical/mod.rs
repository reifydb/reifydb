// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod alter;
mod create;

use reifydb_catalog::{
	CatalogQueryTransaction, table::TableColumnToCreate,
	view::ViewColumnToCreate,
};
use reifydb_core::{
	JoinType, OwnedFragment, SortKey,
	interface::{
		QueryTransaction, SchemaDef, TableDef, ViewDef,
		evaluate::expression::{AliasExpression, Expression},
	},
	result::error::diagnostic::catalog::{
		schema_not_found, table_not_found,
	},
	return_error,
};

use crate::plan::{
	logical::LogicalPlan,
	physical::{
		PhysicalPlan::{TableScan, ViewScan},
		alter::{AlterTablePlan, AlterViewPlan},
	},
};

struct Compiler {}

pub fn compile_physical<T>(
	rx: &mut T,
	logical: Vec<LogicalPlan>,
) -> crate::Result<Option<PhysicalPlan>>
where
	T: QueryTransaction + CatalogQueryTransaction,
{
	Compiler::compile(rx, logical)
}

impl Compiler {
	fn compile<T>(
		rx: &mut T,
		logical: Vec<LogicalPlan>,
	) -> crate::Result<Option<PhysicalPlan>>
	where
		T: QueryTransaction + CatalogQueryTransaction,
	{
		if logical.is_empty() {
			return Ok(None);
		}

		let mut stack: Vec<PhysicalPlan> = Vec::new();
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

				LogicalPlan::AlterTable(alter) => {
					stack.push(Self::compile_alter_table(
						rx, alter,
					)?);
				}

				LogicalPlan::AlterView(alter) => {
					stack.push(Self::compile_alter_view(
						rx, alter,
					)?);
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
					let Some(schema) = rx
						.find_schema_by_name(
							&scan.schema.fragment(),
						)?
					else {
						return_error!(
							schema_not_found(
								Some(scan
									.schema
									.clone(
									)),
								&scan.schema
									.fragment(
									)
							)
						);
					};

					if let Some(table) = rx
						.find_table_by_name(
							schema.id,
							&scan.source.fragment(),
						)? {
						stack.push(TableScan(
							TableScanNode {
								schema,
								table,
							},
						));
					} else if let Some(view) = rx
						.find_view_by_name(
							schema.id,
							&scan.source.fragment(),
						)? {
						stack.push(ViewScan(
							ViewScanNode {
								schema,
								view,
							},
						));
					} else {
						return_error!(table_not_found(
							Some(scan
								.source
								.clone()),
							&scan.schema.fragment(),
							&scan.source.fragment()
						));
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
pub enum PhysicalPlan {
	CreateDeferredView(CreateDeferredViewPlan),
	CreateTransactionalView(CreateTransactionalViewPlan),
	CreateSchema(CreateSchemaPlan),
	CreateTable(CreateTablePlan),
	// Alter
	AlterSequence(AlterSequencePlan),
	AlterTable(AlterTablePlan),
	AlterView(AlterViewPlan),
	// Mutate
	Delete(DeletePlan),
	Insert(InsertPlan),
	Update(UpdatePlan),

	// Query
	Aggregate(AggregateNode),
	Distinct(DistinctNode),
	Filter(FilterNode),
	JoinInner(JoinInnerNode),
	JoinLeft(JoinLeftNode),
	JoinNatural(JoinNaturalNode),
	Take(TakeNode),
	Sort(SortNode),
	Map(MapNode),
	Extend(ExtendNode),
	InlineData(InlineDataNode),
	TableScan(TableScanNode),
	ViewScan(ViewScanNode),
}

#[derive(Debug, Clone)]
pub struct CreateDeferredViewPlan {
	pub schema: SchemaDef,
	pub view: OwnedFragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct CreateTransactionalViewPlan {
	pub schema: SchemaDef,
	pub view: OwnedFragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct CreateSchemaPlan {
	pub schema: OwnedFragment,
	pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan {
	pub schema: SchemaDef,
	pub table: OwnedFragment,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
}

#[derive(Debug, Clone)]
pub struct AlterSequencePlan {
	pub schema: Option<OwnedFragment>,
	pub table: OwnedFragment,
	pub column: OwnedFragment,
	pub value: Expression,
}

#[derive(Debug, Clone)]
pub struct AggregateNode {
	pub input: Box<PhysicalPlan>,
	pub by: Vec<Expression>,
	pub map: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct DistinctNode {
	pub input: Box<PhysicalPlan>,
	pub columns: Vec<OwnedFragment>,
}

#[derive(Debug, Clone)]
pub struct FilterNode {
	pub input: Box<PhysicalPlan>,
	pub conditions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan {
	pub input: Option<Box<PhysicalPlan>>,
	pub schema: Option<OwnedFragment>,
	pub table: Option<OwnedFragment>,
}

#[derive(Debug, Clone)]
pub struct InsertPlan {
	pub input: Box<PhysicalPlan>,
	pub schema: Option<OwnedFragment>,
	pub table: OwnedFragment,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan {
	pub input: Box<PhysicalPlan>,
	pub schema: Option<OwnedFragment>,
	pub table: Option<OwnedFragment>,
}

#[derive(Debug, Clone)]
pub struct JoinInnerNode {
	pub left: Box<PhysicalPlan>,
	pub right: Box<PhysicalPlan>,
	pub on: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct JoinLeftNode {
	pub left: Box<PhysicalPlan>,
	pub right: Box<PhysicalPlan>,
	pub on: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct JoinNaturalNode {
	pub left: Box<PhysicalPlan>,
	pub right: Box<PhysicalPlan>,
	pub join_type: JoinType,
}

#[derive(Debug, Clone)]
pub struct SortNode {
	pub input: Box<PhysicalPlan>,
	pub by: Vec<SortKey>,
}

#[derive(Debug, Clone)]
pub struct MapNode {
	pub input: Option<Box<PhysicalPlan>>,
	pub map: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct ExtendNode {
	pub input: Option<Box<PhysicalPlan>>,
	pub extend: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct InlineDataNode {
	pub rows: Vec<Vec<AliasExpression>>,
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
pub struct TakeNode {
	pub input: Box<PhysicalPlan>,
	pub take: usize,
}
