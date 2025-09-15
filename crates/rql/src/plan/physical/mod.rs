// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod alter;
mod create;

use std::sync::Arc;

pub use alter::{AlterTablePlan, AlterViewPlan};
use reifydb_catalog::{table::TableColumnToCreate, view::ViewColumnToCreate};
use reifydb_core::{
	JoinType, SortKey,
	interface::{
		NamespaceDef, QueryTransaction, TableDef, TableVirtualDef,
		ViewDef,
		evaluate::expression::{AliasExpression, Expression},
		identifier::{
			ColumnIdentifier, DeferredViewIdentifier,
			SequenceIdentifier, TableIdentifier,
			TransactionalViewIdentifier,
		},
	},
};
use reifydb_type::Fragment;

use crate::plan::{
	logical::LogicalPlan,
	physical::PhysicalPlan::{IndexScan, TableScan, ViewScan},
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

				LogicalPlan::CreateNamespace(create) => {
					stack.push(
						Self::compile_create_namespace(
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
							target: delete
								.target
								.clone(),
						},
					))
				}

				LogicalPlan::Insert(insert) => {
					let input = stack.pop().unwrap();
					stack.push(PhysicalPlan::Insert(
						InsertPlan {
							input: Box::new(input),
							target: insert
								.target
								.clone(),
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
							target: update
								.target
								.clone(),
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

				LogicalPlan::Apply(apply) => {
					let input = stack.pop().map(Box::new);
					stack.push(PhysicalPlan::Apply(
						ApplyNode {
							operator: apply
								.operator_name,
							expressions: apply
								.arguments,
							input,
						},
					));
				}

				LogicalPlan::SourceScan(scan) => {
					// Use resolved source directly - no
					// catalog lookup needed!
					use reifydb_core::interface::resolved::ResolvedSource;

					match scan.source.as_ref() {
                        ResolvedSource::Table(resolved_table) => {
                            let namespace = resolved_table.namespace.def.clone();
                            let table = resolved_table.def.clone();

                            // Check if an index was specified
                            if let Some(index) = &scan.index {
                                stack.push(IndexScan(
                                    IndexScanNode {
                                        namespace,
                                        table,
                                        index_name: index.identifier.name.text().to_string(),
                                    },
                                ));
                            } else {
                                stack.push(TableScan(
                                    TableScanNode {
                                        namespace,
                                        table,
                                    },
                                ));
                            }
                        }
                        ResolvedSource::View(resolved_view) => {
                            // Views cannot use index directives
                            if scan.index.is_some() {
                                unimplemented!("views do not support indexes yet");
                            }
                            let namespace = resolved_view.namespace.def.clone();
                            let view = resolved_view.def.clone();
                            stack.push(ViewScan(
                                ViewScanNode {
                                    namespace,
                                    view,
                                },
                            ));
                        }
                        ResolvedSource::DeferredView(resolved_view) => {
                            // Deferred views cannot use index directives
                            if scan.index.is_some() {
                                unimplemented!("views do not support indexes yet");
                            }
                            let namespace = resolved_view.namespace.def.clone();
                            let view = resolved_view.def.clone();
                            stack.push(ViewScan(
                                ViewScanNode {
                                    namespace,
                                    view,
                                },
                            ));
                        }
                        ResolvedSource::TransactionalView(resolved_view) => {
                            // Transactional views cannot use index directives
                            if scan.index.is_some() {
                                unimplemented!("views do not support indexes yet");
                            }
                            let namespace = resolved_view.namespace.def.clone();
                            let view = resolved_view.def.clone();
                            stack.push(ViewScan(
                                ViewScanNode {
                                    namespace,
                                    view,
                                },
                            ));
                        }

                        ResolvedSource::TableVirtual(resolved_virtual) => {
                            // Virtual tables cannot use index directives
                            if scan.index.is_some() {
                                unimplemented!("virtual tables do not support indexes yet");
                            }
                            let namespace = resolved_virtual.namespace.def.clone();
                            let table = Arc::new(resolved_virtual.def.clone());
                            stack.push(PhysicalPlan::TableVirtualScan(
                                TableVirtualScanNode {
                                    namespace,
                                    table,
                                    pushdown_context: None, // TODO: Detect pushdown opportunities
                                },
                            ));
                        }
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

				LogicalPlan::Pipeline(pipeline) => {
					// Compile the pipeline of operations
					// This ensures they all share the same
					// stack
					let pipeline_result = Self::compile(
						rx,
						pipeline.steps,
					)?;
					if let Some(result) = pipeline_result {
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
	CreateNamespace(CreateNamespacePlan<'a>),
	CreateTable(CreateTablePlan<'a>),
	// Alter
	AlterSequence(AlterSequencePlan<'a>),
	AlterTable(AlterTablePlan<'a>),
	AlterView(AlterViewPlan<'a>),
	// Mutate
	Delete(DeletePlan<'a>),
	Insert(InsertPlan<'a>),
	Update(UpdatePlan<'a>),

	// Query
	Aggregate(AggregateNode<'a>),
	Distinct(DistinctNode<'a>),
	Filter(FilterNode<'a>),
	IndexScan(IndexScanNode),
	JoinInner(JoinInnerNode<'a>),
	JoinLeft(JoinLeftNode<'a>),
	JoinNatural(JoinNaturalNode<'a>),
	Take(TakeNode<'a>),
	Sort(SortNode<'a>),
	Map(MapNode<'a>),
	Extend(ExtendNode<'a>),
	Apply(ApplyNode<'a>),
	InlineData(InlineDataNode<'a>),
	TableScan(TableScanNode),
	TableVirtualScan(TableVirtualScanNode<'a>),
	ViewScan(ViewScanNode),
}

#[derive(Debug, Clone)]
pub struct CreateDeferredViewPlan<'a> {
	pub namespace: NamespaceDef,
	pub view: DeferredViewIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Box<PhysicalPlan<'a>>,
}

#[derive(Debug, Clone)]
pub struct CreateTransactionalViewPlan<'a> {
	pub namespace: NamespaceDef,
	pub view: TransactionalViewIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Box<PhysicalPlan<'a>>,
}

#[derive(Debug, Clone)]
pub struct CreateNamespacePlan<'a> {
	pub namespace: Fragment<'a>,
	pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTablePlan<'a> {
	pub namespace: NamespaceDef,
	pub table: TableIdentifier<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
}

#[derive(Debug, Clone)]
pub struct AlterSequencePlan<'a> {
	pub sequence: SequenceIdentifier<'a>,
	pub column: ColumnIdentifier<'a>,
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
	pub columns: Vec<ColumnIdentifier<'a>>,
}

#[derive(Debug, Clone)]
pub struct FilterNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub conditions: Vec<Expression<'a>>,
}

#[derive(Debug, Clone)]
pub struct DeletePlan<'a> {
	pub input: Option<Box<PhysicalPlan<'a>>>,
	pub target: Option<TableIdentifier<'a>>,
}

#[derive(Debug, Clone)]
pub struct InsertPlan<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub target: TableIdentifier<'a>,
}

#[derive(Debug, Clone)]
pub struct UpdatePlan<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub target: Option<TableIdentifier<'a>>,
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
pub struct ApplyNode<'a> {
	pub input: Option<Box<PhysicalPlan<'a>>>,
	pub operator: Fragment<'a>, // FIXME becomes OperatorIdentifier
	pub expressions: Vec<Expression<'a>>,
}

#[derive(Debug, Clone)]
pub struct InlineDataNode<'a> {
	pub rows: Vec<Vec<AliasExpression<'a>>>,
}

#[derive(Debug, Clone)]
pub struct IndexScanNode {
	pub namespace: NamespaceDef,
	pub table: TableDef,
	pub index_name: String,
}

#[derive(Debug, Clone)]
pub struct TableScanNode {
	pub namespace: NamespaceDef,
	pub table: TableDef,
}

#[derive(Debug, Clone)]
pub struct ViewScanNode {
	pub namespace: NamespaceDef,
	pub view: ViewDef,
}

#[derive(Debug, Clone)]
pub struct TableVirtualScanNode<'a> {
	pub namespace: NamespaceDef,
	pub table: Arc<TableVirtualDef>,
	pub pushdown_context: Option<VirtualPushdownContext<'a>>,
}

#[derive(Debug, Clone)]
pub struct VirtualPushdownContext<'a> {
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
