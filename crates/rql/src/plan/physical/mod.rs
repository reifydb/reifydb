// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod alter;
mod create;

pub use alter::{AlterTableNode, AlterViewNode};
use reifydb_catalog::{
	CatalogStore, ring_buffer::create::RingBufferColumnToCreate, table::TableColumnToCreate,
	view::ViewColumnToCreate,
};
use reifydb_core::{
	JoinType, SortKey,
	interface::{
		ColumnIdentifier, NamespaceDef, QueryTransaction,
		evaluate::expression::{AliasExpression, Expression},
		resolved::{
			ResolvedNamespace, ResolvedRingBuffer, ResolvedSequence, ResolvedTable, ResolvedTableVirtual,
			ResolvedView,
		},
	},
};
use reifydb_type::{
	Fragment,
	diagnostic::catalog::{ring_buffer_not_found, table_not_found},
	return_error,
};

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
					stack.push(PhysicalPlan::Aggregate(AggregateNode {
						by: aggregate.by,
						map: aggregate.map,
						input: Box::new(input),
					}));
				}

				LogicalPlan::CreateNamespace(create) => {
					stack.push(Self::compile_create_namespace(rx, create)?);
				}

				LogicalPlan::CreateTable(create) => {
					stack.push(Self::compile_create_table(rx, create)?);
				}

				LogicalPlan::CreateRingBuffer(create) => {
					stack.push(Self::compile_create_ring_buffer(rx, create)?);
				}

				LogicalPlan::CreateDeferredView(create) => {
					stack.push(Self::compile_create_deferred(rx, create)?);
				}

				LogicalPlan::CreateTransactionalView(create) => {
					stack.push(Self::compile_create_transactional(rx, create)?);
				}

				LogicalPlan::AlterSequence(alter) => {
					stack.push(Self::compile_alter_sequence(rx, alter)?);
				}

				LogicalPlan::AlterTable(alter) => {
					stack.push(Self::compile_alter_table(rx, alter)?);
				}

				LogicalPlan::AlterView(alter) => {
					stack.push(Self::compile_alter_view(rx, alter)?);
				}

				LogicalPlan::Filter(filter) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Filter(FilterNode {
						conditions: vec![filter.condition],
						input: Box::new(input),
					}));
				}

				LogicalPlan::InlineData(inline) => {
					stack.push(PhysicalPlan::InlineData(InlineDataNode {
						rows: inline.rows,
					}));
				}

				LogicalPlan::DeleteTable(delete) => {
					// If delete has its own input, compile it first
					// Otherwise, try to pop from stack (for pipeline operations)
					let input = if let Some(delete_input) = delete.input {
						// Recursively compile the input pipeline
						let sub_plan = Self::compile(rx, vec![*delete_input])?
							.expect("Delete input must produce a plan");
						Some(Box::new(sub_plan))
					} else {
						stack.pop().map(|i| Box::new(i))
					};

					// Resolve the table if we have a target
					let target = if let Some(table_id) = delete.target {
						use reifydb_catalog::CatalogStore;
						use reifydb_core::interface::resolved::{
							ResolvedNamespace, ResolvedTable,
						};

						let namespace_name = table_id
							.namespace
							.as_ref()
							.map(|n| n.text())
							.unwrap_or("default");
						let namespace_def =
							CatalogStore::find_namespace_by_name(rx, namespace_name)?
								.unwrap();
						let Some(table_def) = CatalogStore::find_table_by_name(
							rx,
							namespace_def.id,
							table_id.name.text(),
						)?
						else {
							return_error!(table_not_found(
								table_id.name.clone().into_owned(),
								&namespace_def.name,
								table_id.name.text()
							));
						};

						let namespace_id = table_id.namespace.clone().unwrap_or_else(|| {
							use reifydb_type::Fragment;
							Fragment::owned_internal(namespace_def.name.clone())
						});
						let resolved_namespace =
							ResolvedNamespace::new(namespace_id, namespace_def);
						Some(ResolvedTable::new(
							table_id.name.clone(),
							resolved_namespace,
							table_def,
						))
					} else {
						None
					};

					stack.push(PhysicalPlan::Delete(DeleteTableNode {
						input,
						target,
					}))
				}

				LogicalPlan::DeleteRingBuffer(delete) => {
					// If delete has its own input, compile it first
					// Otherwise, try to pop from stack (for pipeline operations)
					let input = if let Some(delete_input) = delete.input {
						// Recursively compile the input pipeline
						let sub_plan = Self::compile(rx, vec![*delete_input])?
							.expect("Delete input must produce a plan");
						Some(Box::new(sub_plan))
					} else {
						stack.pop().map(|i| Box::new(i))
					};

					// Resolve the ring buffer
					use reifydb_core::interface::resolved::{
						ResolvedNamespace, ResolvedRingBuffer,
					};

					let ring_buffer_id = delete.target.clone();
					let namespace_name = ring_buffer_id
						.namespace
						.as_ref()
						.map(|n| n.text())
						.unwrap_or("default");
					let namespace_def =
						CatalogStore::find_namespace_by_name(rx, namespace_name)?.unwrap();
					let Some(ring_buffer_def) = CatalogStore::find_ring_buffer_by_name(
						rx,
						namespace_def.id,
						ring_buffer_id.name.text(),
					)?
					else {
						return_error!(ring_buffer_not_found(
							ring_buffer_id.name.clone().into_owned(),
							&namespace_def.name,
							ring_buffer_id.name.text()
						));
					};

					let namespace_id = ring_buffer_id.namespace.clone().unwrap_or_else(|| {
						use reifydb_type::Fragment;
						Fragment::owned_internal(namespace_def.name.clone())
					});
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						ring_buffer_id.name.clone(),
						resolved_namespace,
						ring_buffer_def,
					);

					stack.push(PhysicalPlan::DeleteRingBuffer(DeleteRingBufferNode {
						input,
						target,
					}))
				}

				LogicalPlan::InsertTable(insert) => {
					let input = stack.pop().unwrap();

					// Resolve the table
					use reifydb_core::interface::resolved::{ResolvedNamespace, ResolvedTable};

					let table_id = insert.target.clone();
					let namespace_name =
						table_id.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						CatalogStore::find_namespace_by_name(rx, namespace_name)?.unwrap();
					let Some(table_def) = CatalogStore::find_table_by_name(
						rx,
						namespace_def.id,
						table_id.name.text(),
					)?
					else {
						return_error!(table_not_found(
							table_id.name.clone().into_owned(),
							&namespace_def.name,
							table_id.name.text()
						));
					};

					let namespace_id = table_id.namespace.clone().unwrap_or_else(|| {
						use reifydb_type::Fragment;
						Fragment::owned_internal(namespace_def.name.clone())
					});
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedTable::new(
						table_id.name.clone(),
						resolved_namespace,
						table_def,
					);

					stack.push(PhysicalPlan::InsertTable(InsertTableNode {
						input: Box::new(input),
						target,
					}))
				}

				LogicalPlan::InsertRingBuffer(insert_rb) => {
					let input = stack.pop().unwrap();

					// Resolve the ring buffer
					use reifydb_core::interface::resolved::{
						ResolvedNamespace, ResolvedRingBuffer,
					};

					let ring_buffer_id = insert_rb.target.clone();
					let namespace_name = ring_buffer_id
						.namespace
						.as_ref()
						.map(|n| n.text())
						.unwrap_or("default");
					let namespace_def =
						CatalogStore::find_namespace_by_name(rx, namespace_name)?.unwrap();
					let Some(ring_buffer_def) = CatalogStore::find_ring_buffer_by_name(
						rx,
						namespace_def.id,
						ring_buffer_id.name.text(),
					)?
					else {
						return_error!(ring_buffer_not_found(
							ring_buffer_id.name.clone().into_owned(),
							&namespace_def.name,
							ring_buffer_id.name.text()
						));
					};

					let namespace_id = ring_buffer_id.namespace.clone().unwrap_or_else(|| {
						use reifydb_type::Fragment;
						Fragment::owned_internal(namespace_def.name.clone())
					});
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						ring_buffer_id.name.clone(),
						resolved_namespace,
						ring_buffer_def,
					);

					stack.push(PhysicalPlan::InsertRingBuffer(InsertRingBufferNode {
						input: Box::new(input),
						target,
					}))
				}

				LogicalPlan::Update(update) => {
					// If update has its own input, compile
					// it first Otherwise, pop from
					// stack (for pipeline operations)
					let input = if let Some(update_input) = update.input {
						// Recursively compile
						// the input pipeline
						let sub_plan = Self::compile(rx, vec![*update_input])?
							.expect("Update input must produce a plan");
						Box::new(sub_plan)
					} else {
						Box::new(stack.pop().expect("Update requires input"))
					};

					// Resolve the table if we have a target
					let target = if let Some(table_id) = update.target {
						use reifydb_core::interface::resolved::{
							ResolvedNamespace, ResolvedTable,
						};

						let namespace_name = table_id
							.namespace
							.as_ref()
							.map(|n| n.text())
							.unwrap_or("default");
						let namespace_def =
							CatalogStore::find_namespace_by_name(rx, namespace_name)?
								.unwrap();
						let Some(table_def) = CatalogStore::find_table_by_name(
							rx,
							namespace_def.id,
							table_id.name.text(),
						)?
						else {
							return_error!(table_not_found(
								table_id.name.clone().into_owned(),
								&namespace_def.name,
								table_id.name.text()
							));
						};

						let namespace_id = table_id.namespace.clone().unwrap_or_else(|| {
							use reifydb_type::Fragment;
							Fragment::owned_internal(namespace_def.name.clone())
						});
						let resolved_namespace =
							ResolvedNamespace::new(namespace_id, namespace_def);
						Some(ResolvedTable::new(
							table_id.name.clone(),
							resolved_namespace,
							table_def,
						))
					} else {
						None
					};

					stack.push(PhysicalPlan::Update(UpdateTableNode {
						input,
						target,
					}))
				}

				LogicalPlan::UpdateRingBuffer(update_rb) => {
					// If update has its own input, compile
					// it first Otherwise, pop from
					// stack (for pipeline operations)
					let input = if let Some(update_input) = update_rb.input {
						// Recursively compile
						// the input pipeline
						let sub_plan = Self::compile(rx, vec![*update_input])?
							.expect("UpdateRingBuffer input must produce a plan");
						Box::new(sub_plan)
					} else {
						Box::new(stack.pop().expect("UpdateRingBuffer requires input"))
					};

					// Resolve the ring buffer
					use reifydb_core::interface::resolved::{
						ResolvedNamespace, ResolvedRingBuffer,
					};

					let ring_buffer_id = update_rb.target.clone();
					let namespace_name = ring_buffer_id
						.namespace
						.as_ref()
						.map(|n| n.text())
						.unwrap_or("default");
					let namespace_def =
						CatalogStore::find_namespace_by_name(rx, namespace_name)?.unwrap();
					let Some(ring_buffer_def) = CatalogStore::find_ring_buffer_by_name(
						rx,
						namespace_def.id,
						ring_buffer_id.name.text(),
					)?
					else {
						return_error!(ring_buffer_not_found(
							ring_buffer_id.name.clone().into_owned(),
							&namespace_def.name,
							ring_buffer_id.name.text()
						));
					};

					let namespace_id = ring_buffer_id.namespace.clone().unwrap_or_else(|| {
						use reifydb_type::Fragment;
						Fragment::owned_internal(namespace_def.name.clone())
					});
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						ring_buffer_id.name.clone(),
						resolved_namespace,
						ring_buffer_def,
					);

					stack.push(PhysicalPlan::UpdateRingBuffer(UpdateRingBufferNode {
						input,
						target,
					}))
				}

				LogicalPlan::JoinInner(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = Self::compile(rx, join.with)?.unwrap();
					stack.push(PhysicalPlan::JoinInner(JoinInnerNode {
						left: Box::new(left),
						right: Box::new(right),
						on: join.on,
					}));
				}

				LogicalPlan::JoinLeft(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = Self::compile(rx, join.with)?.unwrap();
					stack.push(PhysicalPlan::JoinLeft(JoinLeftNode {
						left: Box::new(left),
						right: Box::new(right),
						on: join.on,
					}));
				}

				LogicalPlan::JoinNatural(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = Self::compile(rx, join.with)?.unwrap();
					stack.push(PhysicalPlan::JoinNatural(JoinNaturalNode {
						left: Box::new(left),
						right: Box::new(right),
						join_type: join.join_type,
					}));
				}

				LogicalPlan::Order(order) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Sort(SortNode {
						by: order.by,
						input: Box::new(input),
					}));
				}

				LogicalPlan::Distinct(distinct) => {
					let input = stack.pop().unwrap(); // FIXME

					stack.push(PhysicalPlan::Distinct(DistinctNode {
						columns: distinct.columns,
						input: Box::new(input),
					}));
				}

				LogicalPlan::Map(map) => {
					let input = stack.pop().map(Box::new);
					stack.push(PhysicalPlan::Map(MapNode {
						map: map.map,
						input,
					}));
				}

				LogicalPlan::Extend(extend) => {
					let input = stack.pop().map(Box::new);
					stack.push(PhysicalPlan::Extend(ExtendNode {
						extend: extend.extend,
						input,
					}));
				}

				LogicalPlan::Apply(apply) => {
					let input = stack.pop().map(Box::new);
					stack.push(PhysicalPlan::Apply(ApplyNode {
						operator: apply.operator_name,
						expressions: apply.arguments,
						input,
					}));
				}

				LogicalPlan::SourceScan(scan) => {
					// Use resolved source directly - no
					// catalog lookup needed!
					use reifydb_core::interface::resolved::ResolvedSource;

					match &scan.source {
						ResolvedSource::Table(resolved_table) => {
							// Check if an index was specified
							if let Some(index) = &scan.index {
								stack.push(IndexScan(IndexScanNode {
									source: resolved_table.clone(),
									index_name: index
										.identifier()
										.text()
										.to_string(),
								}));
							} else {
								stack.push(TableScan(TableScanNode {
									source: resolved_table.clone(),
								}));
							}
						}
						ResolvedSource::View(resolved_view) => {
							// Views cannot use index directives
							if scan.index.is_some() {
								unimplemented!("views do not support indexes yet");
							}
							stack.push(ViewScan(ViewScanNode {
								source: resolved_view.clone(),
							}));
						}
						ResolvedSource::DeferredView(resolved_view) => {
							// Deferred views cannot use index directives
							if scan.index.is_some() {
								unimplemented!("views do not support indexes yet");
							}
							// Note: DeferredView shares the same physical operator as View
							// We need to convert it to ResolvedView
							let view = ResolvedView::new(
								resolved_view.identifier().clone(),
								resolved_view.namespace().clone(),
								resolved_view.def().clone(),
							);
							stack.push(ViewScan(ViewScanNode {
								source: view,
							}));
						}
						ResolvedSource::TransactionalView(resolved_view) => {
							// Transactional views cannot use index directives
							if scan.index.is_some() {
								unimplemented!("views do not support indexes yet");
							}
							// Note: TransactionalView shares the same physical operator as
							// View We need to convert it to ResolvedView
							let view = ResolvedView::new(
								resolved_view.identifier().clone(),
								resolved_view.namespace().clone(),
								resolved_view.def().clone(),
							);
							stack.push(ViewScan(ViewScanNode {
								source: view,
							}));
						}

						ResolvedSource::TableVirtual(resolved_virtual) => {
							// Virtual tables cannot use index directives
							if scan.index.is_some() {
								unimplemented!(
									"virtual tables do not support indexes yet"
								);
							}
							stack.push(PhysicalPlan::TableVirtualScan(
								TableVirtualScanNode {
									source: resolved_virtual.clone(),
									pushdown_context: None, /* TODO: Detect
									                         * pushdown opportunities */
								},
							));
						}
						ResolvedSource::RingBuffer(resolved_ring_buffer) => {
							// Ring buffers cannot use index directives
							if scan.index.is_some() {
								unimplemented!(
									"ring buffers do not support indexes yet"
								);
							}
							stack.push(PhysicalPlan::RingBufferScan(RingBufferScanNode {
								source: resolved_ring_buffer.clone(),
							}));
						}
					}
				}

				LogicalPlan::Take(take) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Take(TakeNode {
						take: take.take,
						input: Box::new(input),
					}));
				}

				LogicalPlan::Pipeline(pipeline) => {
					// Compile the pipeline of operations
					// This ensures they all share the same
					// stack
					let pipeline_result = Self::compile(rx, pipeline.steps)?;
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
			panic!("logical plan did not reduce to a single physical plan"); // FIXME
		}

		Ok(Some(stack.pop().unwrap()))
	}
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan<'a> {
	CreateDeferredView(CreateDeferredViewNode<'a>),
	CreateTransactionalView(CreateTransactionalViewNode<'a>),
	CreateNamespace(CreateNamespaceNode<'a>),
	CreateTable(CreateTableNode<'a>),
	CreateRingBuffer(CreateRingBufferNode<'a>),
	// Alter
	AlterSequence(AlterSequenceNode<'a>),
	AlterTable(AlterTableNode<'a>),
	AlterView(AlterViewNode<'a>),
	// Mutate
	Delete(DeleteTableNode<'a>),
	DeleteRingBuffer(DeleteRingBufferNode<'a>),
	InsertTable(InsertTableNode<'a>),
	InsertRingBuffer(InsertRingBufferNode<'a>),
	Update(UpdateTableNode<'a>),
	UpdateRingBuffer(UpdateRingBufferNode<'a>),

	// Query
	Aggregate(AggregateNode<'a>),
	Distinct(DistinctNode<'a>),
	Filter(FilterNode<'a>),
	IndexScan(IndexScanNode<'a>),
	JoinInner(JoinInnerNode<'a>),
	JoinLeft(JoinLeftNode<'a>),
	JoinNatural(JoinNaturalNode<'a>),
	Take(TakeNode<'a>),
	Sort(SortNode<'a>),
	Map(MapNode<'a>),
	Extend(ExtendNode<'a>),
	Apply(ApplyNode<'a>),
	InlineData(InlineDataNode<'a>),
	TableScan(TableScanNode<'a>),
	TableVirtualScan(TableVirtualScanNode<'a>),
	ViewScan(ViewScanNode<'a>),
	RingBufferScan(RingBufferScanNode<'a>),
}

#[derive(Debug, Clone)]
pub struct CreateDeferredViewNode<'a> {
	pub namespace: NamespaceDef, // FIXME REsolvedNamespace
	pub view: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Box<PhysicalPlan<'a>>,
}

#[derive(Debug, Clone)]
pub struct CreateTransactionalViewNode<'a> {
	pub namespace: NamespaceDef, // FIXME REsolvedNamespace
	pub view: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub with: Box<PhysicalPlan<'a>>,
}

#[derive(Debug, Clone)]
pub struct CreateNamespaceNode<'a> {
	pub namespace: Fragment<'a>,
	pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTableNode<'a> {
	pub namespace: ResolvedNamespace<'a>,
	pub table: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
}

#[derive(Debug, Clone)]
pub struct CreateRingBufferNode<'a> {
	pub namespace: ResolvedNamespace<'a>,
	pub ring_buffer: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
}

#[derive(Debug, Clone)]
pub struct AlterSequenceNode<'a> {
	pub sequence: ResolvedSequence<'a>,
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
pub struct DeleteTableNode<'a> {
	pub input: Option<Box<PhysicalPlan<'a>>>,
	pub target: Option<ResolvedTable<'a>>,
}

#[derive(Debug, Clone)]
pub struct InsertTableNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub target: ResolvedTable<'a>,
}

#[derive(Debug, Clone)]
pub struct InsertRingBufferNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub target: ResolvedRingBuffer<'a>,
}

#[derive(Debug, Clone)]
pub struct UpdateTableNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub target: Option<ResolvedTable<'a>>,
}

#[derive(Debug, Clone)]
pub struct DeleteRingBufferNode<'a> {
	pub input: Option<Box<PhysicalPlan<'a>>>,
	pub target: ResolvedRingBuffer<'a>,
}

#[derive(Debug, Clone)]
pub struct UpdateRingBufferNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub target: ResolvedRingBuffer<'a>,
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
pub struct IndexScanNode<'a> {
	pub source: ResolvedTable<'a>,
	pub index_name: String,
}

#[derive(Debug, Clone)]
pub struct TableScanNode<'a> {
	pub source: ResolvedTable<'a>,
}

#[derive(Debug, Clone)]
pub struct ViewScanNode<'a> {
	pub source: ResolvedView<'a>,
}

#[derive(Debug, Clone)]
pub struct RingBufferScanNode<'a> {
	pub source: ResolvedRingBuffer<'a>,
}

#[derive(Debug, Clone)]
pub struct TableVirtualScanNode<'a> {
	pub source: ResolvedTableVirtual<'a>,
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
