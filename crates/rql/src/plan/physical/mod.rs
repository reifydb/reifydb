// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod alter;
mod create;

pub use alter::{AlterFlowAction, AlterFlowNode, AlterTableNode, AlterViewNode};
use reifydb_catalog::{
	CatalogStore,
	store::{ring_buffer::create::RingBufferColumnToCreate, table::TableColumnToCreate, view::ViewColumnToCreate},
};
use reifydb_core::{
	JoinType, SortKey, WindowSize, WindowSlide, WindowType,
	interface::{
		ColumnDef, ColumnId, NamespaceDef, NamespaceId, QueryTransaction, TableDef, TableId,
		catalog::ColumnIndex,
		resolved::{
			ResolvedColumn, ResolvedFlow, ResolvedNamespace, ResolvedRingBuffer, ResolvedSequence,
			ResolvedSource, ResolvedTable, ResolvedTableVirtual, ResolvedView,
		},
	},
};
use reifydb_type::{
	Fragment, Type, TypeConstraint,
	diagnostic::{
		catalog::{ring_buffer_not_found, table_not_found},
		function::internal_error,
	},
	return_error,
};

use crate::{
	expression::{AliasExpression, Expression, VariableExpression},
	plan::{
		logical,
		logical::{
			LogicalPlan,
			row_predicate::{RowPredicate, extract_row_predicate},
		},
		physical::PhysicalPlan::{IndexScan, TableScan, ViewScan},
	},
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

				LogicalPlan::CreateFlow(create) => {
					stack.push(Self::compile_create_flow(rx, create)?);
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

				LogicalPlan::AlterFlow(alter) => {
					stack.push(Self::compile_alter_flow(rx, alter)?);
				}

				LogicalPlan::Filter(filter) => {
					let input = stack.pop().unwrap(); // FIXME

					// Try to optimize rownum predicates for O(1)/O(k) access
					if let Some(predicate) = extract_row_predicate(&filter.condition) {
						// Check if input is a scan node we can optimize
						let source = match &input {
							PhysicalPlan::TableScan(scan) => {
								Some(ResolvedSource::Table(scan.source.clone()))
							}
							PhysicalPlan::ViewScan(scan) => {
								Some(ResolvedSource::View(scan.source.clone()))
							}
							PhysicalPlan::RingBufferScan(scan) => {
								Some(ResolvedSource::RingBuffer(scan.source.clone()))
							}
							_ => None,
						};

						if let Some(source) = source {
							match predicate {
								RowPredicate::Point(row_number) => {
									stack.push(PhysicalPlan::RowPointLookup(
										RowPointLookupNode {
											source,
											row_number,
										},
									));
									continue;
								}
								RowPredicate::List(row_numbers) => {
									stack.push(PhysicalPlan::RowListLookup(
										RowListLookupNode {
											source,
											row_numbers,
										},
									));
									continue;
								}
								RowPredicate::Range {
									start,
									end,
								} => {
									stack.push(PhysicalPlan::RowRangeScan(
										RowRangeScanNode {
											source,
											start,
											end,
										},
									));
									continue;
								}
							}
						}
					}

					// Default: generic filter
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

				LogicalPlan::Generator(generator) => {
					stack.push(PhysicalPlan::Generator(GeneratorNode {
						name: generator.name,
						expressions: generator.expressions,
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
						alias: join.alias,
					}));
				}

				LogicalPlan::JoinLeft(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = Self::compile(rx, join.with)?.unwrap();
					stack.push(PhysicalPlan::JoinLeft(JoinLeftNode {
						left: Box::new(left),
						right: Box::new(right),
						on: join.on,
						alias: join.alias,
					}));
				}

				LogicalPlan::JoinNatural(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = Self::compile(rx, join.with)?.unwrap();
					stack.push(PhysicalPlan::JoinNatural(JoinNaturalNode {
						left: Box::new(left),
						right: Box::new(right),
						join_type: join.join_type,
						alias: join.alias,
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

					// For now, create placeholder resolved columns
					// In a real implementation, this would resolve from the query context
					let resolved_columns = distinct
						.columns
						.into_iter()
						.map(|col| {
							// Create a placeholder resolved column
							let namespace = ResolvedNamespace::new(
								Fragment::owned_internal("_context"),
								NamespaceDef {
									id: NamespaceId(1),
									name: "_context".to_string(),
								},
							);

							let table_def = TableDef {
								id: TableId(1),
								namespace: NamespaceId(1),
								name: "_context".to_string(),
								columns: vec![],
								primary_key: None,
							};

							let resolved_table = ResolvedTable::new(
								Fragment::owned_internal("_context"),
								namespace,
								table_def,
							);

							let resolved_source = ResolvedSource::Table(resolved_table);

							let column_def = ColumnDef {
								id: ColumnId(1),
								name: col.name.text().to_string(),
								constraint: TypeConstraint::unconstrained(Type::Utf8),
								policies: vec![],
								index: ColumnIndex(0),
								auto_increment: false,
							};

							ResolvedColumn::new(col.name, resolved_source, column_def)
						})
						.collect();

					stack.push(PhysicalPlan::Distinct(DistinctNode {
						columns: resolved_columns,
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
						ResolvedSource::Flow(resolved_flow) => {
							// Flows cannot use index directives
							if scan.index.is_some() {
								unimplemented!("flows do not support indexes yet");
							}
							stack.push(PhysicalPlan::FlowScan(FlowScanNode {
								source: resolved_flow.clone(),
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

				LogicalPlan::Window(window) => {
					let input = stack.pop().map(Box::new);
					stack.push(PhysicalPlan::Window(WindowNode {
						window_type: window.window_type,
						size: window.size,
						slide: window.slide,
						group_by: window.group_by,
						aggregations: window.aggregations,
						min_events: window.min_events,
						max_window_count: window.max_window_count,
						max_window_age: window.max_window_age,
						input,
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

				LogicalPlan::Declare(declare_node) => {
					let value = match declare_node.value {
						logical::LetValue::Expression(expr) => LetValue::Expression(expr),
						logical::LetValue::Statement(logical_plans) => {
							// Compile the logical plans to physical plans
							let mut physical_plans = Vec::new();
							for logical_plan in logical_plans {
								if let Some(physical_plan) =
									Self::compile(rx, vec![logical_plan])?
								{
									physical_plans.push(physical_plan);
								}
							}
							LetValue::Statement(physical_plans)
						}
					};

					stack.push(PhysicalPlan::Declare(DeclareNode {
						name: declare_node.name,
						value,
						mutable: declare_node.mutable,
					}));
				}

				LogicalPlan::Assign(assign_node) => {
					let value = match assign_node.value {
						logical::AssignValue::Expression(expr) => AssignValue::Expression(expr),
						logical::AssignValue::Statement(logical_plans) => {
							// Compile the logical plans to physical plans
							let mut physical_plans = Vec::new();
							for logical_plan in logical_plans {
								if let Some(physical_plan) =
									Self::compile(rx, vec![logical_plan])?
								{
									physical_plans.push(physical_plan);
								}
							}
							AssignValue::Statement(physical_plans)
						}
					};

					stack.push(PhysicalPlan::Assign(AssignNode {
						name: assign_node.name,
						value,
					}));
				}

				LogicalPlan::VariableSource(source) => {
					// Create a variable expression to resolve at runtime
					let variable_expr = VariableExpression {
						fragment: source.name.clone(),
					};

					stack.push(PhysicalPlan::Variable(VariableNode {
						variable_expr,
					}));
				}

				LogicalPlan::Environment(_) => {
					stack.push(PhysicalPlan::Environment(EnvironmentNode {}));
				}

				LogicalPlan::Conditional(conditional_node) => {
					// Compile the then branch
					let then_branch = if let Some(then_plan) =
						Self::compile(rx, vec![*conditional_node.then_branch])?
					{
						Box::new(then_plan)
					} else {
						return Err(reifydb_type::Error(internal_error(
							"compile_physical".to_string(),
							"Failed to compile conditional then branch".to_string(),
						)));
					};

					// Compile else if branches
					let mut else_ifs = Vec::new();
					for else_if in conditional_node.else_ifs {
						let condition = else_if.condition;
						let then_branch = if let Some(plan) =
							Self::compile(rx, vec![*else_if.then_branch])?
						{
							Box::new(plan)
						} else {
							return Err(reifydb_type::Error(internal_error(
								"compile_physical".to_string(),
								"Failed to compile conditional else if branch"
									.to_string(),
							)));
						};
						else_ifs.push(ElseIfBranch {
							condition,
							then_branch,
						});
					}

					// Compile optional else branch
					let else_branch = if let Some(else_logical) = conditional_node.else_branch {
						if let Some(plan) = Self::compile(rx, vec![*else_logical])? {
							Some(Box::new(plan))
						} else {
							return Err(reifydb_type::Error(internal_error(
								"compile_physical".to_string(),
								"Failed to compile conditional else branch".to_string(),
							)));
						}
					} else {
						None
					};

					stack.push(PhysicalPlan::Conditional(ConditionalNode {
						condition: conditional_node.condition,
						then_branch,
						else_ifs,
						else_branch,
					}));
				}

				LogicalPlan::Scalarize(scalarize_node) => {
					// Compile the input plan
					let input_plan =
						if let Some(plan) = Self::compile(rx, vec![*scalarize_node.input])? {
							Box::new(plan)
						} else {
							return Err(reifydb_type::Error(internal_error(
								"compile_physical".to_string(),
								"Failed to compile scalarize input".to_string(),
							)));
						};

					stack.push(PhysicalPlan::Scalarize(ScalarizeNode {
						input: input_plan,
						fragment: scalarize_node.fragment,
					}));
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
	CreateFlow(CreateFlowNode<'a>),
	// Alter
	AlterSequence(AlterSequenceNode<'a>),
	AlterTable(AlterTableNode<'a>),
	AlterView(AlterViewNode<'a>),
	AlterFlow(AlterFlowNode<'a>),
	// Mutate
	Delete(DeleteTableNode<'a>),
	DeleteRingBuffer(DeleteRingBufferNode<'a>),
	InsertTable(InsertTableNode<'a>),
	InsertRingBuffer(InsertRingBufferNode<'a>),
	Update(UpdateTableNode<'a>),
	UpdateRingBuffer(UpdateRingBufferNode<'a>),
	// Variable assignment
	Declare(DeclareNode<'a>),
	Assign(AssignNode<'a>),
	// Variable resolution
	Variable(VariableNode<'a>),
	Environment(EnvironmentNode),
	// Control flow
	Conditional(ConditionalNode<'a>),

	// Query
	Aggregate(AggregateNode<'a>),
	Distinct(DistinctNode<'a>),
	Filter(FilterNode<'a>),
	IndexScan(IndexScanNode<'a>),
	// Row-number optimized access
	RowPointLookup(RowPointLookupNode<'a>),
	RowListLookup(RowListLookupNode<'a>),
	RowRangeScan(RowRangeScanNode<'a>),
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
	FlowScan(FlowScanNode<'a>),
	Generator(GeneratorNode<'a>),
	Window(WindowNode<'a>),
	// Auto-scalarization for 1x1 frames
	Scalarize(ScalarizeNode<'a>),
}

#[derive(Debug, Clone)]
pub struct CreateDeferredViewNode<'a> {
	pub namespace: NamespaceDef, // FIXME REsolvedNamespace
	pub view: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: Box<PhysicalPlan<'a>>,
}

#[derive(Debug, Clone)]
pub struct CreateFlowNode<'a> {
	pub namespace: NamespaceDef,
	pub flow: Fragment<'a>,
	pub if_not_exists: bool,
	pub as_clause: Box<PhysicalPlan<'a>>,
}

#[derive(Debug, Clone)]
pub struct CreateTransactionalViewNode<'a> {
	pub namespace: NamespaceDef, // FIXME REsolvedNamespace
	pub view: Fragment<'a>,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: Box<PhysicalPlan<'a>>,
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
	pub column: ResolvedColumn<'a>,
	pub value: Expression<'a>,
}

#[derive(Debug, Clone)]
pub enum LetValue<'a> {
	Expression(Expression<'a>),       // scalar/column expression
	Statement(Vec<PhysicalPlan<'a>>), // query pipeline as physical plans
}

impl<'a> std::fmt::Display for LetValue<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			LetValue::Expression(expr) => write!(f, "{}", expr),
			LetValue::Statement(plans) => write!(f, "Statement({} plans)", plans.len()),
		}
	}
}

#[derive(Debug, Clone)]
pub struct DeclareNode<'a> {
	pub name: Fragment<'a>,
	pub value: LetValue<'a>,
	pub mutable: bool,
}

#[derive(Debug, Clone)]
pub enum AssignValue<'a> {
	Expression(Expression<'a>),       // scalar/column expression
	Statement(Vec<PhysicalPlan<'a>>), // query pipeline as physical plans
}

impl<'a> std::fmt::Display for AssignValue<'a> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			AssignValue::Expression(expr) => write!(f, "{}", expr),
			AssignValue::Statement(plans) => write!(f, "Statement({} plans)", plans.len()),
		}
	}
}

#[derive(Debug, Clone)]
pub struct AssignNode<'a> {
	pub name: Fragment<'a>,
	pub value: AssignValue<'a>,
}

#[derive(Debug, Clone)]
pub struct VariableNode<'a> {
	pub variable_expr: VariableExpression<'a>,
}

#[derive(Debug, Clone)]
pub struct EnvironmentNode {}

#[derive(Debug, Clone)]
pub struct ConditionalNode<'a> {
	pub condition: Expression<'a>,
	pub then_branch: Box<PhysicalPlan<'a>>,
	pub else_ifs: Vec<ElseIfBranch<'a>>,
	pub else_branch: Option<Box<PhysicalPlan<'a>>>,
}

#[derive(Debug, Clone)]
pub struct ElseIfBranch<'a> {
	pub condition: Expression<'a>,
	pub then_branch: Box<PhysicalPlan<'a>>,
}

#[derive(Debug, Clone)]
pub struct ScalarizeNode<'a> {
	pub input: Box<PhysicalPlan<'a>>,
	pub fragment: Fragment<'a>,
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
	pub columns: Vec<ResolvedColumn<'a>>,
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
	pub alias: Option<Fragment<'a>>,
}

#[derive(Debug, Clone)]
pub struct JoinLeftNode<'a> {
	pub left: Box<PhysicalPlan<'a>>,
	pub right: Box<PhysicalPlan<'a>>,
	pub on: Vec<Expression<'a>>,
	pub alias: Option<Fragment<'a>>,
}

#[derive(Debug, Clone)]
pub struct JoinNaturalNode<'a> {
	pub left: Box<PhysicalPlan<'a>>,
	pub right: Box<PhysicalPlan<'a>>,
	pub join_type: JoinType,
	pub alias: Option<Fragment<'a>>,
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
pub struct FlowScanNode<'a> {
	pub source: ResolvedFlow<'a>,
}

#[derive(Debug, Clone)]
pub struct GeneratorNode<'a> {
	pub name: Fragment<'a>,
	pub expressions: Vec<Expression<'a>>,
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

#[derive(Debug, Clone)]
pub struct WindowNode<'a> {
	pub input: Option<Box<PhysicalPlan<'a>>>,
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: Vec<Expression<'a>>,
	pub aggregations: Vec<Expression<'a>>,
	pub min_events: usize,
	pub max_window_count: Option<usize>,
	pub max_window_age: Option<std::time::Duration>,
}

/// O(1) point lookup by row number: `filter rownum == N`
#[derive(Debug, Clone)]
pub struct RowPointLookupNode<'a> {
	/// The source to look up in (table, ring buffer, etc.)
	pub source: ResolvedSource<'a>,
	/// The row number to fetch
	pub row_number: u64,
}

/// O(k) list lookup by row numbers: `filter rownum in [a, b, c]`
#[derive(Debug, Clone)]
pub struct RowListLookupNode<'a> {
	/// The source to look up in
	pub source: ResolvedSource<'a>,
	/// The row numbers to fetch
	pub row_numbers: Vec<u64>,
}

/// Range scan by row numbers: `filter rownum between X and Y`
#[derive(Debug, Clone)]
pub struct RowRangeScanNode<'a> {
	/// The source to scan
	pub source: ResolvedSource<'a>,
	/// Start of the range (inclusive)
	pub start: u64,
	/// End of the range (inclusive)
	pub end: u64,
}
