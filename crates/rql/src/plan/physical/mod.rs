// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

mod alter;
mod create;

pub use alter::{AlterFlowAction, AlterFlowNode, AlterTableNode, AlterViewNode};
use reifydb_catalog::{
	Catalog,
	store::{ringbuffer::create::RingBufferColumnToCreate, table::TableColumnToCreate, view::ViewColumnToCreate},
};
use reifydb_core::{
	JoinType, SortKey, WindowSize, WindowSlide, WindowType,
	interface::{
		ColumnDef, ColumnId, NamespaceDef, NamespaceId, TableDef, TableId,
		catalog::ColumnIndex,
		resolved::{
			ResolvedColumn, ResolvedDictionary, ResolvedFlow, ResolvedNamespace, ResolvedPrimitive,
			ResolvedRingBuffer, ResolvedSequence, ResolvedTable, ResolvedTableVirtual, ResolvedView,
		},
	},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::{
	Fragment, Type, TypeConstraint,
	diagnostic::{
		catalog::{dictionary_not_found, ringbuffer_not_found, table_not_found},
		function::internal_error,
	},
	return_error,
};
use tracing::instrument;

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

pub(crate) struct Compiler {
	pub catalog: Catalog,
}

#[instrument(name = "rql::compile::physical", level = "trace", skip(catalog, rx, logical))]
pub async fn compile_physical<T: IntoStandardTransaction>(
	catalog: &Catalog,
	rx: &mut T,
	logical: Vec<LogicalPlan>,
) -> crate::Result<Option<PhysicalPlan>> {
	Compiler {
		catalog: catalog.clone(),
	}
	.compile(rx, logical)
	.await
}

impl Compiler {
	pub async fn compile<T: IntoStandardTransaction>(
		&self,
		rx: &mut T,
		logical: Vec<LogicalPlan>,
	) -> crate::Result<Option<PhysicalPlan>> {
		if logical.is_empty() {
			return Ok(None);
		}

		let mut stack: Vec<PhysicalPlan> = Vec::new();
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
					stack.push(self.compile_create_namespace(rx, create)?);
				}

				LogicalPlan::CreateTable(create) => {
					stack.push(self.compile_create_table(rx, create).await?);
				}

				LogicalPlan::CreateRingBuffer(create) => {
					stack.push(self.compile_create_ringbuffer(rx, create).await?);
				}

				LogicalPlan::CreateFlow(create) => {
					stack.push(Box::pin(self.compile_create_flow(rx, create)).await?);
				}

				LogicalPlan::CreateDeferredView(create) => {
					stack.push(Box::pin(self.compile_create_deferred(rx, create)).await?);
				}

				LogicalPlan::CreateTransactionalView(create) => {
					stack.push(Box::pin(self.compile_create_transactional(rx, create)).await?);
				}

				LogicalPlan::CreateDictionary(create) => {
					stack.push(self.compile_create_dictionary(rx, create).await?);
				}

				LogicalPlan::AlterSequence(alter) => {
					stack.push(self.compile_alter_sequence(rx, alter).await?);
				}

				LogicalPlan::AlterTable(alter) => {
					stack.push(self.compile_alter_table(rx, alter)?);
				}

				LogicalPlan::AlterView(alter) => {
					stack.push(self.compile_alter_view(rx, alter)?);
				}

				LogicalPlan::AlterFlow(alter) => {
					stack.push(self.compile_alter_flow(rx, alter).await?);
				}

				LogicalPlan::Filter(filter) => {
					let input = stack.pop().unwrap(); // FIXME

					// Try to optimize rownum predicates for O(1)/O(k) access
					if let Some(predicate) = extract_row_predicate(&filter.condition) {
						// Check if input is a scan node we can optimize
						let source = match &input {
							PhysicalPlan::TableScan(scan) => {
								Some(ResolvedPrimitive::Table(scan.source.clone()))
							}
							PhysicalPlan::ViewScan(scan) => {
								Some(ResolvedPrimitive::View(scan.source.clone()))
							}
							PhysicalPlan::RingBufferScan(scan) => {
								Some(ResolvedPrimitive::RingBuffer(scan.source.clone()))
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
						let sub_plan = Box::pin(self.compile(rx, vec![*delete_input]))
							.await?
							.expect("Delete input must produce a plan");
						Some(Box::new(sub_plan))
					} else {
						stack.pop().map(|i| Box::new(i))
					};

					// Resolve the table if we have a target
					let target = if let Some(table_id) = delete.target {
						use reifydb_core::interface::resolved::{
							ResolvedNamespace, ResolvedTable,
						};

						let namespace_name = table_id
							.namespace
							.as_ref()
							.map(|n| n.text())
							.unwrap_or("default");
						let namespace_def = self
							.catalog
							.find_namespace_by_name(rx, namespace_name)
							.await?
							.unwrap();
						let Some(table_def) = self
							.catalog
							.find_table_by_name(rx, namespace_def.id, table_id.name.text())
							.await?
						else {
							return_error!(table_not_found(
								table_id.name.clone(),
								&namespace_def.name,
								table_id.name.text()
							));
						};

						let namespace_id = table_id.namespace.clone().unwrap_or_else(|| {
							use reifydb_type::Fragment;
							Fragment::internal(namespace_def.name.clone())
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
						let sub_plan = Box::pin(self.compile(rx, vec![*delete_input]))
							.await?
							.expect("Delete input must produce a plan");
						Some(Box::new(sub_plan))
					} else {
						stack.pop().map(|i| Box::new(i))
					};

					// Resolve the ring buffer
					use reifydb_core::interface::resolved::{
						ResolvedNamespace, ResolvedRingBuffer,
					};

					let ringbuffer_id = delete.target.clone();
					let namespace_name =
						ringbuffer_id.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, namespace_name).await?.unwrap();
					let Some(ringbuffer_def) = self
						.catalog
						.find_ringbuffer_by_name(
							rx,
							namespace_def.id,
							ringbuffer_id.name.text(),
						)
						.await?
					else {
						return_error!(ringbuffer_not_found(
							ringbuffer_id.name.clone(),
							&namespace_def.name,
							ringbuffer_id.name.text()
						));
					};

					let namespace_id = ringbuffer_id.namespace.clone().unwrap_or_else(|| {
						use reifydb_type::Fragment;
						Fragment::internal(namespace_def.name.clone())
					});
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						ringbuffer_id.name.clone(),
						resolved_namespace,
						ringbuffer_def,
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

					let table = insert.target.clone();
					let namespace_name =
						table.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, namespace_name).await?.unwrap();
					let Some(table_def) = self
						.catalog
						.find_table_by_name(rx, namespace_def.id, table.name.text())
						.await?
					else {
						return_error!(table_not_found(
							table.name.clone(),
							&namespace_def.name,
							table.name.text()
						));
					};

					let namespace_id = table.namespace.clone().unwrap_or_else(|| {
						use reifydb_type::Fragment;
						Fragment::internal(namespace_def.name.clone())
					});
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target =
						ResolvedTable::new(table.name.clone(), resolved_namespace, table_def);

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

					let ringbuffer_id = insert_rb.target.clone();
					let namespace_name =
						ringbuffer_id.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, namespace_name).await?.unwrap();
					let Some(ringbuffer_def) = self
						.catalog
						.find_ringbuffer_by_name(
							rx,
							namespace_def.id,
							ringbuffer_id.name.text(),
						)
						.await?
					else {
						return_error!(ringbuffer_not_found(
							ringbuffer_id.name.clone(),
							&namespace_def.name,
							ringbuffer_id.name.text()
						));
					};

					let namespace_id = ringbuffer_id.namespace.clone().unwrap_or_else(|| {
						use reifydb_type::Fragment;
						Fragment::internal(namespace_def.name.clone())
					});
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						ringbuffer_id.name.clone(),
						resolved_namespace,
						ringbuffer_def,
					);

					stack.push(PhysicalPlan::InsertRingBuffer(InsertRingBufferNode {
						input: Box::new(input),
						target,
					}))
				}

				LogicalPlan::InsertDictionary(insert_dict) => {
					let input = stack.pop().unwrap();

					// Resolve the dictionary
					let dictionary_id = insert_dict.target.clone();
					let namespace_name =
						dictionary_id.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, namespace_name).await?.unwrap();
					let Some(dictionary_def) = self
						.catalog
						.find_dictionary_by_name(
							rx,
							namespace_def.id,
							dictionary_id.name.text(),
						)
						.await?
					else {
						return_error!(dictionary_not_found(
							dictionary_id.name.clone(),
							&namespace_def.name,
							dictionary_id.name.text()
						));
					};

					let namespace_id = dictionary_id.namespace.clone().unwrap_or_else(|| {
						use reifydb_type::Fragment;
						Fragment::internal(namespace_def.name.clone())
					});
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedDictionary::new(
						dictionary_id.name.clone(),
						resolved_namespace,
						dictionary_def,
					);

					stack.push(PhysicalPlan::InsertDictionary(InsertDictionaryNode {
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
						let sub_plan = Box::pin(self.compile(rx, vec![*update_input]))
							.await?
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
						let namespace_def = self
							.catalog
							.find_namespace_by_name(rx, namespace_name)
							.await?
							.unwrap();
						let Some(table_def) = self
							.catalog
							.find_table_by_name(rx, namespace_def.id, table_id.name.text())
							.await?
						else {
							return_error!(table_not_found(
								table_id.name.clone(),
								&namespace_def.name,
								table_id.name.text()
							));
						};

						let namespace_id = table_id.namespace.clone().unwrap_or_else(|| {
							use reifydb_type::Fragment;
							Fragment::internal(namespace_def.name.clone())
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
						let sub_plan = Box::pin(self.compile(rx, vec![*update_input]))
							.await?
							.expect("UpdateRingBuffer input must produce a plan");
						Box::new(sub_plan)
					} else {
						Box::new(stack.pop().expect("UpdateRingBuffer requires input"))
					};

					// Resolve the ring buffer
					use reifydb_core::interface::resolved::{
						ResolvedNamespace, ResolvedRingBuffer,
					};

					let ringbuffer_id = update_rb.target.clone();
					let namespace_name =
						ringbuffer_id.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, namespace_name).await?.unwrap();
					let Some(ringbuffer_def) = self
						.catalog
						.find_ringbuffer_by_name(
							rx,
							namespace_def.id,
							ringbuffer_id.name.text(),
						)
						.await?
					else {
						return_error!(ringbuffer_not_found(
							ringbuffer_id.name.clone(),
							&namespace_def.name,
							ringbuffer_id.name.text()
						));
					};

					let namespace_id = ringbuffer_id.namespace.clone().unwrap_or_else(|| {
						use reifydb_type::Fragment;
						Fragment::internal(namespace_def.name.clone())
					});
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						ringbuffer_id.name.clone(),
						resolved_namespace,
						ringbuffer_def,
					);

					stack.push(PhysicalPlan::UpdateRingBuffer(UpdateRingBufferNode {
						input,
						target,
					}))
				}

				LogicalPlan::JoinInner(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = Box::pin(self.compile(rx, join.with)).await?.unwrap();
					stack.push(PhysicalPlan::JoinInner(JoinInnerNode {
						left: Box::new(left),
						right: Box::new(right),
						on: join.on,
						alias: join.alias,
					}));
				}

				LogicalPlan::JoinLeft(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = Box::pin(self.compile(rx, join.with)).await?.unwrap();
					stack.push(PhysicalPlan::JoinLeft(JoinLeftNode {
						left: Box::new(left),
						right: Box::new(right),
						on: join.on,
						alias: join.alias,
					}));
				}

				LogicalPlan::JoinNatural(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = Box::pin(self.compile(rx, join.with)).await?.unwrap();
					stack.push(PhysicalPlan::JoinNatural(JoinNaturalNode {
						left: Box::new(left),
						right: Box::new(right),
						join_type: join.join_type,
						alias: join.alias,
					}));
				}

				LogicalPlan::Merge(merge) => {
					let left = stack.pop().unwrap();
					let right = Box::pin(self.compile(rx, merge.with)).await?.unwrap();
					stack.push(PhysicalPlan::Merge(MergeNode {
						left: Box::new(left),
						right: Box::new(right),
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
								Fragment::internal("_context"),
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
								Fragment::internal("_context"),
								namespace,
								table_def,
							);

							let resolved_source = ResolvedPrimitive::Table(resolved_table);

							let column_def = ColumnDef {
								id: ColumnId(1),
								name: col.name.text().to_string(),
								constraint: TypeConstraint::unconstrained(Type::Utf8),
								policies: vec![],
								index: ColumnIndex(0),
								auto_increment: false,
								dictionary_id: None,
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
						operator: apply.operator,
						expressions: apply.arguments,
						input,
					}));
				}

				LogicalPlan::PrimitiveScan(scan) => {
					// Use resolved source directly - no
					// catalog lookup needed!
					use reifydb_core::interface::resolved::ResolvedPrimitive;

					match &scan.source {
						ResolvedPrimitive::Table(resolved_table) => {
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
						ResolvedPrimitive::View(resolved_view) => {
							// Views cannot use index directives
							if scan.index.is_some() {
								unimplemented!("views do not support indexes yet");
							}
							stack.push(ViewScan(ViewScanNode {
								source: resolved_view.clone(),
							}));
						}
						ResolvedPrimitive::DeferredView(resolved_view) => {
							// Deferred views cannot use index directives
							if scan.index.is_some() {
								unimplemented!("views do not support indexes yet");
							}
							// Note: DeferredView shares the same physical operator
							// as View We need to convert it to ResolvedView
							let view = ResolvedView::new(
								resolved_view.identifier().clone(),
								resolved_view.namespace().clone(),
								resolved_view.def().clone(),
							);
							stack.push(ViewScan(ViewScanNode {
								source: view,
							}));
						}
						ResolvedPrimitive::TransactionalView(resolved_view) => {
							// Transactional views cannot use index directives
							if scan.index.is_some() {
								unimplemented!("views do not support indexes yet");
							}
							// Note: TransactionalView shares the same physical
							// operator as View We need to convert it to
							// ResolvedView
							let view = ResolvedView::new(
								resolved_view.identifier().clone(),
								resolved_view.namespace().clone(),
								resolved_view.def().clone(),
							);
							stack.push(ViewScan(ViewScanNode {
								source: view,
							}));
						}

						ResolvedPrimitive::TableVirtual(resolved_virtual) => {
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
						ResolvedPrimitive::RingBuffer(resolved_ringbuffer) => {
							// Ring buffers cannot use index directives
							if scan.index.is_some() {
								unimplemented!(
									"ring buffers do not support indexes yet"
								);
							}
							stack.push(PhysicalPlan::RingBufferScan(RingBufferScanNode {
								source: resolved_ringbuffer.clone(),
							}));
						}
						ResolvedPrimitive::Flow(resolved_flow) => {
							// Flows cannot use index directives
							if scan.index.is_some() {
								unimplemented!("flows do not support indexes yet");
							}
							stack.push(PhysicalPlan::FlowScan(FlowScanNode {
								source: resolved_flow.clone(),
							}));
						}
						ResolvedPrimitive::Dictionary(resolved_dictionary) => {
							// Dictionaries cannot use index directives
							if scan.index.is_some() {
								unimplemented!("dictionaries do not support indexes");
							}
							stack.push(PhysicalPlan::DictionaryScan(DictionaryScanNode {
								source: resolved_dictionary.clone(),
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
					let pipeline_result = Box::pin(self.compile(rx, pipeline.steps)).await?;
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
									Box::pin(self.compile(rx, vec![logical_plan]))
										.await?
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
									Box::pin(self.compile(rx, vec![logical_plan]))
										.await?
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
						Box::pin(self.compile(rx, vec![*conditional_node.then_branch])).await?
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
							Box::pin(self.compile(rx, vec![*else_if.then_branch])).await?
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
						if let Some(plan) =
							Box::pin(self.compile(rx, vec![*else_logical])).await?
						{
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
					let input_plan = if let Some(plan) =
						Box::pin(self.compile(rx, vec![*scalarize_node.input])).await?
					{
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
pub enum PhysicalPlan {
	CreateDeferredView(CreateDeferredViewNode),
	CreateTransactionalView(CreateTransactionalViewNode),
	CreateNamespace(CreateNamespaceNode),
	CreateTable(CreateTableNode),
	CreateRingBuffer(CreateRingBufferNode),
	CreateFlow(CreateFlowNode),
	CreateDictionary(CreateDictionaryNode),
	// Alter
	AlterSequence(AlterSequenceNode),
	AlterTable(AlterTableNode),
	AlterView(AlterViewNode),
	AlterFlow(AlterFlowNode),
	// Mutate
	Delete(DeleteTableNode),
	DeleteRingBuffer(DeleteRingBufferNode),
	InsertTable(InsertTableNode),
	InsertRingBuffer(InsertRingBufferNode),
	InsertDictionary(InsertDictionaryNode),
	Update(UpdateTableNode),
	UpdateRingBuffer(UpdateRingBufferNode),
	// Variable assignment
	Declare(DeclareNode),
	Assign(AssignNode),
	// Variable resolution
	Variable(VariableNode),
	Environment(EnvironmentNode),
	// Control flow
	Conditional(ConditionalNode),

	// Query
	Aggregate(AggregateNode),
	Distinct(DistinctNode),
	Filter(FilterNode),
	IndexScan(IndexScanNode),
	// Row-number optimized access
	RowPointLookup(RowPointLookupNode),
	RowListLookup(RowListLookupNode),
	RowRangeScan(RowRangeScanNode),
	JoinInner(JoinInnerNode),
	JoinLeft(JoinLeftNode),
	JoinNatural(JoinNaturalNode),
	Merge(MergeNode),
	Take(TakeNode),
	Sort(SortNode),
	Map(MapNode),
	Extend(ExtendNode),
	Apply(ApplyNode),
	InlineData(InlineDataNode),
	TableScan(TableScanNode),
	TableVirtualScan(TableVirtualScanNode),
	ViewScan(ViewScanNode),
	RingBufferScan(RingBufferScanNode),
	FlowScan(FlowScanNode),
	DictionaryScan(DictionaryScanNode),
	Generator(GeneratorNode),
	Window(WindowNode),
	// Auto-scalarization for 1x1 frames
	Scalarize(ScalarizeNode),
}

#[derive(Debug, Clone)]
pub struct CreateDeferredViewNode {
	pub namespace: NamespaceDef, // FIXME REsolvedNamespace
	pub view: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: Box<PhysicalPlan>,
	pub primary_key: Option<logical::PrimaryKeyDef>,
}

#[derive(Debug, Clone)]
pub struct CreateFlowNode {
	pub namespace: NamespaceDef,
	pub flow: Fragment,
	pub if_not_exists: bool,
	pub as_clause: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct CreateTransactionalViewNode {
	pub namespace: NamespaceDef, // FIXME REsolvedNamespace
	pub view: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<ViewColumnToCreate>,
	pub as_clause: Box<PhysicalPlan>,
	pub primary_key: Option<logical::PrimaryKeyDef>,
}

#[derive(Debug, Clone)]
pub struct CreateNamespaceNode {
	pub namespace: Fragment,
	pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateTableNode {
	pub namespace: ResolvedNamespace,
	pub table: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<TableColumnToCreate>,
	pub primary_key: Option<logical::PrimaryKeyDef>,
}

#[derive(Debug, Clone)]
pub struct CreateRingBufferNode {
	pub namespace: ResolvedNamespace,
	pub ringbuffer: Fragment,
	pub if_not_exists: bool,
	pub columns: Vec<RingBufferColumnToCreate>,
	pub capacity: u64,
	pub primary_key: Option<logical::PrimaryKeyDef>,
}

#[derive(Debug, Clone)]
pub struct CreateDictionaryNode {
	pub namespace: NamespaceDef,
	pub dictionary: Fragment,
	pub if_not_exists: bool,
	pub value_type: Type,
	pub id_type: Type,
}

#[derive(Debug, Clone)]
pub struct AlterSequenceNode {
	pub sequence: ResolvedSequence,
	pub column: ResolvedColumn,
	pub value: Expression,
}

#[derive(Debug, Clone)]
pub enum LetValue {
	Expression(Expression),       // scalar/column expression
	Statement(Vec<PhysicalPlan>), // query pipeline as physical plans
}

impl std::fmt::Display for LetValue {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			LetValue::Expression(expr) => write!(f, "{}", expr),
			LetValue::Statement(plans) => write!(f, "Statement({} plans)", plans.len()),
		}
	}
}

#[derive(Debug, Clone)]
pub struct DeclareNode {
	pub name: Fragment,
	pub value: LetValue,
}

#[derive(Debug, Clone)]
pub enum AssignValue {
	Expression(Expression),       // scalar/column expression
	Statement(Vec<PhysicalPlan>), // query pipeline as physical plans
}

impl std::fmt::Display for AssignValue {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			AssignValue::Expression(expr) => write!(f, "{}", expr),
			AssignValue::Statement(plans) => write!(f, "Statement({} plans)", plans.len()),
		}
	}
}

#[derive(Debug, Clone)]
pub struct AssignNode {
	pub name: Fragment,
	pub value: AssignValue,
}

#[derive(Debug, Clone)]
pub struct VariableNode {
	pub variable_expr: VariableExpression,
}

#[derive(Debug, Clone)]
pub struct EnvironmentNode {}

#[derive(Debug, Clone)]
pub struct ConditionalNode {
	pub condition: Expression,
	pub then_branch: Box<PhysicalPlan>,
	pub else_ifs: Vec<ElseIfBranch>,
	pub else_branch: Option<Box<PhysicalPlan>>,
}

#[derive(Debug, Clone)]
pub struct ElseIfBranch {
	pub condition: Expression,
	pub then_branch: Box<PhysicalPlan>,
}

#[derive(Debug, Clone)]
pub struct ScalarizeNode {
	pub input: Box<PhysicalPlan>,
	pub fragment: Fragment,
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
	pub columns: Vec<ResolvedColumn>,
}

#[derive(Debug, Clone)]
pub struct FilterNode {
	pub input: Box<PhysicalPlan>,
	pub conditions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct DeleteTableNode {
	pub input: Option<Box<PhysicalPlan>>,
	pub target: Option<ResolvedTable>,
}

#[derive(Debug, Clone)]
pub struct InsertTableNode {
	pub input: Box<PhysicalPlan>,
	pub target: ResolvedTable,
}

#[derive(Debug, Clone)]
pub struct InsertRingBufferNode {
	pub input: Box<PhysicalPlan>,
	pub target: ResolvedRingBuffer,
}

#[derive(Debug, Clone)]
pub struct InsertDictionaryNode {
	pub input: Box<PhysicalPlan>,
	pub target: ResolvedDictionary,
}

#[derive(Debug, Clone)]
pub struct UpdateTableNode {
	pub input: Box<PhysicalPlan>,
	pub target: Option<ResolvedTable>,
}

#[derive(Debug, Clone)]
pub struct DeleteRingBufferNode {
	pub input: Option<Box<PhysicalPlan>>,
	pub target: ResolvedRingBuffer,
}

#[derive(Debug, Clone)]
pub struct UpdateRingBufferNode {
	pub input: Box<PhysicalPlan>,
	pub target: ResolvedRingBuffer,
}

#[derive(Debug, Clone)]
pub struct JoinInnerNode {
	pub left: Box<PhysicalPlan>,
	pub right: Box<PhysicalPlan>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
}

#[derive(Debug, Clone)]
pub struct JoinLeftNode {
	pub left: Box<PhysicalPlan>,
	pub right: Box<PhysicalPlan>,
	pub on: Vec<Expression>,
	pub alias: Option<Fragment>,
}

#[derive(Debug, Clone)]
pub struct JoinNaturalNode {
	pub left: Box<PhysicalPlan>,
	pub right: Box<PhysicalPlan>,
	pub join_type: JoinType,
	pub alias: Option<Fragment>,
}

#[derive(Debug, Clone)]
pub struct MergeNode {
	pub left: Box<PhysicalPlan>,
	pub right: Box<PhysicalPlan>,
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
pub struct ApplyNode {
	pub input: Option<Box<PhysicalPlan>>,
	pub operator: Fragment, // FIXME becomes OperatorIdentifier
	pub expressions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct InlineDataNode {
	pub rows: Vec<Vec<AliasExpression>>,
}

#[derive(Debug, Clone)]
pub struct IndexScanNode {
	pub source: ResolvedTable,
	pub index_name: String,
}

#[derive(Debug, Clone)]
pub struct TableScanNode {
	pub source: ResolvedTable,
}

#[derive(Debug, Clone)]
pub struct ViewScanNode {
	pub source: ResolvedView,
}

#[derive(Debug, Clone)]
pub struct RingBufferScanNode {
	pub source: ResolvedRingBuffer,
}

#[derive(Debug, Clone)]
pub struct FlowScanNode {
	pub source: ResolvedFlow,
}

#[derive(Debug, Clone)]
pub struct DictionaryScanNode {
	pub source: ResolvedDictionary,
}

#[derive(Debug, Clone)]
pub struct GeneratorNode {
	pub name: Fragment,
	pub expressions: Vec<Expression>,
}

#[derive(Debug, Clone)]
pub struct TableVirtualScanNode {
	pub source: ResolvedTableVirtual,
	pub pushdown_context: Option<TableVirtualPushdownContext>,
}

#[derive(Debug, Clone)]
pub struct TableVirtualPushdownContext {
	pub filters: Vec<Expression>,
	pub projections: Vec<Expression>,
	pub order_by: Vec<SortKey>,
	pub limit: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct TakeNode {
	pub input: Box<PhysicalPlan>,
	pub take: usize,
}

#[derive(Debug, Clone)]
pub struct WindowNode {
	pub input: Option<Box<PhysicalPlan>>,
	pub window_type: WindowType,
	pub size: WindowSize,
	pub slide: Option<WindowSlide>,
	pub group_by: Vec<Expression>,
	pub aggregations: Vec<Expression>,
	pub min_events: usize,
	pub max_window_count: Option<usize>,
	pub max_window_age: Option<std::time::Duration>,
}

/// O(1) point lookup by row number: `filter rownum == N`
#[derive(Debug, Clone)]
pub struct RowPointLookupNode {
	/// The source to look up in (table, ring buffer, etc.)
	pub source: ResolvedPrimitive,
	/// The row number to fetch
	pub row_number: u64,
}

/// O(k) list lookup by row numbers: `filter rownum in [a, b, c]`
#[derive(Debug, Clone)]
pub struct RowListLookupNode {
	/// The source to look up in
	pub source: ResolvedPrimitive,
	/// The row numbers to fetch
	pub row_numbers: Vec<u64>,
}

/// Range scan by row numbers: `filter rownum between X and Y`
#[derive(Debug, Clone)]
pub struct RowRangeScanNode {
	/// The source to scan
	pub source: ResolvedPrimitive,
	/// Start of the range (inclusive)
	pub start: u64,
	/// End of the range (inclusive)
	pub end: u64,
}
