// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

pub mod alter;
pub mod create;

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	error::diagnostic::catalog::{dictionary_not_found, ringbuffer_not_found, table_not_found},
	interface::{
		catalog::{
			column::{ColumnDef, ColumnIndex},
			id::{ColumnId, NamespaceId, TableId},
			namespace::NamespaceDef,
			table::TableDef,
		},
		resolved::{
			ResolvedColumn, ResolvedDictionary, ResolvedNamespace, ResolvedPrimitive, ResolvedTable,
			ResolvedView,
		},
	},
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::{
	error::diagnostic::function::internal_error,
	fragment::Fragment,
	return_error,
	value::{constraint::TypeConstraint, r#type::Type},
};
use tracing::instrument;

use crate::{
	bump::BumpBox,
	expression::VariableExpression,
	nodes::{
		AggregateNode, AlterSequenceNode, ApplyNode, AssignNode, AssignValue, CallFunctionNode,
		ConditionalNode, CreateDeferredViewNode, CreateDictionaryNode, CreateFlowNode, CreateNamespaceNode,
		CreateRingBufferNode, CreateSubscriptionNode, CreateTableNode, CreateTransactionalViewNode,
		DeclareNode, DefineFunctionNode, DeleteRingBufferNode, DeleteTableNode, DictionaryScanNode,
		DistinctNode, ElseIfBranch, EnvironmentNode, ExtendNode, FilterNode, FlowScanNode, ForPhysicalNode,
		FunctionParameter, GeneratorNode, IndexScanNode, InlineDataNode, InsertDictionaryNode,
		InsertRingBufferNode, InsertTableNode, JoinInnerNode, JoinLeftNode, JoinNaturalNode, LetValue,
		LoopPhysicalNode, MapNode, MergeNode, PatchNode, PhysicalPlan, ReturnNode, RingBufferScanNode,
		RowListLookupNode, RowPointLookupNode, RowRangeScanNode, ScalarizeNode, SortNode, TableScanNode,
		TableVirtualScanNode, TakeNode, UpdateRingBufferNode, UpdateTableNode, VariableNode, ViewScanNode,
		WhilePhysicalNode, WindowNode,
	},
	plan::{
		logical,
		logical::{
			LogicalPlan,
			row_predicate::{RowPredicate, extract_row_predicate},
		},
	},
	query::QueryPlan,
};

pub(crate) struct Compiler {
	pub catalog: Catalog,
}

/// Helper to convert PhysicalPlan to QueryPlan for node inputs
fn to_query_plan(plan: PhysicalPlan) -> QueryPlan {
	plan.try_into().expect("node input must be a query plan")
}

/// Materialize a bump-allocated PrimaryKeyDef to an owned version
pub(crate) fn materialize_primary_key(
	pk: Option<crate::plan::logical::PrimaryKeyDef<'_>>,
) -> Option<crate::nodes::PrimaryKeyDef> {
	pk.map(|pk_def| crate::nodes::PrimaryKeyDef {
		columns: pk_def
			.columns
			.into_iter()
			.map(|col| crate::nodes::PrimaryKeyColumn {
				column: col.column.to_owned(),
				order: col.order,
			})
			.collect(),
	})
}

#[instrument(name = "rql::compile::physical", level = "trace", skip(catalog, rx, logical))]
pub fn compile_physical<'a, T: AsTransaction>(
	catalog: &Catalog,
	rx: &mut T,
	logical: impl IntoIterator<Item = LogicalPlan<'a>>,
) -> crate::Result<Option<PhysicalPlan>> {
	Compiler {
		catalog: catalog.clone(),
	}
	.compile(rx, logical)
}

impl Compiler {
	pub fn compile<'a, T: AsTransaction>(
		&self,
		rx: &mut T,
		logical: impl IntoIterator<Item = LogicalPlan<'a>>,
	) -> crate::Result<Option<PhysicalPlan>> {
		let mut stack: Vec<PhysicalPlan> = Vec::new();
		for plan in logical {
			match plan {
				LogicalPlan::Aggregate(aggregate) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Aggregate(AggregateNode {
						by: aggregate.by,
						map: aggregate.map,
						input: Box::new(to_query_plan(input)),
					}));
				}

				LogicalPlan::CreateNamespace(create) => {
					stack.push(self.compile_create_namespace(rx, create)?);
				}

				LogicalPlan::CreateTable(create) => {
					stack.push(self.compile_create_table(rx, create)?);
				}

				LogicalPlan::CreateRingBuffer(create) => {
					stack.push(self.compile_create_ringbuffer(rx, create)?);
				}

				LogicalPlan::CreateFlow(create) => {
					stack.push(self.compile_create_flow(rx, create)?);
				}

				LogicalPlan::CreateDeferredView(create) => {
					stack.push(self.compile_create_deferred(rx, create)?);
				}

				LogicalPlan::CreateTransactionalView(create) => {
					stack.push(self.compile_create_transactional(rx, create)?);
				}

				LogicalPlan::CreateDictionary(create) => {
					stack.push(self.compile_create_dictionary(rx, create)?);
				}

				LogicalPlan::CreateSubscription(create) => {
					stack.push(self.compile_create_subscription(rx, create)?);
				}

				LogicalPlan::AlterSequence(alter) => {
					stack.push(self.compile_alter_sequence(rx, alter)?);
				}

				LogicalPlan::AlterTable(alter) => {
					stack.push(self.compile_alter_table(rx, alter)?);
				}

				LogicalPlan::AlterView(alter) => {
					stack.push(self.compile_alter_view(rx, alter)?);
				}

				LogicalPlan::AlterFlow(alter) => {
					stack.push(self.compile_alter_flow(rx, alter)?);
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
						input: Box::new(to_query_plan(input)),
					}));
				}

				LogicalPlan::InlineData(inline) => {
					stack.push(PhysicalPlan::InlineData(InlineDataNode {
						rows: inline.rows,
					}));
				}

				LogicalPlan::Generator(generator) => {
					stack.push(PhysicalPlan::Generator(GeneratorNode {
						name: generator.name.to_owned(),
						expressions: generator.expressions,
					}));
				}

				LogicalPlan::DeleteTable(delete) => {
					// If delete has its own input, compile it first
					// Otherwise, try to pop from stack (for pipeline operations)
					let input = if let Some(delete_input) = delete.input {
						// Recursively compile the input pipeline
						let sub_plan = self
							.compile(
								rx,
								std::iter::once(BumpBox::into_inner(delete_input)),
							)?
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
							.find_namespace_by_name(rx, namespace_name)?
							.unwrap();
						let Some(table_def) = self.catalog.find_table_by_name(
							rx,
							namespace_def.id,
							table_id.name.text(),
						)?
						else {
							return_error!(table_not_found(
								table_id.name.to_owned(),
								&namespace_def.name,
								table_id.name.text()
							));
						};

						let namespace_id =
							table_id.namespace.map(|n| n.to_owned()).unwrap_or_else(|| {
								Fragment::internal(namespace_def.name.clone())
							});
						let resolved_namespace =
							ResolvedNamespace::new(namespace_id, namespace_def);
						Some(ResolvedTable::new(
							table_id.name.to_owned(),
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
						let sub_plan = self
							.compile(
								rx,
								std::iter::once(BumpBox::into_inner(delete_input)),
							)?
							.expect("Delete input must produce a plan");
						Some(Box::new(sub_plan))
					} else {
						stack.pop().map(|i| Box::new(i))
					};

					// Resolve the ring buffer
					use reifydb_core::interface::resolved::{
						ResolvedNamespace, ResolvedRingBuffer,
					};

					let ringbuffer_id = delete.target;
					let namespace_name =
						ringbuffer_id.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, namespace_name)?.unwrap();
					let Some(ringbuffer_def) = self.catalog.find_ringbuffer_by_name(
						rx,
						namespace_def.id,
						ringbuffer_id.name.text(),
					)?
					else {
						return_error!(ringbuffer_not_found(
							ringbuffer_id.name.to_owned(),
							&namespace_def.name,
							ringbuffer_id.name.text()
						));
					};

					let namespace_id = ringbuffer_id
						.namespace
						.map(|n| n.to_owned())
						.unwrap_or_else(|| Fragment::internal(namespace_def.name.clone()));
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						ringbuffer_id.name.to_owned(),
						resolved_namespace,
						ringbuffer_def,
					);

					stack.push(PhysicalPlan::DeleteRingBuffer(DeleteRingBufferNode {
						input,
						target,
					}))
				}

				LogicalPlan::InsertTable(insert) => {
					// Compile the source from the INSERT node
					let input = self
						.compile(rx, std::iter::once(BumpBox::into_inner(insert.source)))?
						.expect("Insert source must produce a plan");

					// Resolve the table
					use reifydb_core::interface::resolved::{ResolvedNamespace, ResolvedTable};

					let table = insert.target;
					let namespace_name =
						table.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, namespace_name)?.unwrap();
					let Some(table_def) = self.catalog.find_table_by_name(
						rx,
						namespace_def.id,
						table.name.text(),
					)?
					else {
						return_error!(table_not_found(
							table.name.to_owned(),
							&namespace_def.name,
							table.name.text()
						));
					};

					let namespace_id = table
						.namespace
						.map(|n| n.to_owned())
						.unwrap_or_else(|| Fragment::internal(namespace_def.name.clone()));
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedTable::new(
						table.name.to_owned(),
						resolved_namespace,
						table_def,
					);

					stack.push(PhysicalPlan::InsertTable(InsertTableNode {
						input: Box::new(input),
						target,
					}))
				}

				LogicalPlan::InsertRingBuffer(insert_rb) => {
					// Compile the source from the INSERT node
					let input = self
						.compile(rx, std::iter::once(BumpBox::into_inner(insert_rb.source)))?
						.expect("Insert source must produce a plan");

					// Resolve the ring buffer
					use reifydb_core::interface::resolved::{
						ResolvedNamespace, ResolvedRingBuffer,
					};

					let ringbuffer_id = insert_rb.target;
					let namespace_name =
						ringbuffer_id.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, namespace_name)?.unwrap();
					let Some(ringbuffer_def) = self.catalog.find_ringbuffer_by_name(
						rx,
						namespace_def.id,
						ringbuffer_id.name.text(),
					)?
					else {
						return_error!(ringbuffer_not_found(
							ringbuffer_id.name.to_owned(),
							&namespace_def.name,
							ringbuffer_id.name.text()
						));
					};

					let namespace_id = ringbuffer_id
						.namespace
						.map(|n| n.to_owned())
						.unwrap_or_else(|| Fragment::internal(namespace_def.name.clone()));
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						ringbuffer_id.name.to_owned(),
						resolved_namespace,
						ringbuffer_def,
					);

					stack.push(PhysicalPlan::InsertRingBuffer(InsertRingBufferNode {
						input: Box::new(input),
						target,
					}))
				}

				LogicalPlan::InsertDictionary(insert_dict) => {
					// Compile the source from the INSERT node
					let input = self
						.compile(rx, std::iter::once(BumpBox::into_inner(insert_dict.source)))?
						.expect("Insert source must produce a plan");

					// Resolve the dictionary
					let dictionary_id = insert_dict.target;
					let namespace_name =
						dictionary_id.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, namespace_name)?.unwrap();
					let Some(dictionary_def) = self.catalog.find_dictionary_by_name(
						rx,
						namespace_def.id,
						dictionary_id.name.text(),
					)?
					else {
						return_error!(dictionary_not_found(
							dictionary_id.name.to_owned(),
							&namespace_def.name,
							dictionary_id.name.text()
						));
					};

					let namespace_id = dictionary_id
						.namespace
						.map(|n| n.to_owned())
						.unwrap_or_else(|| Fragment::internal(namespace_def.name.clone()));
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedDictionary::new(
						dictionary_id.name.to_owned(),
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
						let sub_plan = self
							.compile(
								rx,
								std::iter::once(BumpBox::into_inner(update_input)),
							)?
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
							.find_namespace_by_name(rx, namespace_name)?
							.unwrap();
						let Some(table_def) = self.catalog.find_table_by_name(
							rx,
							namespace_def.id,
							table_id.name.text(),
						)?
						else {
							return_error!(table_not_found(
								table_id.name.to_owned(),
								&namespace_def.name,
								table_id.name.text()
							));
						};

						let namespace_id =
							table_id.namespace.map(|n| n.to_owned()).unwrap_or_else(|| {
								Fragment::internal(namespace_def.name.clone())
							});
						let resolved_namespace =
							ResolvedNamespace::new(namespace_id, namespace_def);
						Some(ResolvedTable::new(
							table_id.name.to_owned(),
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
						let sub_plan = self
							.compile(
								rx,
								std::iter::once(BumpBox::into_inner(update_input)),
							)?
							.expect("UpdateRingBuffer input must produce a plan");
						Box::new(sub_plan)
					} else {
						Box::new(stack.pop().expect("UpdateRingBuffer requires input"))
					};

					// Resolve the ring buffer
					use reifydb_core::interface::resolved::{
						ResolvedNamespace, ResolvedRingBuffer,
					};

					let ringbuffer_id = update_rb.target;
					let namespace_name =
						ringbuffer_id.namespace.as_ref().map(|n| n.text()).unwrap_or("default");
					let namespace_def =
						self.catalog.find_namespace_by_name(rx, namespace_name)?.unwrap();
					let Some(ringbuffer_def) = self.catalog.find_ringbuffer_by_name(
						rx,
						namespace_def.id,
						ringbuffer_id.name.text(),
					)?
					else {
						return_error!(ringbuffer_not_found(
							ringbuffer_id.name.to_owned(),
							&namespace_def.name,
							ringbuffer_id.name.text()
						));
					};

					let namespace_id = ringbuffer_id
						.namespace
						.map(|n| n.to_owned())
						.unwrap_or_else(|| Fragment::internal(namespace_def.name.clone()));
					let resolved_namespace = ResolvedNamespace::new(namespace_id, namespace_def);
					let target = ResolvedRingBuffer::new(
						ringbuffer_id.name.to_owned(),
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
					let right = self.compile(rx, join.with)?.unwrap();
					stack.push(PhysicalPlan::JoinInner(JoinInnerNode {
						left: Box::new(to_query_plan(left)),
						right: Box::new(to_query_plan(right)),
						on: join.on,
						alias: join.alias.map(|a| a.to_owned()),
					}));
				}

				LogicalPlan::JoinLeft(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = self.compile(rx, join.with)?.unwrap();
					stack.push(PhysicalPlan::JoinLeft(JoinLeftNode {
						left: Box::new(to_query_plan(left)),
						right: Box::new(to_query_plan(right)),
						on: join.on,
						alias: join.alias.map(|a| a.to_owned()),
					}));
				}

				LogicalPlan::JoinNatural(join) => {
					let left = stack.pop().unwrap(); // FIXME;
					let right = self.compile(rx, join.with)?.unwrap();
					stack.push(PhysicalPlan::JoinNatural(JoinNaturalNode {
						left: Box::new(to_query_plan(left)),
						right: Box::new(to_query_plan(right)),
						join_type: join.join_type,
						alias: join.alias.map(|a| a.to_owned()),
					}));
				}

				LogicalPlan::Merge(merge) => {
					let left = stack.pop().unwrap();
					let right = self.compile(rx, merge.with)?.unwrap();
					stack.push(PhysicalPlan::Merge(MergeNode {
						left: Box::new(to_query_plan(left)),
						right: Box::new(to_query_plan(right)),
					}));
				}

				LogicalPlan::Order(order) => {
					let input = stack.pop().unwrap(); // FIXME
					stack.push(PhysicalPlan::Sort(SortNode {
						by: order.by,
						input: Box::new(to_query_plan(input)),
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

							ResolvedColumn::new(
								col.name.to_owned(),
								resolved_source,
								column_def,
							)
						})
						.collect();

					stack.push(PhysicalPlan::Distinct(DistinctNode {
						columns: resolved_columns,
						input: Box::new(to_query_plan(input)),
					}));
				}

				LogicalPlan::Map(map) => {
					let input = stack.pop().map(|p| Box::new(to_query_plan(p)));
					stack.push(PhysicalPlan::Map(MapNode {
						map: map.map,
						input,
					}));
				}

				LogicalPlan::Extend(extend) => {
					let input = stack.pop().map(|p| Box::new(to_query_plan(p)));
					stack.push(PhysicalPlan::Extend(ExtendNode {
						extend: extend.extend,
						input,
					}));
				}

				LogicalPlan::Patch(patch) => {
					let input = stack.pop().map(|p| Box::new(to_query_plan(p)));
					stack.push(PhysicalPlan::Patch(PatchNode {
						assignments: patch.assignments,
						input,
					}));
				}

				LogicalPlan::Apply(apply) => {
					let input = stack.pop().map(|p| Box::new(to_query_plan(p)));
					stack.push(PhysicalPlan::Apply(ApplyNode {
						operator: apply.operator.to_owned(),
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
								stack.push(PhysicalPlan::IndexScan(IndexScanNode {
									source: resolved_table.clone(),
									index_name: index
										.identifier()
										.text()
										.to_string(),
								}));
							} else {
								stack.push(PhysicalPlan::TableScan(TableScanNode {
									source: resolved_table.clone(),
								}));
							}
						}
						ResolvedPrimitive::View(resolved_view) => {
							// Views cannot use index directives
							if scan.index.is_some() {
								unimplemented!("views do not support indexes yet");
							}
							stack.push(PhysicalPlan::ViewScan(ViewScanNode {
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
							stack.push(PhysicalPlan::ViewScan(ViewScanNode {
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
							stack.push(PhysicalPlan::ViewScan(ViewScanNode {
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
						input: Box::new(to_query_plan(input)),
					}));
				}

				LogicalPlan::Window(window) => {
					let input = stack.pop().map(|p| Box::new(to_query_plan(p)));
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
					let pipeline_result = self.compile(rx, pipeline.steps)?;
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
									self.compile(rx, std::iter::once(logical_plan))?
								{
									physical_plans.push(physical_plan);
								}
							}
							LetValue::Statement(physical_plans)
						}
					};

					stack.push(PhysicalPlan::Declare(DeclareNode {
						name: declare_node.name.to_owned(),
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
									self.compile(rx, std::iter::once(logical_plan))?
								{
									physical_plans.push(physical_plan);
								}
							}
							AssignValue::Statement(physical_plans)
						}
					};

					stack.push(PhysicalPlan::Assign(AssignNode {
						name: assign_node.name.to_owned(),
						value,
					}));
				}

				LogicalPlan::VariableSource(source) => {
					// Create a variable expression to resolve at runtime
					let variable_expr = VariableExpression {
						fragment: source.name.to_owned(),
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
					let then_branch = if let Some(then_plan) = self.compile(
						rx,
						std::iter::once(BumpBox::into_inner(conditional_node.then_branch)),
					)? {
						Box::new(then_plan)
					} else {
						return Err(reifydb_type::error::Error(internal_error(
							"compile_physical".into(),
							"Failed to compile conditional then branch".to_string(),
						)));
					};

					// Compile else if branches
					let mut else_ifs = Vec::new();
					for else_if in conditional_node.else_ifs {
						let condition = else_if.condition;
						let then_branch = if let Some(plan) = self.compile(
							rx,
							std::iter::once(BumpBox::into_inner(else_if.then_branch)),
						)? {
							Box::new(plan)
						} else {
							return Err(reifydb_type::error::Error(internal_error(
								"compile_physical".into(),
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
						if let Some(plan) = self.compile(
							rx,
							std::iter::once(BumpBox::into_inner(else_logical)),
						)? {
							Some(Box::new(plan))
						} else {
							return Err(reifydb_type::error::Error(internal_error(
								"compile_physical".into(),
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
					let input_plan = if let Some(plan) = self.compile(
						rx,
						std::iter::once(BumpBox::into_inner(scalarize_node.input)),
					)? {
						Box::new(to_query_plan(plan))
					} else {
						return Err(reifydb_type::error::Error(internal_error(
							"compile_physical".into(),
							"Failed to compile scalarize input".to_string(),
						)));
					};

					stack.push(PhysicalPlan::Scalarize(ScalarizeNode {
						input: input_plan,
						fragment: scalarize_node.fragment.to_owned(),
					}));
				}

				LogicalPlan::Loop(loop_node) => {
					let mut body = Vec::new();
					for statement_plans in loop_node.body {
						for logical_plan in statement_plans {
							if let Some(physical_plan) =
								self.compile(rx, std::iter::once(logical_plan))?
							{
								body.push(physical_plan);
							}
						}
					}
					stack.push(PhysicalPlan::Loop(LoopPhysicalNode {
						body,
					}));
				}

				LogicalPlan::While(while_node) => {
					let mut body = Vec::new();
					for statement_plans in while_node.body {
						for logical_plan in statement_plans {
							if let Some(physical_plan) =
								self.compile(rx, std::iter::once(logical_plan))?
							{
								body.push(physical_plan);
							}
						}
					}
					stack.push(PhysicalPlan::While(WhilePhysicalNode {
						condition: while_node.condition,
						body,
					}));
				}

				LogicalPlan::For(for_node) => {
					let iterable = self
						.compile(rx, for_node.iterable)?
						.expect("For iterable must produce a plan");
					let mut body = Vec::new();
					for statement_plans in for_node.body {
						for logical_plan in statement_plans {
							if let Some(physical_plan) =
								self.compile(rx, std::iter::once(logical_plan))?
							{
								body.push(physical_plan);
							}
						}
					}
					stack.push(PhysicalPlan::For(ForPhysicalNode {
						variable_name: for_node.variable_name.to_owned(),
						iterable: Box::new(iterable),
						body,
					}));
				}

				LogicalPlan::Break => {
					stack.push(PhysicalPlan::Break);
				}

				LogicalPlan::Continue => {
					stack.push(PhysicalPlan::Continue);
				}

				LogicalPlan::DefineFunction(def_node) => {
					// Convert parameters
					let parameters: Vec<FunctionParameter> = def_node
						.parameters
						.into_iter()
						.map(|p| FunctionParameter {
							name: p.name.to_owned(),
							type_constraint: p.type_constraint,
						})
						.collect();

					// Compile the body
					let mut body = Vec::new();
					for statement_plans in def_node.body {
						for logical_plan in statement_plans {
							if let Some(physical_plan) =
								self.compile(rx, std::iter::once(logical_plan))?
							{
								body.push(physical_plan);
							}
						}
					}

					stack.push(PhysicalPlan::DefineFunction(DefineFunctionNode {
						name: def_node.name.to_owned(),
						parameters,
						return_type: def_node.return_type,
						body,
					}));
				}

				LogicalPlan::Return(ret_node) => {
					stack.push(PhysicalPlan::Return(ReturnNode {
						value: ret_node.value,
					}));
				}

				LogicalPlan::CallFunction(call_node) => {
					stack.push(PhysicalPlan::CallFunction(CallFunctionNode {
						name: call_node.name.to_owned(),
						arguments: call_node.arguments,
					}));
				}

				_ => unimplemented!(),
			}
		}

		if stack.is_empty() {
			return Ok(None);
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
