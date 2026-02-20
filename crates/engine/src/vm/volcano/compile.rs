// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::vtable::{
	VTableContext,
	system::{
		cdc_consumers::CdcConsumers, column_policies::ColumnPolicies, columns::ColumnsTable,
		dictionaries::Dictionaries, dictionary_storage_stats::DictionaryStorageStats, enums::Enums,
		flow_edges::FlowEdges, flow_lags::FlowLags, flow_node_storage_stats::FlowNodeStorageStats,
		flow_node_types::FlowNodeTypes, flow_nodes::FlowNodes, flow_operator_inputs::FlowOperatorInputs,
		flow_operator_outputs::FlowOperatorOutputs, flow_operators::FlowOperators,
		flow_storage_stats::FlowStorageStats, flows::Flows, index_storage_stats::IndexStorageStats,
		namespaces::Namespaces, operator_retention_policies::OperatorRetentionPolicies,
		primary_key_columns::PrimaryKeyColumns, primary_keys::PrimaryKeys,
		primitive_retention_policies::PrimitiveRetentionPolicies,
		ringbuffer_storage_stats::RingBufferStorageStats, ringbuffers::RingBuffers,
		schema_fields::SchemaFields, schemas::Schemas, sequences::Sequences,
		table_storage_stats::TableStorageStats, tables::Tables, tables_virtual::TablesVirtual, types::Types,
		versions::Versions, view_storage_stats::ViewStorageStats, views::Views,
	},
	tables::VTables,
};
use reifydb_core::interface::{
	catalog::id::{IndexId, NamespaceId},
	resolved::ResolvedPrimitive,
};
use reifydb_rql::{
	expression::{AliasExpression, ConstantExpression, Expression, IdentExpression},
	nodes::{
		AggregateNode as RqlAggregateNode, AssertNode as RqlAssertNode, ExtendNode as RqlExtendNode,
		FilterNode as RqlFilterNode, GeneratorNode as RqlGeneratorNode, InlineDataNode as RqlInlineDataNode,
		JoinInnerNode as RqlJoinInnerNode, JoinLeftNode as RqlJoinLeftNode,
		JoinNaturalNode as RqlJoinNaturalNode, MapNode as RqlMapNode, PatchNode as RqlPatchNode,
		RowListLookupNode as RqlRowListLookupNode, RowPointLookupNode as RqlRowPointLookupNode,
		RowRangeScanNode as RqlRowRangeScanNode, SortNode as RqlSortNode, TakeNode as RqlTakeNode,
	},
	query::QueryPlan as RqlQueryPlan,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::constraint::Constraint};
use tracing::instrument;

use crate::vm::volcano::{
	aggregate::AggregateNode,
	assert::{AssertNode, AssertWithoutInputNode},
	distinct::DistinctNode,
	environment::EnvironmentNode,
	extend::{ExtendNode, ExtendWithoutInputNode},
	filter::FilterNode,
	generator::GeneratorNode,
	inline::InlineDataNode,
	join::{
		hash::{self, HashJoinNode},
		natural::NaturalJoinNode,
		nested_loop::NestedLoopJoinNode,
	},
	map::{MapNode, MapWithoutInputNode},
	patch::PatchNode,
	query::{QueryContext, QueryNode},
	row_lookup::{RowListLookupNode, RowPointLookupNode, RowRangeScanNode},
	scalarize::ScalarizeNode,
	scan::{
		dictionary::DictionaryScanNode, index::IndexScanNode, ringbuffer::RingBufferScan, table::TableScanNode,
		view::ViewScanNode, vtable::VirtualScanNode,
	},
	sort::SortNode,
	take::TakeNode,
	top_k::TopKNode,
	variable::VariableNode,
};

// Extract the source name from a query plan if it's a scan node
fn extract_source_name_from_query(plan: &RqlQueryPlan) -> Option<Fragment> {
	match plan {
		RqlQueryPlan::TableScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::ViewScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::RingBufferScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::DictionaryScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		// For other node types, try to recursively find the source
		RqlQueryPlan::Assert(node) => node.input.as_ref().and_then(|p| extract_source_name_from_query(p)),
		RqlQueryPlan::Filter(node) => extract_source_name_from_query(&node.input),
		RqlQueryPlan::Map(node) => node.input.as_ref().and_then(|p| extract_source_name_from_query(p)),
		RqlQueryPlan::Take(node) => extract_source_name_from_query(&node.input),
		_ => None,
	}
}

pub(crate) fn extract_resolved_source(plan: &RqlQueryPlan) -> Option<ResolvedPrimitive> {
	match plan {
		RqlQueryPlan::TableScan(node) => Some(ResolvedPrimitive::Table(node.source.clone())),
		RqlQueryPlan::ViewScan(node) => Some(ResolvedPrimitive::View(node.source.clone())),
		RqlQueryPlan::RingBufferScan(node) => Some(ResolvedPrimitive::RingBuffer(node.source.clone())),
		RqlQueryPlan::DictionaryScan(node) => Some(ResolvedPrimitive::Dictionary(node.source.clone())),
		RqlQueryPlan::Filter(node) => extract_resolved_source(&node.input),
		RqlQueryPlan::Assert(node) => node.input.as_ref().and_then(|p| extract_resolved_source(p)),
		RqlQueryPlan::Map(node) => node.input.as_ref().and_then(|p| extract_resolved_source(p)),
		RqlQueryPlan::Take(node) => extract_resolved_source(&node.input),
		RqlQueryPlan::Sort(node) => extract_resolved_source(&node.input),
		_ => None,
	}
}

/// Expand sumtype constructors and unit variant identifiers in UPDATE/PATCH assignments.
/// For UPDATE, we must explicitly null out non-active variant fields because PatchNode
/// only replaces columns that appear in the assignments.
fn expand_patch_sumtype_assignments(
	assignments: Vec<Expression>,
	source: &ResolvedPrimitive,
	catalog: &reifydb_catalog::catalog::Catalog,
	rx: &mut Transaction<'_>,
) -> Vec<Expression> {
	let mut expanded = Vec::with_capacity(assignments.len());

	for expr in assignments {
		let Expression::Alias(ref alias_expr) = expr else {
			expanded.push(expr);
			continue;
		};

		let col_name = alias_expr.alias.name().to_string();
		let tag_col_name = format!("{}_tag", col_name);

		// Check if this assignment targets a SumType column
		let tag_col = source.columns().iter().find(|c| c.name == tag_col_name);
		let sumtype_info = tag_col.and_then(|tc| {
			if let Some(Constraint::SumType(id)) = tc.constraint.constraint() {
				catalog.get_sumtype(rx, *id).ok().map(|def| (def, *id))
			} else {
				None
			}
		});

		let Some((sumtype_def, _)) = sumtype_info else {
			expanded.push(expr);
			continue;
		};

		let fragment = alias_expr.fragment.clone();

		match alias_expr.expression.as_ref() {
			Expression::SumTypeConstructor(ctor) => {
				let variant_name_lower = ctor.variant_name.text().to_lowercase();
				let variant = sumtype_def
					.variants
					.iter()
					.find(|v| v.name.to_lowercase() == variant_name_lower)
					.expect("variant not found in sumtype");

				// Tag column
				expanded.push(Expression::Alias(AliasExpression {
					alias: IdentExpression(Fragment::internal(format!("{}_tag", col_name))),
					expression: Box::new(Expression::Constant(ConstantExpression::Number {
						fragment: Fragment::internal(variant.tag.to_string()),
					})),
					fragment: fragment.clone(),
				}));

				// Build field lookup from constructor
				let field_map: std::collections::HashMap<String, &Expression> = ctor
					.columns
					.iter()
					.map(|(name, expr)| (name.text().to_lowercase(), expr))
					.collect();

				// All variant fields: active variant gets values, others get None
				for v in &sumtype_def.variants {
					for field in &v.fields {
						let phys_col_name = format!(
							"{}_{}_{}",
							col_name,
							v.name.to_lowercase(),
							field.name.to_lowercase()
						);
						let field_expr = if v.name.to_lowercase() == variant_name_lower {
							if let Some(e) = field_map.get(&field.name.to_lowercase()) {
								(*e).clone()
							} else {
								Expression::Constant(ConstantExpression::None {
									fragment: fragment.clone(),
								})
							}
						} else {
							Expression::Constant(ConstantExpression::None {
								fragment: fragment.clone(),
							})
						};
						expanded.push(Expression::Alias(AliasExpression {
							alias: IdentExpression(Fragment::internal(phys_col_name)),
							expression: Box::new(field_expr),
							fragment: fragment.clone(),
						}));
					}
				}
			}
			Expression::Column(col) => {
				// Check if bare identifier matches a unit variant
				let variant_name_lower = col.0.name.text().to_lowercase();
				if let Some(variant) = sumtype_def
					.variants
					.iter()
					.find(|v| v.name.to_lowercase() == variant_name_lower)
				{
					// Tag column
					expanded.push(Expression::Alias(AliasExpression {
						alias: IdentExpression(Fragment::internal(format!("{}_tag", col_name))),
						expression: Box::new(Expression::Constant(
							ConstantExpression::Number {
								fragment: Fragment::internal(variant.tag.to_string()),
							},
						)),
						fragment: fragment.clone(),
					}));

					// All variant fields set to None
					for v in &sumtype_def.variants {
						for field in &v.fields {
							let phys_col_name = format!(
								"{}_{}_{}",
								col_name,
								v.name.to_lowercase(),
								field.name.to_lowercase()
							);
							expanded.push(Expression::Alias(AliasExpression {
								alias: IdentExpression(Fragment::internal(
									phys_col_name,
								)),
								expression: Box::new(Expression::Constant(
									ConstantExpression::None {
										fragment: fragment.clone(),
									},
								)),
								fragment: fragment.clone(),
							}));
						}
					}
				} else {
					expanded.push(expr);
				}
			}
			_ => {
				expanded.push(expr);
			}
		}
	}

	expanded
}

#[instrument(name = "volcano::compile", level = "trace", skip(plan, rx, context))]
pub(crate) fn compile<'a>(
	plan: RqlQueryPlan,
	rx: &mut Transaction<'a>,
	context: Arc<QueryContext>,
) -> Box<dyn QueryNode> {
	match plan {
		RqlQueryPlan::Aggregate(RqlAggregateNode {
			by,
			map,
			input,
		}) => {
			let input_node = compile(*input, rx, context.clone());
			Box::new(AggregateNode::new(input_node, by, map, context))
		}

		RqlQueryPlan::Assert(RqlAssertNode {
			conditions,
			input,
			message,
		}) => {
			if let Some(input) = input {
				let input_node = compile(*input, rx, context);
				Box::new(AssertNode::new(input_node, conditions, message))
			} else {
				Box::new(AssertWithoutInputNode::new(conditions, message))
			}
		}

		RqlQueryPlan::Filter(RqlFilterNode {
			mut conditions,
			input,
		}) => {
			if let Some(source) = extract_resolved_source(&input) {
				for expr in &mut conditions {
					super::filter::resolve_is_variant_tags(
						expr,
						&source,
						&context.services.catalog,
						rx,
					)
					.expect("resolve IS variant tags");
				}
			}
			let input_node = compile(*input, rx, context);
			Box::new(FilterNode::new(input_node, conditions))
		}

		RqlQueryPlan::Take(RqlTakeNode {
			take,
			input,
		}) => {
			if let RqlQueryPlan::Sort(sort_node) = *input {
				let input_node = compile(*sort_node.input, rx, context);
				return Box::new(TopKNode::new(input_node, sort_node.by, take));
			}
			let input_node = compile(*input, rx, context);
			Box::new(TakeNode::new(input_node, take))
		}

		RqlQueryPlan::Sort(RqlSortNode {
			by,
			input,
		}) => {
			let input_node = compile(*input, rx, context);
			Box::new(SortNode::new(input_node, by))
		}

		RqlQueryPlan::Map(RqlMapNode {
			mut map,
			input,
		}) => {
			if let Some(input) = input {
				if let Some(source) = extract_resolved_source(&input) {
					for expr in &mut map {
						super::filter::resolve_is_variant_tags(
							expr,
							&source,
							&context.services.catalog,
							rx,
						)
						.expect("resolve IS variant tags in map");
					}
				}
				let input_node = compile(*input, rx, context);
				Box::new(MapNode::new(input_node, map))
			} else {
				Box::new(MapWithoutInputNode::new(map))
			}
		}

		RqlQueryPlan::Extend(RqlExtendNode {
			mut extend,
			input,
		}) => {
			if let Some(input) = input {
				if let Some(source) = extract_resolved_source(&input) {
					for expr in &mut extend {
						super::filter::resolve_is_variant_tags(
							expr,
							&source,
							&context.services.catalog,
							rx,
						)
						.expect("resolve IS variant tags in extend");
					}
				}
				let input_node = compile(*input, rx, context);
				Box::new(ExtendNode::new(input_node, extend))
			} else {
				Box::new(ExtendWithoutInputNode::new(extend))
			}
		}

		RqlQueryPlan::Patch(RqlPatchNode {
			mut assignments,
			input,
		}) => {
			// Patch requires input - it merges with existing row
			let input = input.expect("Patch requires input");

			// Expand sumtype constructors and unit variant identifiers in assignments
			if let Some(source) = extract_resolved_source(&input) {
				assignments = expand_patch_sumtype_assignments(
					assignments,
					&source,
					&context.services.catalog,
					rx,
				);
			}

			let input_node = compile(*input, rx, context);
			Box::new(PatchNode::new(input_node, assignments))
		}

		RqlQueryPlan::JoinInner(RqlJoinInnerNode {
			left,
			right,
			on,
			alias,
		}) => {
			// Extract source name from right plan for fallback alias
			let source_name = extract_source_name_from_query(&right);

			// Use explicit alias, or fall back to extracted source name, or use "other"
			let effective_alias =
				alias.or(source_name).or_else(|| Some(Fragment::internal("other".to_string())));

			let left_node = compile(*left, rx, context.clone());
			let right_node = compile(*right, rx, context.clone());

			let analysis = hash::extract_equi_keys(&on);
			if !analysis.equi_keys.is_empty() {
				Box::new(HashJoinNode::new_inner(left_node, right_node, analysis, effective_alias))
			} else {
				Box::new(NestedLoopJoinNode::new_inner(left_node, right_node, on, effective_alias))
			}
		}

		RqlQueryPlan::JoinLeft(RqlJoinLeftNode {
			left,
			right,
			on,
			alias,
		}) => {
			// Extract source name from right plan for fallback alias
			let source_name = extract_source_name_from_query(&right);

			// Use explicit alias, or fall back to extracted source name, or use "other"
			let effective_alias =
				alias.or(source_name).or_else(|| Some(Fragment::internal("other".to_string())));

			let left_node = compile(*left, rx, context.clone());
			let right_node = compile(*right, rx, context.clone());

			let analysis = hash::extract_equi_keys(&on);
			if !analysis.equi_keys.is_empty() {
				Box::new(HashJoinNode::new_left(left_node, right_node, analysis, effective_alias))
			} else {
				Box::new(NestedLoopJoinNode::new_left(left_node, right_node, on, effective_alias))
			}
		}

		RqlQueryPlan::JoinNatural(RqlJoinNaturalNode {
			left,
			right,
			join_type,
			alias,
		}) => {
			// Extract source name from right plan for fallback alias
			let source_name = extract_source_name_from_query(&right);
			// Use explicit alias, or fall back to extracted source name, or use "other"
			let effective_alias =
				alias.or(source_name).or_else(|| Some(Fragment::internal("other".to_string())));

			let left_node = compile(*left, rx, context.clone());
			let right_node = compile(*right, rx, context.clone());
			Box::new(NaturalJoinNode::new(left_node, right_node, join_type, effective_alias))
		}

		RqlQueryPlan::InlineData(RqlInlineDataNode {
			rows,
		}) => Box::new(InlineDataNode::new(rows, context)),

		RqlQueryPlan::Generator(RqlGeneratorNode {
			name,
			expressions,
		}) => Box::new(GeneratorNode::new(name, expressions)),

		RqlQueryPlan::IndexScan(node) => {
			let table = node.source.def().clone();
			let Some(pk) = table.primary_key.clone() else {
				unimplemented!()
			};

			Box::new(IndexScanNode::new(table, IndexId::primary(pk.id), context).unwrap())
		}

		RqlQueryPlan::TableScan(node) => {
			Box::new(TableScanNode::new(node.source.clone(), context, rx).unwrap())
		}

		RqlQueryPlan::ViewScan(node) => Box::new(ViewScanNode::new(node.source.clone(), context).unwrap()),

		RqlQueryPlan::RingBufferScan(node) => {
			Box::new(RingBufferScan::new(node.source.clone(), context, rx).unwrap())
		}

		RqlQueryPlan::FlowScan(_node) => {
			// TODO: Implement FlowScan execution
			unimplemented!("FlowScan execution not yet implemented")
		}

		RqlQueryPlan::DictionaryScan(node) => {
			Box::new(DictionaryScanNode::new(node.source.clone(), context).unwrap())
		}

		RqlQueryPlan::TableVirtualScan(node) => {
			// Create the appropriate virtual table implementation
			let namespace = node.source.namespace().def();
			let table = node.source.def();

			// First check user-defined virtual tables
			let virtual_table_impl: VTables = if let Some(user_table) =
				context.services.virtual_table_registry.find_by_name(namespace.id, &table.name)
			{
				// User-defined virtual table - registry returns VTableImpl directly
				user_table
			} else if namespace.id == NamespaceId(1) {
				// Built-in system virtual tables
				match table.name.as_str() {
					"sequences" => VTables::Sequences(Sequences::new()),
					"namespaces" => VTables::Namespaces(Namespaces::new()),
					"tables" => VTables::Tables(Tables::new()),
					"views" => VTables::Views(Views::new()),
					"flows" => VTables::Flows(Flows::new()),
					"flow_lags" => VTables::FlowLags(FlowLags::new(context.services.ioc.clone())),
					"flow_nodes" => VTables::FlowNodes(FlowNodes::new()),
					"flow_edges" => VTables::FlowEdges(FlowEdges::new()),
					"columns" => VTables::Columns(ColumnsTable::new()),
					"primary_keys" => VTables::PrimaryKeys(PrimaryKeys::new()),
					"primary_key_columns" => VTables::PrimaryKeyColumns(PrimaryKeyColumns::new()),
					"column_policies" => VTables::ColumnPolicies(ColumnPolicies::new()),
					"versions" => VTables::Versions(Versions::new(context.services.ioc.clone())),
					"primitive_retention_policies" => {
						VTables::PrimitiveRetentionPolicies(PrimitiveRetentionPolicies::new())
					}
					"operator_retention_policies" => {
						VTables::OperatorRetentionPolicies(OperatorRetentionPolicies::new())
					}
					"cdc_consumers" => VTables::CdcConsumers(CdcConsumers::new()),
					"flow_operators" => VTables::FlowOperators(FlowOperators::new(
						context.services.flow_operator_store.clone(),
					)),
					"dictionaries" => VTables::Dictionaries(Dictionaries::new()),
					"virtual_tables" => VTables::TablesVirtual(TablesVirtual::new(
						context.services.catalog.clone(),
					)),
					"types" => VTables::Types(Types::new()),
					"flow_node_types" => VTables::FlowNodeTypes(FlowNodeTypes::new()),
					"flow_operator_inputs" => VTables::FlowOperatorInputs(FlowOperatorInputs::new(
						context.services.flow_operator_store.clone(),
					)),
					"flow_operator_outputs" => VTables::FlowOperatorOutputs(
						FlowOperatorOutputs::new(context.services.flow_operator_store.clone()),
					),
					"ringbuffers" => VTables::RingBuffers(RingBuffers::new()),
					"table_storage_stats" => VTables::TableStorageStats(TableStorageStats::new(
						context.services.stats_reader.clone(),
					)),
					"view_storage_stats" => VTables::ViewStorageStats(ViewStorageStats::new(
						context.services.stats_reader.clone(),
					)),
					"flow_storage_stats" => VTables::FlowStorageStats(FlowStorageStats::new(
						context.services.stats_reader.clone(),
					)),
					"flow_node_storage_stats" => VTables::FlowNodeStorageStats(
						FlowNodeStorageStats::new(context.services.stats_reader.clone()),
					),
					"index_storage_stats" => VTables::IndexStorageStats(IndexStorageStats::new(
						context.services.stats_reader.clone(),
					)),
					"ringbuffer_storage_stats" => VTables::RingBufferStorageStats(
						RingBufferStorageStats::new(context.services.stats_reader.clone()),
					),
					"dictionary_storage_stats" => VTables::DictionaryStorageStats(
						DictionaryStorageStats::new(context.services.stats_reader.clone()),
					),
					"schemas" => VTables::Schemas(Schemas::new(context.services.catalog.clone())),
					"schema_fields" => VTables::SchemaFields(SchemaFields::new(
						context.services.catalog.clone(),
					)),
					"enums" => VTables::Enums(Enums::new()),
					_ => panic!("Unknown virtual table type: {}", table.name),
				}
			} else {
				panic!("Unknown virtual table type: {}.{}", namespace.name, table.name)
			};

			let virtual_context = node
				.pushdown_context
				.map(|ctx| VTableContext::PushDown {
					order_by: ctx.order_by,
					limit: ctx.limit,
					params: context.params.clone(),
				})
				.unwrap_or(VTableContext::Basic {
					params: context.params.clone(),
				});

			Box::new(VirtualScanNode::new(virtual_table_impl, context, virtual_context).unwrap())
		}

		RqlQueryPlan::Variable(var_node) => Box::new(VariableNode::new(var_node.variable_expr)),

		RqlQueryPlan::Environment(_) => Box::new(EnvironmentNode::new()),

		RqlQueryPlan::Scalarize(scalarize_node) => {
			let input = compile(*scalarize_node.input, rx, context.clone());
			Box::new(ScalarizeNode::new(input))
		}

		RqlQueryPlan::Distinct(distinct_node) => {
			let input = compile(*distinct_node.input, rx, context);
			Box::new(DistinctNode::new(input, distinct_node.columns))
		}
		RqlQueryPlan::Apply(apply_node) => {
			let operator_name = apply_node.operator.text().to_string();
			let transform = context
				.services
				.transforms
				.get_transform(&operator_name)
				.unwrap_or_else(|| panic!("Unknown transform: {}", operator_name));

			let input = apply_node.input.expect("Apply requires input");
			let input_node = compile(*input, rx, context);

			Box::new(super::apply_transform::ApplyTransformNode::new(input_node, transform))
		}
		RqlQueryPlan::Window(_) => {
			unimplemented!(
				"Window operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
			)
		}
		RqlQueryPlan::Append(_) => {
			unimplemented!(
				"Append operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
			)
		}

		// Row-number optimized access nodes
		RqlQueryPlan::RowPointLookup(RqlRowPointLookupNode {
			source,
			row_number,
		}) => {
			let resolved_source = reifydb_core::interface::resolved::ResolvedPrimitive::from(source);
			Box::new(
				RowPointLookupNode::new(resolved_source, row_number, context)
					.expect("Failed to create RowPointLookupNode"),
			)
		}
		RqlQueryPlan::RowListLookup(RqlRowListLookupNode {
			source,
			row_numbers,
		}) => {
			let resolved_source = reifydb_core::interface::resolved::ResolvedPrimitive::from(source);
			Box::new(
				RowListLookupNode::new(resolved_source, row_numbers, context)
					.expect("Failed to create RowListLookupNode"),
			)
		}
		RqlQueryPlan::RowRangeScan(RqlRowRangeScanNode {
			source,
			start,
			end,
		}) => {
			let resolved_source = reifydb_core::interface::resolved::ResolvedPrimitive::from(source);
			Box::new(
				RowRangeScanNode::new(resolved_source, start, end, context)
					.expect("Failed to create RowRangeScanNode"),
			)
		}
	}
}
