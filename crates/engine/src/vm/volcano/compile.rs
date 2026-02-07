// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::vtable::{
	VTableContext,
	system::{
		cdc_consumers::CdcConsumers, column_policies::ColumnPolicies, columns::ColumnsTable,
		dictionaries::Dictionaries, dictionary_storage_stats::DictionaryStorageStats, flow_edges::FlowEdges,
		flow_lags::FlowLags, flow_node_storage_stats::FlowNodeStorageStats, flow_node_types::FlowNodeTypes,
		flow_nodes::FlowNodes, flow_operator_inputs::FlowOperatorInputs,
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
use reifydb_core::interface::catalog::id::{IndexId, NamespaceId};
use reifydb_rql::{
	nodes::{
		AggregateNode as RqlAggregateNode, ExtendNode as RqlExtendNode, FilterNode as RqlFilterNode,
		GeneratorNode as RqlGeneratorNode, InlineDataNode as RqlInlineDataNode,
		JoinInnerNode as RqlJoinInnerNode, JoinLeftNode as RqlJoinLeftNode,
		JoinNaturalNode as RqlJoinNaturalNode, MapNode as RqlMapNode, PatchNode as RqlPatchNode,
		RowListLookupNode as RqlRowListLookupNode, RowPointLookupNode as RqlRowPointLookupNode,
		RowRangeScanNode as RqlRowRangeScanNode, SortNode as RqlSortNode, TakeNode as RqlTakeNode,
	},
	query::QueryPlan as RqlQueryPlan,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::fragment::Fragment;
use tracing::instrument;

use crate::vm::volcano::{
	aggregate::AggregateNode,
	environment::EnvironmentNode,
	extend::{ExtendNode, ExtendWithoutInputNode},
	filter::FilterNode,
	generator::GeneratorNode,
	inline::InlineDataNode,
	join::{inner::InnerJoinNode, left::LeftJoinNode, natural::NaturalJoinNode},
	map::{MapNode, MapWithoutInputNode},
	patch::PatchNode,
	query::{QueryContext, QueryOperator},
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
		RqlQueryPlan::Filter(node) => extract_source_name_from_query(&node.input),
		RqlQueryPlan::Map(node) => node.input.as_ref().and_then(|p| extract_source_name_from_query(p)),
		RqlQueryPlan::Take(node) => extract_source_name_from_query(&node.input),
		_ => None,
	}
}

#[instrument(name = "volcano::compile", level = "trace", skip(plan, rx, context))]
pub(crate) fn compile<'a>(plan: RqlQueryPlan, rx: &mut Transaction<'a>, context: Arc<QueryContext>) -> QueryOperator {
	match plan {
		RqlQueryPlan::Aggregate(RqlAggregateNode {
			by,
			map,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context.clone()));
			QueryOperator::Aggregate(AggregateNode::new(input_node, by, map, context))
		}

		RqlQueryPlan::Filter(RqlFilterNode {
			conditions,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context));
			QueryOperator::Filter(FilterNode::new(input_node, conditions))
		}

		RqlQueryPlan::Take(RqlTakeNode {
			take,
			input,
		}) => {
			if let RqlQueryPlan::Sort(sort_node) = *input {
				let input_node = Box::new(compile(*sort_node.input, rx, context));
				return QueryOperator::TopK(TopKNode::new(input_node, sort_node.by, take));
			}
			let input_node = Box::new(compile(*input, rx, context));
			QueryOperator::Take(TakeNode::new(input_node, take))
		}

		RqlQueryPlan::Sort(RqlSortNode {
			by,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context));
			QueryOperator::Sort(SortNode::new(input_node, by))
		}

		RqlQueryPlan::Map(RqlMapNode {
			map,
			input,
		}) => {
			if let Some(input) = input {
				let input_node = Box::new(compile(*input, rx, context));
				QueryOperator::Map(MapNode::new(input_node, map))
			} else {
				QueryOperator::MapWithoutInput(MapWithoutInputNode::new(map))
			}
		}

		RqlQueryPlan::Extend(RqlExtendNode {
			extend,
			input,
		}) => {
			if let Some(input) = input {
				let input_node = Box::new(compile(*input, rx, context));
				QueryOperator::Extend(ExtendNode::new(input_node, extend))
			} else {
				QueryOperator::ExtendWithoutInput(ExtendWithoutInputNode::new(extend))
			}
		}

		RqlQueryPlan::Patch(RqlPatchNode {
			assignments,
			input,
		}) => {
			// Patch requires input - it merges with existing row
			let input = input.expect("Patch requires input");
			let input_node = Box::new(compile(*input, rx, context));
			QueryOperator::Patch(PatchNode::new(input_node, assignments))
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

			let left_node = Box::new(compile(*left, rx, context.clone()));
			let right_node = Box::new(compile(*right, rx, context.clone()));
			QueryOperator::InnerJoin(InnerJoinNode::new(left_node, right_node, on, effective_alias))
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

			let left_node = Box::new(compile(*left, rx, context.clone()));
			let right_node = Box::new(compile(*right, rx, context.clone()));
			QueryOperator::LeftJoin(LeftJoinNode::new(left_node, right_node, on, effective_alias))
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

			let left_node = Box::new(compile(*left, rx, context.clone()));
			let right_node = Box::new(compile(*right, rx, context.clone()));
			QueryOperator::NaturalJoin(NaturalJoinNode::new(left_node, right_node, join_type, effective_alias))
		}

		RqlQueryPlan::InlineData(RqlInlineDataNode {
			rows,
		}) => QueryOperator::InlineData(InlineDataNode::new(rows, context)),

		RqlQueryPlan::Generator(RqlGeneratorNode {
			name,
			expressions,
		}) => QueryOperator::Generator(GeneratorNode::new(name, expressions)),

		RqlQueryPlan::IndexScan(node) => {
			let table = node.source.def().clone();
			let Some(pk) = table.primary_key.clone() else {
				unimplemented!()
			};

			QueryOperator::IndexScan(IndexScanNode::new(table, IndexId::primary(pk.id), context).unwrap())
		}

		RqlQueryPlan::TableScan(node) => {
			QueryOperator::TableScan(TableScanNode::new(node.source.clone(), context, rx).unwrap())
		}

		RqlQueryPlan::ViewScan(node) => {
			QueryOperator::ViewScan(ViewScanNode::new(node.source.clone(), context).unwrap())
		}

		RqlQueryPlan::RingBufferScan(node) => {
			QueryOperator::RingBufferScan(RingBufferScan::new(node.source.clone(), context, rx).unwrap())
		}

		RqlQueryPlan::FlowScan(_node) => {
			// TODO: Implement FlowScan execution
			unimplemented!("FlowScan execution not yet implemented")
		}

		RqlQueryPlan::DictionaryScan(node) => {
			QueryOperator::DictionaryScan(DictionaryScanNode::new(node.source.clone(), context).unwrap())
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

			QueryOperator::VirtualScan(
				VirtualScanNode::new(virtual_table_impl, context, virtual_context).unwrap(),
			)
		}

		RqlQueryPlan::Variable(var_node) => QueryOperator::Variable(VariableNode::new(var_node.variable_expr)),

		RqlQueryPlan::Environment(_) => QueryOperator::Environment(EnvironmentNode::new()),

		RqlQueryPlan::Scalarize(scalarize_node) => {
			let input = compile(*scalarize_node.input, rx, context.clone());
			QueryOperator::Scalarize(ScalarizeNode::new(Box::new(input)))
		}

		RqlQueryPlan::Distinct(_) => unreachable!(),
		RqlQueryPlan::Apply(_) => {
			unimplemented!(
				"Apply operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
			)
		}
		RqlQueryPlan::Window(_) => {
			unimplemented!(
				"Window operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
			)
		}
		RqlQueryPlan::Merge(_) => {
			unimplemented!(
				"Merge operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
			)
		}

		// Row-number optimized access nodes
		RqlQueryPlan::RowPointLookup(RqlRowPointLookupNode {
			source,
			row_number,
		}) => {
			let resolved_source = reifydb_core::interface::resolved::ResolvedPrimitive::from(source);
			QueryOperator::RowPointLookup(
				RowPointLookupNode::new(resolved_source, row_number, context)
					.expect("Failed to create RowPointLookupNode"),
			)
		}
		RqlQueryPlan::RowListLookup(RqlRowListLookupNode {
			source,
			row_numbers,
		}) => {
			let resolved_source = reifydb_core::interface::resolved::ResolvedPrimitive::from(source);
			QueryOperator::RowListLookup(
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
			QueryOperator::RowRangeScan(
				RowRangeScanNode::new(resolved_source, start, end, context)
					.expect("Failed to create RowRangeScanNode"),
			)
		}
	}
}
