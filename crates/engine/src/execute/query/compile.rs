// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use async_recursion::async_recursion;
use reifydb_catalog::vtable::{
	VTableContext, VTables,
	system::{
		CdcConsumers, ColumnPolicies, ColumnsTable, Dictionaries, DictionaryStorageStats, FlowEdges, FlowLags,
		FlowNodeStorageStats, FlowNodeTypes, FlowNodes, FlowOperatorInputs, FlowOperatorOutputs, FlowOperators,
		FlowStorageStats, Flows, IndexStorageStats, Namespaces, OperatorRetentionPolicies, PrimaryKeyColumns,
		PrimaryKeys, PrimitiveRetentionPolicies, RingBufferStorageStats, RingBuffers, Sequences,
		TableStorageStats, Tables, TablesVirtual, Types, Versions, ViewStorageStats, Views,
	},
};
use reifydb_core::interface::{IndexId, NamespaceId};
use reifydb_rql::plan::{physical, physical::PhysicalPlan};
use reifydb_type::Fragment;
use tracing::instrument;

use crate::{
	StandardTransaction,
	execute::{
		ExecutionContext, ExecutionPlan,
		query::{
			aggregate::AggregateNode,
			assign::AssignNode,
			conditional::ConditionalNode,
			declare::DeclareNode,
			dictionary_scan::DictionaryScanNode,
			environment::EnvironmentNode,
			extend::{ExtendNode, ExtendWithoutInputNode},
			filter::FilterNode,
			generator::GeneratorNode,
			index_scan::IndexScanNode,
			inline::InlineDataNode,
			join::{InnerJoinNode, LeftJoinNode, NaturalJoinNode},
			map::{MapNode, MapWithoutInputNode},
			ringbuffer_scan::RingBufferScan,
			row_lookup::{RowListLookupNode, RowPointLookupNode, RowRangeScanNode},
			scalarize::ScalarizeNode,
			sort::SortNode,
			table_scan::TableScanNode,
			take::TakeNode,
			top_k::TopKNode,
			variable::VariableNode,
			view_scan::ViewScanNode,
			vtable_scan::VirtualScanNode,
		},
	},
};

// Extract the source name from a physical plan if it's a scan node
fn extract_source_name_from_physical<'a>(plan: &PhysicalPlan) -> Option<Fragment> {
	match plan {
		PhysicalPlan::TableScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		PhysicalPlan::ViewScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		PhysicalPlan::RingBufferScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		PhysicalPlan::DictionaryScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		// For other node types, try to recursively find the source
		PhysicalPlan::Filter(node) => extract_source_name_from_physical(&node.input),
		PhysicalPlan::Map(node) => node.input.as_ref().and_then(|p| extract_source_name_from_physical(p)),
		PhysicalPlan::Take(node) => extract_source_name_from_physical(&node.input),
		_ => None,
	}
}

#[async_recursion]
#[instrument(name = "query::compile", level = "trace", skip(plan, rx, context))]
pub(crate) async fn compile<'a>(
	plan: PhysicalPlan,
	rx: &mut StandardTransaction<'a>,
	context: Arc<ExecutionContext>,
) -> ExecutionPlan {
	match plan {
		PhysicalPlan::Aggregate(physical::AggregateNode {
			by,
			map,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context.clone()).await);
			ExecutionPlan::Aggregate(AggregateNode::new(input_node, by, map, context))
		}

		PhysicalPlan::Filter(physical::FilterNode {
			conditions,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context).await);
			ExecutionPlan::Filter(FilterNode::new(input_node, conditions))
		}

		PhysicalPlan::Take(physical::TakeNode {
			take,
			input,
		}) => {
			// Top-K optimization: if input is a Sort, fuse into TopKNode
			if let PhysicalPlan::Sort(physical::SortNode {
				by,
				input: sort_input,
			}) = *input
			{
				let input_node = Box::new(compile(*sort_input, rx, context).await);
				return ExecutionPlan::TopK(TopKNode::new(input_node, by, take));
			}
			// Fallback: regular Take
			let input_node = Box::new(compile(*input, rx, context).await);
			ExecutionPlan::Take(TakeNode::new(input_node, take))
		}

		PhysicalPlan::Sort(physical::SortNode {
			by,
			input,
		}) => {
			let input_node = Box::new(compile(*input, rx, context).await);
			ExecutionPlan::Sort(SortNode::new(input_node, by))
		}

		PhysicalPlan::Map(physical::MapNode {
			map,
			input,
		}) => {
			if let Some(input) = input {
				let input_node = Box::new(compile(*input, rx, context).await);
				ExecutionPlan::Map(MapNode::new(input_node, map))
			} else {
				ExecutionPlan::MapWithoutInput(MapWithoutInputNode::new(map))
			}
		}

		PhysicalPlan::Extend(physical::ExtendNode {
			extend,
			input,
		}) => {
			if let Some(input) = input {
				let input_node = Box::new(compile(*input, rx, context).await);
				ExecutionPlan::Extend(ExtendNode::new(input_node, extend))
			} else {
				ExecutionPlan::ExtendWithoutInput(ExtendWithoutInputNode::new(extend))
			}
		}

		PhysicalPlan::JoinInner(physical::JoinInnerNode {
			left,
			right,
			on,
			alias,
		}) => {
			// Extract source name from right plan for fallback alias
			let source_name = extract_source_name_from_physical(&right);

			// Use explicit alias, or fall back to extracted source name, or use "other"
			let effective_alias =
				alias.or(source_name).or_else(|| Some(Fragment::internal("other".to_string())));

			let left_node = Box::new(compile(*left, rx, context.clone()).await);
			let right_node = Box::new(compile(*right, rx, context.clone()).await);
			ExecutionPlan::InnerJoin(InnerJoinNode::new(left_node, right_node, on, effective_alias))
		}

		PhysicalPlan::JoinLeft(physical::JoinLeftNode {
			left,
			right,
			on,
			alias,
		}) => {
			// Extract source name from right plan for fallback alias
			let source_name = extract_source_name_from_physical(&right);

			// Use explicit alias, or fall back to extracted source name, or use "other"
			let effective_alias =
				alias.or(source_name).or_else(|| Some(Fragment::internal("other".to_string())));

			let left_node = Box::new(compile(*left, rx, context.clone()).await);
			let right_node = Box::new(compile(*right, rx, context.clone()).await);
			ExecutionPlan::LeftJoin(LeftJoinNode::new(left_node, right_node, on, effective_alias))
		}

		PhysicalPlan::JoinNatural(physical::JoinNaturalNode {
			left,
			right,
			join_type,
			alias,
		}) => {
			// Extract source name from right plan for fallback alias
			let source_name = extract_source_name_from_physical(&right);
			// Use explicit alias, or fall back to extracted source name, or use "other"
			let effective_alias =
				alias.or(source_name).or_else(|| Some(Fragment::internal("other".to_string())));

			let left_node = Box::new(compile(*left, rx, context.clone()).await);
			let right_node = Box::new(compile(*right, rx, context.clone()).await);
			ExecutionPlan::NaturalJoin(NaturalJoinNode::new(
				left_node,
				right_node,
				join_type,
				effective_alias,
			))
		}

		PhysicalPlan::InlineData(physical::InlineDataNode {
			rows,
		}) => ExecutionPlan::InlineData(InlineDataNode::new(rows, context)),

		PhysicalPlan::Generator(physical::GeneratorNode {
			name,
			expressions,
		}) => ExecutionPlan::Generator(GeneratorNode::new(name, expressions)),

		PhysicalPlan::IndexScan(node) => {
			let table = node.source.def().clone();
			let Some(pk) = table.primary_key.clone() else {
				unimplemented!()
			};

			ExecutionPlan::IndexScan(IndexScanNode::new(table, IndexId::primary(pk.id), context).unwrap())
		}

		PhysicalPlan::TableScan(node) => {
			ExecutionPlan::TableScan(TableScanNode::new(node.source.clone(), context, rx).await.unwrap())
		}

		PhysicalPlan::ViewScan(node) => {
			ExecutionPlan::ViewScan(ViewScanNode::new(node.source.clone(), context).unwrap())
		}

		PhysicalPlan::RingBufferScan(node) => ExecutionPlan::RingBufferScan(
			RingBufferScan::new(node.source.clone(), context, rx).await.unwrap(),
		),

		PhysicalPlan::FlowScan(_node) => {
			// TODO: Implement FlowScan execution
			unimplemented!("FlowScan execution not yet implemented")
		}

		PhysicalPlan::DictionaryScan(node) => {
			ExecutionPlan::DictionaryScan(DictionaryScanNode::new(node.source.clone(), context).unwrap())
		}

		PhysicalPlan::TableVirtualScan(node) => {
			// Create the appropriate virtual table implementation
			let namespace = node.source.namespace().def();
			let table = node.source.def();

			// First check user-defined virtual tables
			let virtual_table_impl: VTables = if let Some(user_table) =
				context.executor.virtual_table_registry.find_by_name(namespace.id, &table.name)
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
					"flow_lags" => VTables::FlowLags(FlowLags::new(context.executor.ioc.clone())),
					"flow_nodes" => VTables::FlowNodes(FlowNodes::new()),
					"flow_edges" => VTables::FlowEdges(FlowEdges::new()),
					"columns" => VTables::Columns(ColumnsTable::new()),
					"primary_keys" => VTables::PrimaryKeys(PrimaryKeys::new()),
					"primary_key_columns" => VTables::PrimaryKeyColumns(PrimaryKeyColumns::new()),
					"column_policies" => VTables::ColumnPolicies(ColumnPolicies::new()),
					"versions" => VTables::Versions(Versions::new()),
					"primitive_retention_policies" => {
						VTables::PrimitiveRetentionPolicies(PrimitiveRetentionPolicies::new())
					}
					"operator_retention_policies" => {
						VTables::OperatorRetentionPolicies(OperatorRetentionPolicies::new())
					}
					"cdc_consumers" => VTables::CdcConsumers(CdcConsumers::new()),
					"flow_operators" => VTables::FlowOperators(FlowOperators::new(
						context.executor.flow_operator_store.clone(),
					)),
					"dictionaries" => VTables::Dictionaries(Dictionaries::new()),
					"virtual_tables" => VTables::TablesVirtual(TablesVirtual::new()),
					"types" => VTables::Types(Types::new()),
					"flow_node_types" => VTables::FlowNodeTypes(FlowNodeTypes::new()),
					"flow_operator_inputs" => VTables::FlowOperatorInputs(FlowOperatorInputs::new(
						context.executor.flow_operator_store.clone(),
					)),
					"flow_operator_outputs" => VTables::FlowOperatorOutputs(
						FlowOperatorOutputs::new(context.executor.flow_operator_store.clone()),
					),
					"ringbuffers" => VTables::RingBuffers(RingBuffers::new()),
					"table_storage_stats" => VTables::TableStorageStats(TableStorageStats::new(
						context.executor.stats_tracker.clone(),
					)),
					"view_storage_stats" => VTables::ViewStorageStats(ViewStorageStats::new(
						context.executor.stats_tracker.clone(),
					)),
					"flow_storage_stats" => VTables::FlowStorageStats(FlowStorageStats::new(
						context.executor.stats_tracker.clone(),
					)),
					"flow_node_storage_stats" => VTables::FlowNodeStorageStats(
						FlowNodeStorageStats::new(context.executor.stats_tracker.clone()),
					),
					"index_storage_stats" => VTables::IndexStorageStats(IndexStorageStats::new(
						context.executor.stats_tracker.clone(),
					)),
					"ringbuffer_storage_stats" => VTables::RingBufferStorageStats(
						RingBufferStorageStats::new(context.executor.stats_tracker.clone()),
					),
					"dictionary_storage_stats" => VTables::DictionaryStorageStats(
						DictionaryStorageStats::new(context.executor.stats_tracker.clone()),
					),
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

			ExecutionPlan::VirtualScan(
				VirtualScanNode::new(virtual_table_impl, context, virtual_context).unwrap(),
			)
		}

		PhysicalPlan::Declare(declare_node) => ExecutionPlan::Declare(DeclareNode::new(declare_node)),

		PhysicalPlan::Assign(assign_node) => ExecutionPlan::Assign(AssignNode::new(assign_node)),

		PhysicalPlan::Conditional(conditional_node) => {
			ExecutionPlan::Conditional(ConditionalNode::new(conditional_node))
		}

		PhysicalPlan::Variable(var_node) => ExecutionPlan::Variable(VariableNode::new(var_node.variable_expr)),

		PhysicalPlan::Environment(_) => ExecutionPlan::Environment(EnvironmentNode::new()),

		PhysicalPlan::Scalarize(scalarize_node) => {
			let input = compile(*scalarize_node.input, rx, context.clone()).await;
			ExecutionPlan::Scalarize(ScalarizeNode::new(Box::new(input)))
		}

		PhysicalPlan::AlterSequence(_)
		| PhysicalPlan::AlterTable(_)
		| PhysicalPlan::AlterView(_)
		| PhysicalPlan::AlterFlow(_)
		| PhysicalPlan::CreateDeferredView(_)
		| PhysicalPlan::CreateTransactionalView(_)
		| PhysicalPlan::CreateNamespace(_)
		| PhysicalPlan::CreateTable(_)
		| PhysicalPlan::CreateRingBuffer(_)
		| PhysicalPlan::CreateFlow(_)
		| PhysicalPlan::CreateDictionary(_)
		| PhysicalPlan::Delete(_)
		| PhysicalPlan::DeleteRingBuffer(_)
		| PhysicalPlan::InsertTable(_)
		| PhysicalPlan::InsertRingBuffer(_)
		| PhysicalPlan::InsertDictionary(_)
		| PhysicalPlan::Update(_)
		| PhysicalPlan::UpdateRingBuffer(_)
		| PhysicalPlan::Distinct(_) => unreachable!(),
		PhysicalPlan::Apply(_) => {
			unimplemented!(
				"Apply operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
			)
		}
		PhysicalPlan::Window(_) => {
			unimplemented!(
				"Window operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
			)
		}
		PhysicalPlan::Merge(_) => {
			unimplemented!(
				"Merge operator is only supported in deferred views and requires the flow engine. Use within a CREATE DEFERRED VIEW statement."
			)
		}

		// Row-number optimized access nodes
		PhysicalPlan::RowPointLookup(physical::RowPointLookupNode {
			source,
			row_number,
		}) => {
			let resolved_source = reifydb_core::interface::ResolvedPrimitive::from(source);
			ExecutionPlan::RowPointLookup(
				RowPointLookupNode::new(resolved_source, row_number, context)
					.expect("Failed to create RowPointLookupNode"),
			)
		}
		PhysicalPlan::RowListLookup(physical::RowListLookupNode {
			source,
			row_numbers,
		}) => {
			let resolved_source = reifydb_core::interface::ResolvedPrimitive::from(source);
			ExecutionPlan::RowListLookup(
				RowListLookupNode::new(resolved_source, row_numbers, context)
					.expect("Failed to create RowListLookupNode"),
			)
		}
		PhysicalPlan::RowRangeScan(physical::RowRangeScanNode {
			source,
			start,
			end,
		}) => {
			let resolved_source = reifydb_core::interface::ResolvedPrimitive::from(source);
			ExecutionPlan::RowRangeScan(
				RowRangeScanNode::new(resolved_source, start, end, context)
					.expect("Failed to create RowRangeScanNode"),
			)
		}
	}
}
