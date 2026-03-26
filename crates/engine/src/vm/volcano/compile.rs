// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections, sync::Arc};

use reifydb_catalog::{
	catalog::Catalog,
	vtable::{
		VTableContext,
		system::{
			cdc_consumers::SystemCdcConsumers, column_properties::SystemColumnProperties,
			columns::SystemColumnsTable, configs::SystemConfigs, dictionaries::SystemDictionaries,
			dictionary_storage_stats::SystemDictionaryStorageStats, enum_variants::SystemEnumVariants,
			enums::SystemEnums, event_variants::SystemEventVariants, events::SystemEvents,
			flow_edges::SystemFlowEdges, flow_lags::SystemFlowLags,
			flow_node_storage_stats::SystemFlowNodeStorageStats, flow_node_types::SystemFlowNodeTypes,
			flow_nodes::SystemFlowNodes, flow_operator_inputs::SystemFlowOperatorInputs,
			flow_operator_outputs::SystemFlowOperatorOutputs, flow_operators::SystemFlowOperators,
			flow_storage_stats::SystemFlowStorageStats, flows::SystemFlows,
			granted_roles::SystemGrantedRoles, handlers::SystemHandlers, identities::SystemIdentities,
			index_storage_stats::SystemIndexStorageStats, migrations::SystemMigrations,
			namespaces::SystemNamespaces, operator_retention_policies::SystemOperatorRetentionPolicies,
			policies::SystemPolicies, policy_operations::SystemPolicyOperations,
			primary_key_columns::SystemPrimaryKeyColumns, primary_keys::SystemPrimaryKeys,
			primitive_retention_policies::SystemPrimitiveRetentionPolicies, procedures::SystemProcedures,
			ringbuffer_storage_stats::SystemRingBufferStorageStats, ringbuffers::SystemRingBuffers,
			roles::SystemRoles, schema_fields::SystemSchemaFields, schemas::SystemSchemas,
			sequences::SystemSequences, series::SystemSeries, table_storage_stats::SystemTableStorageStats,
			tables::SystemTables, tables_virtual::SystemTablesVirtual, tag_variants::SystemTagVariants,
			tags::SystemTags, types::SystemTypes, versions::SystemVersions,
			view_storage_stats::SystemViewStorageStats, views::SystemViews,
			virtual_table_columns::SystemVirtualTableColumns,
		},
		tables::VTables,
	},
};
use reifydb_core::interface::{
	catalog::id::{IndexId, NamespaceId},
	resolved::ResolvedPrimitive,
};
use reifydb_rql::{
	expression::{AliasExpression, ConstantExpression, Expression, IdentExpression},
	nodes::{
		AggregateNode as RqlAggregateNode, AssertNode as RqlAssertNode, ExtendNode as RqlExtendNode,
		FilterNode as RqlFilterNode, GateNode as RqlGateNode, GeneratorNode as RqlGeneratorNode,
		InlineDataNode as RqlInlineDataNode, JoinInnerNode as RqlJoinInnerNode,
		JoinLeftNode as RqlJoinLeftNode, JoinNaturalNode as RqlJoinNaturalNode, MapNode as RqlMapNode,
		PatchNode as RqlPatchNode, RowListLookupNode as RqlRowListLookupNode,
		RowPointLookupNode as RqlRowPointLookupNode, RowRangeScanNode as RqlRowRangeScanNode,
		SortNode as RqlSortNode, TakeLimit, TakeNode as RqlTakeNode,
	},
	query::QueryPlan as RqlQueryPlan,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::constraint::Constraint};
use tracing::instrument;

use super::{apply_transform::ApplyTransformNode, filter::resolve_is_variant_tags, run_tests::RunTestsQueryNode};
use crate::vm::{
	stack::Variable,
	volcano::{
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
			dictionary::DictionaryScanNode, index::IndexScanNode, remote::RemoteFetchNode,
			ringbuffer::RingBufferScan, series::SeriesScanNode as VolcanoSeriesScanNode,
			table::TableScanNode, view::ViewScanNode, vtable::VirtualScanNode,
		},
		sort::SortNode,
		take::TakeNode,
		top_k::TopKNode,
		variable::VariableNode,
	},
};

// Extract the source name from a query plan if it's a scan node
fn extract_source_name_from_query(plan: &RqlQueryPlan) -> Option<Fragment> {
	match plan {
		RqlQueryPlan::TableScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::ViewScan(node) => Some(Fragment::internal(node.source.def().name().to_string())),
		RqlQueryPlan::RingBufferScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::DictionaryScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::SeriesScan(node) => Some(Fragment::internal(node.source.def().name.clone())),
		RqlQueryPlan::RemoteScan(_) => None,
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
		RqlQueryPlan::SeriesScan(node) => Some(ResolvedPrimitive::Series(node.source.clone())),
		RqlQueryPlan::RemoteScan(_) => None,
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
	catalog: &Catalog,
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

		let Some((sumtype, _)) = sumtype_info else {
			expanded.push(expr);
			continue;
		};

		let fragment = alias_expr.fragment.clone();

		match alias_expr.expression.as_ref() {
			Expression::SumTypeConstructor(ctor) => {
				let variant_name_lower = ctor.variant_name.text().to_lowercase();
				let variant = sumtype
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
				let field_map: collections::HashMap<String, &Expression> = ctor
					.columns
					.iter()
					.map(|(name, expr)| (name.text().to_lowercase(), expr))
					.collect();

				// All variant fields: active variant gets values, others get None
				for v in &sumtype.variants {
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
				if let Some(variant) =
					sumtype.variants.iter().find(|v| v.name.to_lowercase() == variant_name_lower)
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
					for v in &sumtype.variants {
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
					resolve_is_variant_tags(expr, &source, &context.services.catalog, rx)
						.expect("resolve IS variant tags");
				}
			}
			let input_node = compile(*input, rx, context);
			Box::new(FilterNode::new(input_node, conditions))
		}
		RqlQueryPlan::Gate(RqlGateNode {
			conditions,
			input,
		}) => {
			let input_node = compile(*input, rx, context);
			Box::new(FilterNode::new(input_node, conditions))
		}

		RqlQueryPlan::Take(RqlTakeNode {
			take,
			input,
		}) => {
			let limit = match take {
				TakeLimit::Literal(n) => n,
				TakeLimit::Variable(ref name) => context
					.symbols
					.get(name)
					.and_then(|var| match var {
						Variable::Scalar(cols) | Variable::Columns(cols) => {
							cols.scalar_value().to_usize()
						}
						_ => None,
					})
					.expect(&format!("TAKE variable ${} must be a numeric value", name)),
			};
			if let RqlQueryPlan::Sort(sort_node) = *input {
				let input_node = compile(*sort_node.input, rx, context);
				return Box::new(TopKNode::new(input_node, sort_node.by, limit));
			}
			let input_node = compile(*input, rx, context);
			Box::new(TakeNode::new(input_node, limit))
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
						resolve_is_variant_tags(expr, &source, &context.services.catalog, rx)
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
						resolve_is_variant_tags(expr, &source, &context.services.catalog, rx)
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

		RqlQueryPlan::DictionaryScan(node) => {
			Box::new(DictionaryScanNode::new(node.source.clone(), context).unwrap())
		}

		RqlQueryPlan::SeriesScan(node) => Box::new(
			VolcanoSeriesScanNode::new(
				node.source.clone(),
				node.key_range_start,
				node.key_range_end,
				node.variant_tag,
				context,
			)
			.unwrap(),
		),

		RqlQueryPlan::TableVirtualScan(node) => {
			// Create the appropriate virtual table implementation
			let namespace = node.source.namespace().def();
			let table = node.source.def();

			// First check user-defined virtual tables
			let virtual_table_impl: VTables = if let Some(user_table) =
				context.services.virtual_table_registry.find_by_name(namespace.id(), &table.name)
			{
				// User-defined virtual table - registry returns VTableImpl directly
				user_table
			} else if namespace.id() == NamespaceId::SYSTEM {
				// Built-in system virtual tables
				match table.name.as_str() {
					"sequences" => VTables::Sequences(SystemSequences::new()),
					"namespaces" => VTables::Namespaces(SystemNamespaces::new()),
					"tables" => VTables::Tables(SystemTables::new()),
					"views" => VTables::Views(SystemViews::new()),
					"flows" => VTables::Flows(SystemFlows::new()),
					"flow_lags" => {
						VTables::FlowLags(SystemFlowLags::new(context.services.ioc.clone()))
					}
					"flow_nodes" => VTables::FlowNodes(SystemFlowNodes::new()),
					"flow_edges" => VTables::FlowEdges(SystemFlowEdges::new()),
					"columns" => VTables::Columns(SystemColumnsTable::new()),
					"primary_keys" => VTables::PrimaryKeys(SystemPrimaryKeys::new()),
					"primary_key_columns" => {
						VTables::PrimaryKeyColumns(SystemPrimaryKeyColumns::new())
					}
					"column_properties" => VTables::ColumnProperties(SystemColumnProperties::new()),
					"versions" => {
						VTables::Versions(SystemVersions::new(context.services.ioc.clone()))
					}
					"primitive_retention_policies" => VTables::PrimitiveRetentionPolicies(
						SystemPrimitiveRetentionPolicies::new(),
					),
					"operator_retention_policies" => VTables::OperatorRetentionPolicies(
						SystemOperatorRetentionPolicies::new(),
					),
					"cdc_consumers" => VTables::CdcConsumers(SystemCdcConsumers::new()),
					"flow_operators" => VTables::FlowOperators(SystemFlowOperators::new(
						context.services.flow_operator_store.clone(),
					)),
					"dictionaries" => VTables::Dictionaries(SystemDictionaries::new()),
					"virtual_tables" => VTables::TablesVirtual(SystemTablesVirtual::new(
						context.services.catalog.clone(),
					)),
					"types" => VTables::Types(SystemTypes::new()),
					"flow_node_types" => VTables::FlowNodeTypes(SystemFlowNodeTypes::new()),
					"flow_operator_inputs" => {
						VTables::FlowOperatorInputs(SystemFlowOperatorInputs::new(
							context.services.flow_operator_store.clone(),
						))
					}
					"flow_operator_outputs" => {
						VTables::FlowOperatorOutputs(SystemFlowOperatorOutputs::new(
							context.services.flow_operator_store.clone(),
						))
					}
					"ringbuffers" => VTables::RingBuffers(SystemRingBuffers::new()),
					"table_storage_stats" => VTables::TableStorageStats(
						SystemTableStorageStats::new(context.services.stats_reader.clone()),
					),
					"view_storage_stats" => VTables::ViewStorageStats(SystemViewStorageStats::new(
						context.services.stats_reader.clone(),
					)),
					"flow_storage_stats" => VTables::FlowStorageStats(SystemFlowStorageStats::new(
						context.services.stats_reader.clone(),
					)),
					"flow_node_storage_stats" => VTables::FlowNodeStorageStats(
						SystemFlowNodeStorageStats::new(context.services.stats_reader.clone()),
					),
					"index_storage_stats" => VTables::IndexStorageStats(
						SystemIndexStorageStats::new(context.services.stats_reader.clone()),
					),
					"ringbuffer_storage_stats" => {
						VTables::RingBufferStorageStats(SystemRingBufferStorageStats::new(
							context.services.stats_reader.clone(),
						))
					}
					"dictionary_storage_stats" => {
						VTables::DictionaryStorageStats(SystemDictionaryStorageStats::new(
							context.services.stats_reader.clone(),
						))
					}
					"schemas" => {
						VTables::Schemas(SystemSchemas::new(context.services.catalog.clone()))
					}
					"schema_fields" => VTables::SchemaFields(SystemSchemaFields::new(
						context.services.catalog.clone(),
					)),
					"enums" => VTables::Enums(SystemEnums::new()),
					"enum_variants" => VTables::EnumVariants(SystemEnumVariants::new()),
					"events" => VTables::Events(SystemEvents::new()),
					"event_variants" => VTables::EventVariants(SystemEventVariants::new()),
					"procedures" => VTables::Procedures(SystemProcedures::new(
						context.services.catalog.clone(),
					)),
					"handlers" => {
						VTables::Handlers(SystemHandlers::new(context.services.catalog.clone()))
					}
					"tags" => VTables::Tags(SystemTags::new()),
					"tag_variants" => VTables::TagVariants(SystemTagVariants::new()),
					"series" => VTables::Series(SystemSeries::new()),
					"identities" => VTables::Identities(SystemIdentities::new()),
					"roles" => VTables::Roles(SystemRoles::new()),
					"granted_roles" => VTables::GrantedRoles(SystemGrantedRoles::new()),
					"policies" => VTables::Policies(SystemPolicies::new()),
					"policy_operations" => VTables::PolicyOperations(SystemPolicyOperations::new()),
					"migrations" => VTables::Migrations(SystemMigrations::new()),
					"configs" => VTables::Configs(SystemConfigs::new(context.services.ioc.clone())),
					"virtual_table_columns" => VTables::VirtualTableColumns(
						SystemVirtualTableColumns::new(context.services.catalog.clone()),
					),
					_ => panic!("Unknown virtual table type: {}", table.name),
				}
			} else {
				panic!("Unknown virtual table type: {}.{}", namespace.name(), table.name)
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

		RqlQueryPlan::RemoteScan(node) => {
			Box::new(RemoteFetchNode::new(node.address, node.token, node.remote_rql, node.variables))
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

			Box::new(ApplyTransformNode::new(input_node, transform))
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

		RqlQueryPlan::RunTests(node) => Box::new(RunTestsQueryNode::new(node, context.clone())),

		RqlQueryPlan::CallFunction(node) => Box::new(GeneratorNode::new(node.name, node.arguments)),

		// Row-number optimized access nodes
		RqlQueryPlan::RowPointLookup(RqlRowPointLookupNode {
			source,
			row_number,
		}) => {
			let resolved_source = ResolvedPrimitive::from(source);
			Box::new(
				RowPointLookupNode::new(resolved_source, row_number, context)
					.expect("Failed to create RowPointLookupNode"),
			)
		}
		RqlQueryPlan::RowListLookup(RqlRowListLookupNode {
			source,
			row_numbers,
		}) => {
			let resolved_source = ResolvedPrimitive::from(source);
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
			let resolved_source = ResolvedPrimitive::from(source);
			Box::new(
				RowRangeScanNode::new(resolved_source, start, end, context)
					.expect("Failed to create RowRangeScanNode"),
			)
		}
	}
}
