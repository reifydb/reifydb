// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_catalog::vtable::{
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
		flow_storage_stats::SystemFlowStorageStats, flows::SystemFlows, granted_roles::SystemGrantedRoles,
		handlers::SystemHandlers, identities::SystemIdentities, index_storage_stats::SystemIndexStorageStats,
		migrations::SystemMigrations, namespaces::SystemNamespaces,
		operator_retention_policies::SystemOperatorRetentionPolicies, policies::SystemPolicies,
		policy_operations::SystemPolicyOperations, primary_key_columns::SystemPrimaryKeyColumns,
		primary_keys::SystemPrimaryKeys, procedures::SystemProcedures,
		ringbuffer_storage_stats::SystemRingBufferStorageStats, ringbuffers::SystemRingBuffers,
		roles::SystemRoles, sequences::SystemSequences, series::SystemSeries, shape_fields::SystemShapeFields,
		shape_retention_policies::SystemShapeRetentionPolicies, shapes::SystemShapes,
		table_storage_stats::SystemTableStorageStats, tables::SystemTables,
		tables_virtual::SystemTablesVirtual, tag_variants::SystemTagVariants, tags::SystemTags,
		types::SystemTypes, versions::SystemVersions, view_storage_stats::SystemViewStorageStats,
		views::SystemViews, virtual_table_columns::SystemVirtualTableColumns,
	},
	tables::VTables,
};
use reifydb_core::interface::catalog::id::NamespaceId;
use reifydb_rql::nodes::TableVirtualScanNode;

use crate::vm::volcano::{
	query::{QueryContext, QueryNode},
	scan::vtable::VirtualScanNode,
};

pub(crate) fn compile_virtual_scan(node: TableVirtualScanNode, context: Arc<QueryContext>) -> Box<dyn QueryNode> {
	let namespace = node.source.namespace().def();
	let table = node.source.def();

	// First check user-defined virtual tables
	let virtual_table_impl: VTables = if let Some(user_table) =
		context.services.virtual_table_registry.find_by_name(namespace.id(), &table.name)
	{
		user_table
	} else if namespace.id() == NamespaceId::SYSTEM {
		// Built-in system virtual tables
		compile_system_vtable(&table.name, &context)
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

fn compile_system_vtable(name: &str, context: &QueryContext) -> VTables {
	match name {
		"sequences" => VTables::Sequences(SystemSequences::new()),
		"namespaces" => VTables::Namespaces(SystemNamespaces::new()),
		"tables" => VTables::Tables(SystemTables::new()),
		"views" => VTables::Views(SystemViews::new()),
		"flows" => VTables::Flows(SystemFlows::new()),
		"flow_lags" => VTables::FlowLags(SystemFlowLags::new(context.services.ioc.clone())),
		"flow_nodes" => VTables::FlowNodes(SystemFlowNodes::new()),
		"flow_edges" => VTables::FlowEdges(SystemFlowEdges::new()),
		"columns" => VTables::Columns(SystemColumnsTable::new()),
		"primary_keys" => VTables::PrimaryKeys(SystemPrimaryKeys::new()),
		"primary_key_columns" => VTables::PrimaryKeyColumns(SystemPrimaryKeyColumns::new()),
		"column_properties" => VTables::ColumnProperties(SystemColumnProperties::new()),
		"versions" => VTables::Versions(SystemVersions::new(context.services.ioc.clone())),
		"shape_retention_policies" => VTables::ShapeRetentionPolicies(SystemShapeRetentionPolicies::new()),
		"operator_retention_policies" => {
			VTables::OperatorRetentionPolicies(SystemOperatorRetentionPolicies::new())
		}
		"cdc_consumers" => VTables::CdcConsumers(SystemCdcConsumers::new()),
		"flow_operators" => {
			VTables::FlowOperators(SystemFlowOperators::new(context.services.flow_operator_store.clone()))
		}
		"dictionaries" => VTables::Dictionaries(SystemDictionaries::new()),
		"virtual_tables" => VTables::TablesVirtual(SystemTablesVirtual::new(context.services.catalog.clone())),
		"types" => VTables::Types(SystemTypes::new()),
		"flow_node_types" => VTables::FlowNodeTypes(SystemFlowNodeTypes::new()),
		"flow_operator_inputs" => VTables::FlowOperatorInputs(SystemFlowOperatorInputs::new(
			context.services.flow_operator_store.clone(),
		)),
		"flow_operator_outputs" => VTables::FlowOperatorOutputs(SystemFlowOperatorOutputs::new(
			context.services.flow_operator_store.clone(),
		)),
		"ringbuffers" => VTables::RingBuffers(SystemRingBuffers::new()),
		"table_storage_stats" => {
			VTables::TableStorageStats(SystemTableStorageStats::new(context.services.stats_reader.clone()))
		}
		"view_storage_stats" => {
			VTables::ViewStorageStats(SystemViewStorageStats::new(context.services.stats_reader.clone()))
		}
		"flow_storage_stats" => {
			VTables::FlowStorageStats(SystemFlowStorageStats::new(context.services.stats_reader.clone()))
		}
		"flow_node_storage_stats" => VTables::FlowNodeStorageStats(SystemFlowNodeStorageStats::new(
			context.services.stats_reader.clone(),
		)),
		"index_storage_stats" => {
			VTables::IndexStorageStats(SystemIndexStorageStats::new(context.services.stats_reader.clone()))
		}
		"ringbuffer_storage_stats" => VTables::RingBufferStorageStats(SystemRingBufferStorageStats::new(
			context.services.stats_reader.clone(),
		)),
		"dictionary_storage_stats" => VTables::DictionaryStorageStats(SystemDictionaryStorageStats::new(
			context.services.stats_reader.clone(),
		)),
		"shapes" => VTables::Shapes(SystemShapes::new(context.services.catalog.clone())),
		"shape_fields" => VTables::ShapeFields(SystemShapeFields::new(context.services.catalog.clone())),
		"enums" => VTables::Enums(SystemEnums::new()),
		"enum_variants" => VTables::EnumVariants(SystemEnumVariants::new()),
		"events" => VTables::Events(SystemEvents::new()),
		"event_variants" => VTables::EventVariants(SystemEventVariants::new()),
		"procedures" => VTables::Procedures(SystemProcedures::new(context.services.catalog.clone())),
		"handlers" => VTables::Handlers(SystemHandlers::new(context.services.catalog.clone())),
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
		"virtual_table_columns" => {
			VTables::VirtualTableColumns(SystemVirtualTableColumns::new(context.services.catalog.clone()))
		}
		_ => panic!("Unknown virtual table type: {}", name),
	}
}
