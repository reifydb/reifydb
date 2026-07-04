// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::{
	system::SystemCatalog,
	vtable::{
		VTableContext,
		system::{
			bindings::{grpc::SystemBindingsGrpc, http::SystemBindingsHttp, ws::SystemBindingsWs},
			cdc_consumers::SystemCdcConsumers,
			column_properties::SystemColumnProperties,
			columns::SystemColumnsTable,
			configs::SystemConfigs,
			dictionaries::SystemDictionaries,
			enum_variants::SystemEnumVariants,
			enums::SystemEnums,
			event_variants::SystemEventVariants,
			events::SystemEvents,
			flow_edges::SystemFlowEdges,
			flow_node_types::SystemFlowNodeTypes,
			flow_nodes::SystemFlowNodes,
			flow_operator_inputs::SystemFlowOperatorInputs,
			flow_operator_outputs::SystemFlowOperatorOutputs,
			flow_operators::SystemFlowOperators,
			flow_watermarks::SystemFlowWatermarks,
			flows::SystemFlows,
			granted_roles::SystemGrantedRoles,
			handlers::SystemHandlers,
			identities::SystemIdentities,
			identity_attribute_values::SystemIdentityAttributeValues,
			identity_attributes::SystemIdentityAttributes,
			metrics::{StatsPrimitive, cdc::SystemMetricsCdc, storage::SystemMetricsStorage},
			migrations::SystemMigrations,
			namespaces::SystemNamespaces,
			operator_retention_strategies::SystemOperatorRetentionStrategies,
			policies::SystemPolicies,
			policy_operations::SystemPolicyOperations,
			primary_key_columns::SystemPrimaryKeyColumns,
			primary_keys::SystemPrimaryKeys,
			procedures::{
				ffi::SystemProceduresFFI, native::SystemProceduresNative, rql::SystemProceduresRql,
				test::SystemProceduresTest, wasm::SystemProceduresWasm,
			},
			ringbuffers::SystemRingBuffers,
			roles::SystemRoles,
			sequences::SystemSequences,
			series::SystemSeries,
			shape_fields::SystemShapeFields,
			shape_retention_strategies::SystemShapeRetentionStrategies,
			shapes::SystemShapes,
			subscription_watermarks::SystemSubscriptionWatermarks,
			subscriptions::SystemSubscriptions,
			tables::SystemTables,
			tables_virtual::SystemTablesVirtual,
			tag_variants::SystemTagVariants,
			tags::SystemTags,
			types::SystemTypes,
			versions::SystemVersions,
			views::SystemViews,
			virtual_table_columns::SystemVirtualTableColumns,
		},
		tables::VTables,
	},
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

	let virtual_table_impl: VTables = if let Some(user_table) =
		context.services.virtual_table_registry.find_by_name(namespace.id(), &table.name)
	{
		user_table
	} else if namespace.id() == NamespaceId::SYSTEM {
		compile_system_vtable(&table.name, &context)
	} else if let Some(vtable) = compile_metrics_storage_vtable(namespace.id(), &context) {
		vtable
	} else if let Some(vtable) = compile_metrics_cdc_vtable(namespace.id(), &context) {
		vtable
	} else if namespace.id() == NamespaceId::SYSTEM_PROCEDURES {
		compile_procedures_vtable(&table.name, &context)
	} else if namespace.id() == NamespaceId::SYSTEM_BINDINGS {
		compile_bindings_vtable(&table.name)
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
		"flow_watermarks" => VTables::FlowWatermarks(SystemFlowWatermarks::new(context.services.ioc.clone())),
		"subscription_watermarks" => {
			VTables::SubscriptionWatermarks(SystemSubscriptionWatermarks::new(context.services.ioc.clone()))
		}
		"flow_nodes" => VTables::FlowNodes(SystemFlowNodes::new()),
		"flow_edges" => VTables::FlowEdges(SystemFlowEdges::new()),
		"columns" => VTables::Columns(SystemColumnsTable::new()),
		"primary_keys" => VTables::PrimaryKeys(SystemPrimaryKeys::new()),
		"primary_key_columns" => VTables::PrimaryKeyColumns(SystemPrimaryKeyColumns::new()),
		"column_properties" => VTables::ColumnProperties(SystemColumnProperties::new()),
		"versions" => VTables::Versions(SystemVersions::new(context.services.ioc.clone())),
		"shape_retention_policies" => VTables::ShapeRetentionStrategies(SystemShapeRetentionStrategies::new()),
		"operator_retention_policies" => {
			VTables::OperatorRetentionStrategies(SystemOperatorRetentionStrategies::new())
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
		"shapes" => VTables::Shapes(SystemShapes::new(context.services.catalog.clone())),
		"shape_fields" => VTables::ShapeFields(SystemShapeFields::new(context.services.catalog.clone())),
		"enums" => VTables::Enums(SystemEnums::new()),
		"enum_variants" => VTables::EnumVariants(SystemEnumVariants::new()),
		"events" => VTables::Events(SystemEvents::new()),
		"event_variants" => VTables::EventVariants(SystemEventVariants::new()),
		"handlers" => VTables::Handlers(SystemHandlers::new(context.services.catalog.clone())),
		"tags" => VTables::Tags(SystemTags::new()),
		"tag_variants" => VTables::TagVariants(SystemTagVariants::new()),
		"series" => VTables::Series(SystemSeries::new()),
		"identities" => VTables::Identities(SystemIdentities::new()),
		"identity_attributes" => VTables::IdentityAttributes(SystemIdentityAttributes::new()),
		"identity_attribute_values" => VTables::IdentityAttributeValues(SystemIdentityAttributeValues::new()),
		"roles" => VTables::Roles(SystemRoles::new()),
		"granted_roles" => VTables::GrantedRoles(SystemGrantedRoles::new()),
		"policies" => VTables::Policies(SystemPolicies::new()),
		"policy_operations" => VTables::PolicyOperations(SystemPolicyOperations::new()),
		"migrations" => VTables::Migrations(SystemMigrations::new()),
		"configs" => VTables::Configs(SystemConfigs::new(context.services.ioc.clone())),
		"subscriptions" => VTables::Subscriptions(SystemSubscriptions::new(context.services.ioc.clone())),
		"virtual_table_columns" => {
			VTables::VirtualTableColumns(SystemVirtualTableColumns::new(context.services.catalog.clone()))
		}
		_ => panic!("Unknown virtual table type: {}", name),
	}
}

fn compile_metrics_storage_vtable(namespace: NamespaceId, context: &QueryContext) -> Option<VTables> {
	let (vtable, primitive) = if namespace == NamespaceId::SYSTEM_METRICS_STORAGE_TABLE {
		(SystemCatalog::get_system_metrics_storage_table_table(), StatsPrimitive::Table)
	} else if namespace == NamespaceId::SYSTEM_METRICS_STORAGE_VIEW {
		(SystemCatalog::get_system_metrics_storage_view_table(), StatsPrimitive::View)
	} else if namespace == NamespaceId::SYSTEM_METRICS_STORAGE_TABLE_VIRTUAL {
		(SystemCatalog::get_system_metrics_storage_table_virtual_table(), StatsPrimitive::TableVirtual)
	} else if namespace == NamespaceId::SYSTEM_METRICS_STORAGE_RINGBUFFER {
		(SystemCatalog::get_system_metrics_storage_ringbuffer_table(), StatsPrimitive::RingBuffer)
	} else if namespace == NamespaceId::SYSTEM_METRICS_STORAGE_DICTIONARY {
		(SystemCatalog::get_system_metrics_storage_dictionary_table(), StatsPrimitive::Dictionary)
	} else if namespace == NamespaceId::SYSTEM_METRICS_STORAGE_SERIES {
		(SystemCatalog::get_system_metrics_storage_series_table(), StatsPrimitive::Series)
	} else if namespace == NamespaceId::SYSTEM_METRICS_STORAGE_FLOW {
		(SystemCatalog::get_system_metrics_storage_flow_table(), StatsPrimitive::Flow)
	} else if namespace == NamespaceId::SYSTEM_METRICS_STORAGE_FLOW_NODE {
		(SystemCatalog::get_system_metrics_storage_flow_node_table(), StatsPrimitive::FlowNode)
	} else if namespace == NamespaceId::SYSTEM_METRICS_STORAGE_SYSTEM {
		(SystemCatalog::get_system_metrics_storage_system_table(), StatsPrimitive::System)
	} else {
		return None;
	};
	let reader = context.services.stats_reader.clone();
	Some(VTables::MetricsStorage(SystemMetricsStorage::new(vtable, primitive, reader)))
}

fn compile_metrics_cdc_vtable(namespace: NamespaceId, context: &QueryContext) -> Option<VTables> {
	let (vtable, primitive) = if namespace == NamespaceId::SYSTEM_METRICS_CDC_TABLE {
		(SystemCatalog::get_system_metrics_cdc_table_table(), StatsPrimitive::Table)
	} else if namespace == NamespaceId::SYSTEM_METRICS_CDC_VIEW {
		(SystemCatalog::get_system_metrics_cdc_view_table(), StatsPrimitive::View)
	} else if namespace == NamespaceId::SYSTEM_METRICS_CDC_TABLE_VIRTUAL {
		(SystemCatalog::get_system_metrics_cdc_table_virtual_table(), StatsPrimitive::TableVirtual)
	} else if namespace == NamespaceId::SYSTEM_METRICS_CDC_RINGBUFFER {
		(SystemCatalog::get_system_metrics_cdc_ringbuffer_table(), StatsPrimitive::RingBuffer)
	} else if namespace == NamespaceId::SYSTEM_METRICS_CDC_DICTIONARY {
		(SystemCatalog::get_system_metrics_cdc_dictionary_table(), StatsPrimitive::Dictionary)
	} else if namespace == NamespaceId::SYSTEM_METRICS_CDC_SERIES {
		(SystemCatalog::get_system_metrics_cdc_series_table(), StatsPrimitive::Series)
	} else if namespace == NamespaceId::SYSTEM_METRICS_CDC_FLOW {
		(SystemCatalog::get_system_metrics_cdc_flow_table(), StatsPrimitive::Flow)
	} else if namespace == NamespaceId::SYSTEM_METRICS_CDC_FLOW_NODE {
		(SystemCatalog::get_system_metrics_cdc_flow_node_table(), StatsPrimitive::FlowNode)
	} else if namespace == NamespaceId::SYSTEM_METRICS_CDC_SYSTEM {
		(SystemCatalog::get_system_metrics_cdc_system_table(), StatsPrimitive::System)
	} else {
		return None;
	};
	let reader = context.services.stats_reader.clone();
	Some(VTables::MetricsCdc(SystemMetricsCdc::new(vtable, primitive, reader)))
}

fn compile_procedures_vtable(name: &str, context: &QueryContext) -> VTables {
	let catalog = context.services.catalog.clone();
	match name {
		"rql" => VTables::ProceduresRql(SystemProceduresRql::new()),
		"test" => VTables::ProceduresTest(SystemProceduresTest::new()),
		"native" => VTables::ProceduresNative(SystemProceduresNative::new(catalog)),
		"ffi" => VTables::ProceduresFFI(SystemProceduresFFI::new(catalog)),
		"wasm" => VTables::ProceduresWasm(SystemProceduresWasm::new(catalog)),
		_ => panic!("Unknown system::procedures virtual table: {}", name),
	}
}

fn compile_bindings_vtable(name: &str) -> VTables {
	match name {
		"http" => VTables::BindingsHttp(SystemBindingsHttp::new()),
		"grpc" => VTables::BindingsGrpc(SystemBindingsGrpc::new()),
		"ws" => VTables::BindingsWs(SystemBindingsWs::new()),
		_ => panic!("Unknown system::bindings virtual table: {}", name),
	}
}
