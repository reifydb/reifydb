// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_catalog::vtable::{
	VTableContext,
	system::{
		authentications::SystemAuthentications,
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
		flow_watermarks::SystemFlowWatermarks,
		flows::SystemFlows,
		granted_roles::SystemGrantedRoles,
		handlers::SystemHandlers,
		identities::SystemIdentities,
		identity_attribute_values::SystemIdentityAttributeValues,
		identity_attributes::SystemIdentityAttributes,
		migrations::SystemMigrations,
		namespaces::SystemNamespaces,
		operator_libraries::SystemOperatorLibraries,
		operator_library_inputs::SystemOperatorLibraryInputs,
		operator_library_outputs::SystemOperatorLibraryOutputs,
		operator_types::SystemOperatorTypes,
		operators::SystemOperators,
		policies::SystemPolicies,
		policy_operations::SystemPolicyOperations,
		primary_key_columns::SystemPrimaryKeyColumns,
		primary_keys::SystemPrimaryKeys,
		procedures::{
			ffi::SystemProceduresFFI, native::SystemProceduresNative, rql::SystemProceduresRql,
			test::SystemProceduresTest, wasm::SystemProceduresWasm,
		},
		queues::SystemQueues,
		relationships::SystemRelationships,
		ringbuffers::SystemRingBuffers,
		roles::SystemRoles,
		row_shape_fields::SystemRowShapeFields,
		row_shapes::SystemRowShapes,
		sequences::SystemSequences,
		series::SystemSeries,
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
		"operators" => VTables::Operators(SystemOperators::new()),
		"flow_edges" => VTables::FlowEdges(SystemFlowEdges::new()),
		"columns" => VTables::Columns(SystemColumnsTable::new()),
		"primary_keys" => VTables::PrimaryKeys(SystemPrimaryKeys::new()),
		"primary_key_columns" => VTables::PrimaryKeyColumns(SystemPrimaryKeyColumns::new()),
		"relationships" => VTables::Relationships(SystemRelationships::new()),
		"column_properties" => VTables::ColumnProperties(SystemColumnProperties::new()),
		"versions" => VTables::Versions(SystemVersions::new(context.services.ioc.clone())),
		"cdc_consumers" => VTables::CdcConsumers(SystemCdcConsumers::new()),
		"operator_libraries" => VTables::OperatorLibraries(SystemOperatorLibraries::new(
			context.services.operator_store.clone(),
		)),
		"dictionaries" => VTables::Dictionaries(SystemDictionaries::new()),
		"virtual_tables" => VTables::TablesVirtual(SystemTablesVirtual::new(context.services.catalog.clone())),
		"types" => VTables::Types(SystemTypes::new()),
		"operator_types" => VTables::OperatorTypes(SystemOperatorTypes::new()),
		"operator_library_inputs" => VTables::OperatorLibraryInputs(SystemOperatorLibraryInputs::new(
			context.services.operator_store.clone(),
		)),
		"operator_library_outputs" => VTables::OperatorLibraryOutputs(SystemOperatorLibraryOutputs::new(
			context.services.operator_store.clone(),
		)),
		"ringbuffers" => VTables::RingBuffers(SystemRingBuffers::new()),
		"queues" => VTables::Queues(SystemQueues::new()),
		"row_shapes" => VTables::RowShapes(SystemRowShapes::new(context.services.catalog.clone())),
		"row_shape_fields" => {
			VTables::RowShapeFields(SystemRowShapeFields::new(context.services.catalog.clone()))
		}
		"enums" => VTables::Enums(SystemEnums::new()),
		"enum_variants" => VTables::EnumVariants(SystemEnumVariants::new()),
		"events" => VTables::Events(SystemEvents::new()),
		"event_variants" => VTables::EventVariants(SystemEventVariants::new()),
		"handlers" => VTables::Handlers(SystemHandlers::new(context.services.catalog.clone())),
		"tags" => VTables::Tags(SystemTags::new()),
		"tag_variants" => VTables::TagVariants(SystemTagVariants::new()),
		"series" => VTables::Series(SystemSeries::new()),
		"authentications" => VTables::Authentications(SystemAuthentications::new()),
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
