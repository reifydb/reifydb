// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_catalog::vtable::{
	VTableContext,
	system::{
		authentication::{credentials::SystemAuthenticationCredentials, methods::SystemAuthenticationMethods},
		bindings::{grpc::SystemBindingGrpc, http::SystemBindingHttp, ws::SystemBindingWs},
		column_properties::SystemColumnProperties,
		columns::SystemColumnsTable,
		configs::SystemConfigs,
		dictionaries::SystemDictionaries,
		enum_variants::SystemEnumVariants,
		enums::SystemEnums,
		event_variants::SystemEventVariants,
		events::SystemEvents,
		flow_edges::SystemFlowEdges,
		flow_units::SystemFlowUnits,
		flow_watermarks::SystemFlowWatermarks,
		flows::SystemFlowFlows,
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
			extern_c::SystemProcedureExternC, extern_wasm::SystemProcedureExternWasm,
			in_process::SystemProcedureInProcess, rql::SystemProcedureRql, test::SystemProcedureTest,
		},
		queue_partitions::SystemQueuePartitions,
		queues::SystemQueueQueues,
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
		types::SystemTypes,
		versions::SystemVersions,
		views::SystemViews,
		virtual_table_columns::SystemVirtualTableColumns,
		windowed_view_group_members::SystemWindowedViewGroupMembers,
		windowed_view_groups::SystemWindowedViewGroups,
	},
	tables::VTables,
};
use reifydb_core::{
	error::diagnostic::catalog::virtual_table_not_found, interface::catalog::id::NamespaceId,
	value::column::headers::ColumnHeaders,
};
use reifydb_rql::nodes::TableVirtualScanNode;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::return_error;

use crate::{
	Result,
	vm::volcano::{
		query::{QueryContext, QueryNode},
		scan::vtable::VirtualScanNode,
	},
};

pub(crate) fn compile_virtual_scan(node: TableVirtualScanNode, context: Arc<QueryContext>) -> Box<dyn QueryNode> {
	let namespace = node.source.namespace().def();
	let table = node.source.def();

	let virtual_table_impl: Option<VTables> = if let Some(user_table) =
		context.services.virtual_table_registry.find_by_name(namespace.id(), &table.name)
	{
		Some(user_table)
	} else if namespace.id() == NamespaceId::SYSTEM {
		compile_system_vtable(&table.name, &context)
	} else if namespace.id() == NamespaceId::SYSTEM_FLOW {
		compile_flow_vtable(&table.name, &context)
	} else if namespace.id() == NamespaceId::SYSTEM_FLOW_OPERATOR {
		compile_flow_operator_vtable(&table.name, &context)
	} else if namespace.id() == NamespaceId::SYSTEM_FLOW_OPERATOR_LIBRARY {
		compile_flow_operator_library_vtable(&table.name, &context)
	} else if namespace.id() == NamespaceId::SYSTEM_PROCEDURE {
		compile_procedures_vtable(&table.name, &context)
	} else if namespace.id() == NamespaceId::SYSTEM_BINDING {
		compile_bindings_vtable(&table.name)
	} else if namespace.id() == NamespaceId::SYSTEM_QUEUE {
		compile_queue_vtable(&table.name)
	} else if namespace.id() == NamespaceId::SYSTEM_AUTHENTICATION {
		compile_authentication_vtable(&table.name)
	} else {
		None
	};

	let Some(virtual_table_impl) = virtual_table_impl else {
		return Box::new(UnknownVirtualScanNode {
			namespace: namespace.name().to_string(),
			name: table.name.clone(),
		});
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

fn compile_system_vtable(name: &str, context: &QueryContext) -> Option<VTables> {
	Some(match name {
		"sequences" => VTables::Sequences(SystemSequences::new()),
		"namespaces" => VTables::Namespaces(SystemNamespaces::new()),
		"tables" => VTables::Tables(SystemTables::new()),
		"views" => VTables::Views(SystemViews::new()),
		"subscription_watermarks" => {
			VTables::SubscriptionWatermarks(SystemSubscriptionWatermarks::new(context.services.ioc.clone()))
		}
		"columns" => VTables::Columns(SystemColumnsTable::new()),
		"primary_keys" => VTables::PrimaryKeys(SystemPrimaryKeys::new()),
		"primary_key_columns" => VTables::PrimaryKeyColumns(SystemPrimaryKeyColumns::new()),
		"relationships" => VTables::Relationships(SystemRelationships::new()),
		"windowed_view_groups" => VTables::WindowedViewGroups(SystemWindowedViewGroups::new()),
		"windowed_view_group_members" => {
			VTables::WindowedViewGroupMembers(SystemWindowedViewGroupMembers::new())
		}
		"column_properties" => VTables::ColumnProperties(SystemColumnProperties::new()),
		"versions" => VTables::Versions(SystemVersions::new(context.services.ioc.clone())),
		"dictionaries" => VTables::Dictionaries(SystemDictionaries::new()),
		"virtual_tables" => VTables::TablesVirtual(SystemTablesVirtual::new(context.services.catalog.clone())),
		"types" => VTables::Types(SystemTypes::new()),
		"ringbuffers" => VTables::RingBuffers(SystemRingBuffers::new()),
		"row_shapes" => VTables::RowShapes(SystemRowShapes::new(context.services.catalog.clone())),
		"row_shape_fields" => {
			VTables::RowShapeFields(SystemRowShapeFields::new(context.services.catalog.clone()))
		}
		"enums" => VTables::Enums(SystemEnums::new()),
		"enum_variants" => VTables::EnumVariants(SystemEnumVariants::new()),
		"events" => VTables::Events(SystemEvents::new()),
		"event_variants" => VTables::EventVariants(SystemEventVariants::new()),
		"handlers" => VTables::Handlers(SystemHandlers::new(context.services.catalog.clone())),
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
		_ => return None,
	})
}

fn compile_flow_vtable(name: &str, context: &QueryContext) -> Option<VTables> {
	Some(match name {
		"flows" => VTables::FlowFlows(SystemFlowFlows::new()),
		"operators" => VTables::Operators(SystemOperators::new()),
		"edges" => VTables::FlowEdges(SystemFlowEdges::new()),
		"units" => VTables::FlowUnits(SystemFlowUnits::new(context.services.ioc.clone())),
		"watermarks" => VTables::FlowWatermarks(SystemFlowWatermarks::new(context.services.ioc.clone())),
		_ => return None,
	})
}

fn compile_flow_operator_vtable(name: &str, context: &QueryContext) -> Option<VTables> {
	Some(match name {
		"types" => VTables::OperatorTypes(SystemOperatorTypes::new()),
		"libraries" => {
			VTables::OperatorLibraries(SystemOperatorLibraries::new(context.services.operators.clone()))
		}
		_ => return None,
	})
}

fn compile_flow_operator_library_vtable(name: &str, context: &QueryContext) -> Option<VTables> {
	Some(match name {
		"inputs" => VTables::OperatorLibraryInputs(SystemOperatorLibraryInputs::new(
			context.services.operators.clone(),
		)),
		"outputs" => VTables::OperatorLibraryOutputs(SystemOperatorLibraryOutputs::new(
			context.services.operators.clone(),
		)),
		_ => return None,
	})
}

fn compile_procedures_vtable(name: &str, context: &QueryContext) -> Option<VTables> {
	let catalog = context.services.catalog.clone();
	Some(match name {
		"rql" => VTables::ProcedureRql(SystemProcedureRql::new()),
		"test" => VTables::ProcedureTest(SystemProcedureTest::new()),
		"in_process" => VTables::ProcedureInProcess(SystemProcedureInProcess::new(catalog)),
		"extern_c" => VTables::ProcedureExternC(SystemProcedureExternC::new(catalog)),
		"extern_wasm" => VTables::ProcedureExternWasm(SystemProcedureExternWasm::new(catalog)),
		_ => return None,
	})
}

fn compile_bindings_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"http" => VTables::BindingHttp(SystemBindingHttp::new()),
		"grpc" => VTables::BindingGrpc(SystemBindingGrpc::new()),
		"ws" => VTables::BindingWs(SystemBindingWs::new()),
		_ => return None,
	})
}

fn compile_queue_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"queues" => VTables::QueueQueues(SystemQueueQueues::new()),
		"partitions" => VTables::QueuePartitions(SystemQueuePartitions::new()),
		_ => return None,
	})
}

fn compile_authentication_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"credentials" => VTables::AuthenticationCredentials(SystemAuthenticationCredentials::new()),
		"methods" => VTables::AuthenticationMethods(SystemAuthenticationMethods::new()),
		_ => return None,
	})
}

struct UnknownVirtualScanNode {
	namespace: String,
	name: String,
}

impl QueryNode for UnknownVirtualScanNode {
	fn initialize<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		return_error!(virtual_table_not_found(&self.namespace, &self.name));
	}

	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		Ok(None)
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		None
	}
}
