// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use reifydb_catalog::vtable::{
	VTableContext,
	system::{
		authentication::{credentials::SystemAuthenticationCredentials, methods::SystemAuthenticationMethods},
		bindings::{
			grpc::SystemBindingGrpc, http::SystemBindingHttp, list::SystemBindings, ws::SystemBindingWs,
		},
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
		functions::{FunctionRow, SystemFunctions},
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
			in_process::SystemProcedureInProcess, list::SystemProcedures, rql::SystemProcedureRql,
			test::SystemProcedureTest,
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
use reifydb_routine_abi::{Arity, FunctionKind, registry::Routines};
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
	} else if namespace.id() == NamespaceId::SYSTEM_ENUM {
		compile_enum_vtable(&table.name)
	} else if namespace.id() == NamespaceId::SYSTEM_EVENT {
		compile_event_vtable(&table.name)
	} else if namespace.id() == NamespaceId::SYSTEM_VIRTUAL_TABLE {
		compile_virtual_table_vtable(&table.name, &context)
	} else if namespace.id() == NamespaceId::SYSTEM_ROW_SHAPE {
		compile_row_shape_vtable(&table.name, &context)
	} else if namespace.id() == NamespaceId::SYSTEM_POLICY {
		compile_policy_vtable(&table.name)
	} else if namespace.id() == NamespaceId::SYSTEM_SUBSCRIPTION {
		compile_subscription_vtable(&table.name, &context)
	} else if namespace.id() == NamespaceId::SYSTEM_PRIMARY_KEY {
		compile_primary_key_vtable(&table.name)
	} else if namespace.id() == NamespaceId::SYSTEM_COLUMN {
		compile_column_vtable(&table.name)
	} else if namespace.id() == NamespaceId::SYSTEM_IDENTITY {
		compile_identity_vtable(&table.name)
	} else if namespace.id() == NamespaceId::SYSTEM_WINDOWED_VIEW_GROUP {
		compile_windowed_view_group_vtable(&table.name)
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
		"flows" => VTables::FlowFlows(SystemFlowFlows::new()),
		"queues" => VTables::QueueQueues(SystemQueueQueues::new()),
		"procedures" => VTables::Procedures(SystemProcedures::new(context.services.catalog.clone())),
		"bindings" => VTables::Bindings(SystemBindings::new()),
		"functions" => VTables::Functions(SystemFunctions::new(function_rows(&context.services.routines))),
		"columns" => VTables::Columns(SystemColumnsTable::new()),
		"primary_keys" => VTables::PrimaryKeys(SystemPrimaryKeys::new()),
		"relationships" => VTables::Relationships(SystemRelationships::new()),
		"windowed_view_groups" => VTables::WindowedViewGroups(SystemWindowedViewGroups::new()),
		"versions" => VTables::Versions(SystemVersions::new(context.services.ioc.clone())),
		"dictionaries" => VTables::Dictionaries(SystemDictionaries::new()),
		"virtual_tables" => VTables::TablesVirtual(SystemTablesVirtual::new(context.services.catalog.clone())),
		"types" => VTables::Types(SystemTypes::new()),
		"ringbuffers" => VTables::RingBuffers(SystemRingBuffers::new()),
		"row_shapes" => VTables::RowShapes(SystemRowShapes::new(context.services.catalog.clone())),
		"enums" => VTables::Enums(SystemEnums::new()),
		"events" => VTables::Events(SystemEvents::new()),
		"handlers" => VTables::Handlers(SystemHandlers::new(context.services.catalog.clone())),
		"series" => VTables::Series(SystemSeries::new()),
		"identities" => VTables::Identities(SystemIdentities::new()),
		"roles" => VTables::Roles(SystemRoles::new()),
		"policies" => VTables::Policies(SystemPolicies::new()),
		"migrations" => VTables::Migrations(SystemMigrations::new()),
		"configs" => VTables::Configs(SystemConfigs::new(context.services.ioc.clone())),
		"subscriptions" => VTables::Subscriptions(SystemSubscriptions::new(context.services.ioc.clone())),
		_ => return None,
	})
}

fn compile_flow_vtable(name: &str, context: &QueryContext) -> Option<VTables> {
	Some(match name {
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
		"partitions" => VTables::QueuePartitions(SystemQueuePartitions::new()),
		_ => return None,
	})
}

fn compile_authentication_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"methods" => VTables::AuthenticationMethods(SystemAuthenticationMethods::new()),
		_ => return None,
	})
}

fn compile_enum_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"variants" => VTables::EnumVariants(SystemEnumVariants::new()),
		_ => return None,
	})
}

fn compile_event_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"variants" => VTables::EventVariants(SystemEventVariants::new()),
		_ => return None,
	})
}

fn compile_virtual_table_vtable(name: &str, context: &QueryContext) -> Option<VTables> {
	Some(match name {
		"columns" => {
			VTables::VirtualTableColumns(SystemVirtualTableColumns::new(context.services.catalog.clone()))
		}
		_ => return None,
	})
}

fn compile_row_shape_vtable(name: &str, context: &QueryContext) -> Option<VTables> {
	Some(match name {
		"fields" => VTables::RowShapeFields(SystemRowShapeFields::new(context.services.catalog.clone())),
		_ => return None,
	})
}

fn compile_policy_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"operations" => VTables::PolicyOperations(SystemPolicyOperations::new()),
		_ => return None,
	})
}

fn compile_subscription_vtable(name: &str, context: &QueryContext) -> Option<VTables> {
	Some(match name {
		"watermarks" => {
			VTables::SubscriptionWatermarks(SystemSubscriptionWatermarks::new(context.services.ioc.clone()))
		}
		_ => return None,
	})
}

fn compile_primary_key_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"columns" => VTables::PrimaryKeyColumns(SystemPrimaryKeyColumns::new()),
		_ => return None,
	})
}

fn compile_column_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"properties" => VTables::ColumnProperties(SystemColumnProperties::new()),
		_ => return None,
	})
}

fn compile_identity_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"attributes" => VTables::IdentityAttributes(SystemIdentityAttributes::new()),
		"attribute_values" => VTables::IdentityAttributeValues(SystemIdentityAttributeValues::new()),
		"roles" => VTables::GrantedRoles(SystemGrantedRoles::new()),
		"credentials" => VTables::AuthenticationCredentials(SystemAuthenticationCredentials::new()),
		_ => return None,
	})
}

fn compile_windowed_view_group_vtable(name: &str) -> Option<VTables> {
	Some(match name {
		"members" => VTables::WindowedViewGroupMembers(SystemWindowedViewGroupMembers::new()),
		_ => return None,
	})
}

fn function_rows(routines: &Routines) -> Vec<FunctionRow> {
	let mut rows = Vec::new();
	for entry in routines.list_functions() {
		let (min_args, max_args) = match entry.function.arity() {
			Arity::Exact(n) => (n as u64, Some(n as u64)),
			Arity::Range(min, max) => (min as u64, Some(max as u64)),
			Arity::AtLeast(n) => (n as u64, None),
			Arity::Any => (0, None),
		};
		let namespace = entry.name.rsplit_once("::").map(|(ns, _)| ns.to_string());
		let description = entry.function.info().description.clone();
		for kind in entry.function.kinds() {
			rows.push(FunctionRow {
				name: entry.name.clone(),
				namespace: namespace.clone(),
				kind: match kind {
					FunctionKind::Scalar => "scalar",
					FunctionKind::Aggregate => "aggregate",
					FunctionKind::Generator => "generator",
				},
				min_args,
				max_args,
				description: description.clone(),
				alias_of: entry.alias_of.clone(),
			});
		}
	}
	rows.sort_by(|a, b| a.name.cmp(&b.name).then(a.kind.cmp(b.kind)));
	rows
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
