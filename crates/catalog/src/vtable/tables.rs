// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::{interface::catalog::vtable::VTable, value::column::columns::Columns};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::params::Params;

use super::{
	BaseVTable, Batch, VTableContext,
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
		metrics::{cdc::SystemMetricsCdc, storage::SystemMetricsStorage},
		migrations::SystemMigrations,
		namespaces::SystemNamespaces,
		operator_retention_strategies::SystemOperatorRetentionStrategies,
		policies::SystemPolicies,
		policy_operations::SystemPolicyOperations,
		primary_key_columns::SystemPrimaryKeyColumns,
		primary_keys::SystemPrimaryKeys,
		procedures::{
			ffi::SystemProceduresFfi, native::SystemProceduresNative, rql::SystemProceduresRql,
			test::SystemProceduresTest, wasm::SystemProceduresWasm,
		},
		ringbuffers::SystemRingBuffers,
		roles::SystemRoles,
		sequences::SystemSequences,
		series::SystemSeries,
		shape_fields::SystemShapeFields,
		shape_retention_strategies::SystemShapeRetentionStrategies,
		shapes::SystemShapes,
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
};
use crate::Result;

pub type UserVTableDataFunction = Arc<dyn Fn(&Params) -> Columns + Send + Sync>;

pub enum VTables {
	Sequences(SystemSequences),
	Namespaces(SystemNamespaces),
	Tables(SystemTables),
	Views(SystemViews),
	Flows(SystemFlows),
	FlowWatermarks(SystemFlowWatermarks),
	FlowNodes(SystemFlowNodes),
	FlowEdges(SystemFlowEdges),
	Columns(SystemColumnsTable),
	PrimaryKeys(SystemPrimaryKeys),
	PrimaryKeyColumns(SystemPrimaryKeyColumns),
	ColumnProperties(SystemColumnProperties),
	Versions(SystemVersions),
	ShapeRetentionStrategies(SystemShapeRetentionStrategies),
	OperatorRetentionStrategies(SystemOperatorRetentionStrategies),
	CdcConsumers(SystemCdcConsumers),

	FlowOperators(SystemFlowOperators),
	FlowOperatorInputs(SystemFlowOperatorInputs),
	FlowOperatorOutputs(SystemFlowOperatorOutputs),
	Dictionaries(SystemDictionaries),
	TablesVirtual(SystemTablesVirtual),
	Types(SystemTypes),
	FlowNodeTypes(SystemFlowNodeTypes),
	RingBuffers(SystemRingBuffers),
	MetricsStorage(SystemMetricsStorage),
	MetricsCdc(SystemMetricsCdc),
	Shapes(SystemShapes),
	ShapeFields(SystemShapeFields),
	Enums(SystemEnums),
	EnumVariants(SystemEnumVariants),
	Events(SystemEvents),
	EventVariants(SystemEventVariants),
	Handlers(SystemHandlers),
	Tags(SystemTags),
	TagVariants(SystemTagVariants),
	Series(SystemSeries),
	ProceduresRql(SystemProceduresRql),
	ProceduresTest(SystemProceduresTest),
	ProceduresNative(SystemProceduresNative),
	ProceduresFfi(SystemProceduresFfi),
	ProceduresWasm(SystemProceduresWasm),
	BindingsHttp(SystemBindingsHttp),
	BindingsGrpc(SystemBindingsGrpc),
	BindingsWs(SystemBindingsWs),
	Identities(SystemIdentities),
	Roles(SystemRoles),
	GrantedRoles(SystemGrantedRoles),
	Policies(SystemPolicies),
	PolicyOperations(SystemPolicyOperations),
	Migrations(SystemMigrations),

	Configs(SystemConfigs),
	Subscriptions(SystemSubscriptions),
	VirtualTableColumns(SystemVirtualTableColumns),

	UserDefined {
		vtable: Arc<VTable>,
		data_fn: UserVTableDataFunction,

		params: Option<Params>,
		exhausted: bool,
	},
}

impl VTables {
	pub fn vtable(&self) -> &VTable {
		match self {
			Self::Sequences(t) => &t.vtable,
			Self::Namespaces(t) => &t.vtable,
			Self::Tables(t) => &t.vtable,
			Self::Views(t) => &t.vtable,
			Self::Flows(t) => &t.vtable,
			Self::FlowWatermarks(t) => &t.vtable,
			Self::FlowNodes(t) => &t.vtable,
			Self::FlowEdges(t) => &t.vtable,
			Self::Columns(t) => &t.vtable,
			Self::PrimaryKeys(t) => &t.vtable,
			Self::PrimaryKeyColumns(t) => &t.vtable,
			Self::ColumnProperties(t) => &t.vtable,
			Self::Versions(t) => &t.vtable,
			Self::ShapeRetentionStrategies(t) => &t.vtable,
			Self::OperatorRetentionStrategies(t) => &t.vtable,
			Self::CdcConsumers(t) => &t.vtable,
			Self::FlowOperators(t) => &t.vtable,
			Self::FlowOperatorInputs(t) => &t.vtable,
			Self::FlowOperatorOutputs(t) => &t.vtable,
			Self::Dictionaries(t) => &t.vtable,
			Self::TablesVirtual(t) => &t.vtable,
			Self::Types(t) => &t.vtable,
			Self::FlowNodeTypes(t) => &t.vtable,
			Self::RingBuffers(t) => &t.vtable,
			Self::MetricsStorage(t) => &t.vtable,
			Self::MetricsCdc(t) => &t.vtable,
			Self::Shapes(t) => &t.vtable,
			Self::ShapeFields(t) => &t.vtable,
			Self::Enums(t) => &t.vtable,
			Self::EnumVariants(t) => &t.vtable,
			Self::Events(t) => &t.vtable,
			Self::EventVariants(t) => &t.vtable,
			Self::Handlers(t) => &t.vtable,
			Self::Tags(t) => &t.vtable,
			Self::TagVariants(t) => &t.vtable,
			Self::Series(t) => &t.vtable,
			Self::ProceduresRql(t) => &t.vtable,
			Self::ProceduresTest(t) => &t.vtable,
			Self::ProceduresNative(t) => &t.vtable,
			Self::ProceduresFfi(t) => &t.vtable,
			Self::ProceduresWasm(t) => &t.vtable,
			Self::BindingsHttp(t) => &t.vtable,
			Self::BindingsGrpc(t) => &t.vtable,
			Self::BindingsWs(t) => &t.vtable,
			Self::Identities(t) => &t.vtable,
			Self::Roles(t) => &t.vtable,
			Self::GrantedRoles(t) => &t.vtable,
			Self::Policies(t) => &t.vtable,
			Self::PolicyOperations(t) => &t.vtable,
			Self::Migrations(t) => &t.vtable,
			Self::Configs(t) => &t.vtable,
			Self::Subscriptions(t) => &t.vtable,
			Self::VirtualTableColumns(t) => &t.vtable,
			Self::UserDefined {
				vtable,
				..
			} => vtable,
		}
	}

	pub fn initialize(&mut self, txn: &mut Transaction<'_>, ctx: VTableContext) -> Result<()> {
		match self {
			Self::Sequences(t) => t.initialize(txn, ctx),
			Self::Namespaces(t) => t.initialize(txn, ctx),
			Self::Tables(t) => t.initialize(txn, ctx),
			Self::Views(t) => t.initialize(txn, ctx),
			Self::Flows(t) => t.initialize(txn, ctx),
			Self::FlowWatermarks(t) => t.initialize(txn, ctx),
			Self::FlowNodes(t) => t.initialize(txn, ctx),
			Self::FlowEdges(t) => t.initialize(txn, ctx),
			Self::Columns(t) => t.initialize(txn, ctx),
			Self::PrimaryKeys(t) => t.initialize(txn, ctx),
			Self::PrimaryKeyColumns(t) => t.initialize(txn, ctx),
			Self::ColumnProperties(t) => t.initialize(txn, ctx),
			Self::Versions(t) => t.initialize(txn, ctx),
			Self::ShapeRetentionStrategies(t) => t.initialize(txn, ctx),
			Self::OperatorRetentionStrategies(t) => t.initialize(txn, ctx),
			Self::CdcConsumers(t) => t.initialize(txn, ctx),
			Self::FlowOperators(t) => t.initialize(txn, ctx),
			Self::FlowOperatorInputs(t) => t.initialize(txn, ctx),
			Self::FlowOperatorOutputs(t) => t.initialize(txn, ctx),
			Self::Dictionaries(t) => t.initialize(txn, ctx),
			Self::TablesVirtual(t) => t.initialize(txn, ctx),
			Self::Types(t) => t.initialize(txn, ctx),
			Self::FlowNodeTypes(t) => t.initialize(txn, ctx),
			Self::RingBuffers(t) => t.initialize(txn, ctx),
			Self::MetricsStorage(t) => t.initialize(txn, ctx),
			Self::MetricsCdc(t) => t.initialize(txn, ctx),
			Self::Shapes(t) => t.initialize(txn, ctx),
			Self::ShapeFields(t) => t.initialize(txn, ctx),
			Self::Enums(t) => t.initialize(txn, ctx),
			Self::EnumVariants(t) => t.initialize(txn, ctx),
			Self::Events(t) => t.initialize(txn, ctx),
			Self::EventVariants(t) => t.initialize(txn, ctx),
			Self::Handlers(t) => t.initialize(txn, ctx),
			Self::Tags(t) => t.initialize(txn, ctx),
			Self::TagVariants(t) => t.initialize(txn, ctx),
			Self::Series(t) => t.initialize(txn, ctx),
			Self::ProceduresRql(t) => t.initialize(txn, ctx),
			Self::ProceduresTest(t) => t.initialize(txn, ctx),
			Self::ProceduresNative(t) => t.initialize(txn, ctx),
			Self::ProceduresFfi(t) => t.initialize(txn, ctx),
			Self::ProceduresWasm(t) => t.initialize(txn, ctx),
			Self::BindingsHttp(t) => t.initialize(txn, ctx),
			Self::BindingsGrpc(t) => t.initialize(txn, ctx),
			Self::BindingsWs(t) => t.initialize(txn, ctx),
			Self::Identities(t) => t.initialize(txn, ctx),
			Self::Roles(t) => t.initialize(txn, ctx),
			Self::GrantedRoles(t) => t.initialize(txn, ctx),
			Self::Policies(t) => t.initialize(txn, ctx),
			Self::PolicyOperations(t) => t.initialize(txn, ctx),
			Self::Migrations(t) => t.initialize(txn, ctx),
			Self::Configs(t) => t.initialize(txn, ctx),
			Self::Subscriptions(t) => t.initialize(txn, ctx),
			Self::VirtualTableColumns(t) => t.initialize(txn, ctx),
			Self::UserDefined {
				params: stored_params,
				exhausted,
				..
			} => {
				*stored_params = Some(match ctx {
					VTableContext::Basic {
						params,
					} => params,
					VTableContext::PushDown {
						params,
						..
					} => params,
				});
				*exhausted = false;
				Ok(())
			}
		}
	}

	pub fn next(&mut self, txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
		match self {
			Self::Sequences(t) => t.next(txn),
			Self::Namespaces(t) => t.next(txn),
			Self::Tables(t) => t.next(txn),
			Self::Views(t) => t.next(txn),
			Self::Flows(t) => t.next(txn),
			Self::FlowWatermarks(t) => t.next(txn),
			Self::FlowNodes(t) => t.next(txn),
			Self::FlowEdges(t) => t.next(txn),
			Self::Columns(t) => t.next(txn),
			Self::PrimaryKeys(t) => t.next(txn),
			Self::PrimaryKeyColumns(t) => t.next(txn),
			Self::ColumnProperties(t) => t.next(txn),
			Self::Versions(t) => t.next(txn),
			Self::ShapeRetentionStrategies(t) => t.next(txn),
			Self::OperatorRetentionStrategies(t) => t.next(txn),
			Self::CdcConsumers(t) => t.next(txn),
			Self::FlowOperators(t) => t.next(txn),
			Self::FlowOperatorInputs(t) => t.next(txn),
			Self::FlowOperatorOutputs(t) => t.next(txn),
			Self::Dictionaries(t) => t.next(txn),
			Self::TablesVirtual(t) => t.next(txn),
			Self::Types(t) => t.next(txn),
			Self::FlowNodeTypes(t) => t.next(txn),
			Self::RingBuffers(t) => t.next(txn),
			Self::MetricsStorage(t) => t.next(txn),
			Self::MetricsCdc(t) => t.next(txn),
			Self::Shapes(t) => t.next(txn),
			Self::ShapeFields(t) => t.next(txn),
			Self::Enums(t) => t.next(txn),
			Self::EnumVariants(t) => t.next(txn),
			Self::Events(t) => t.next(txn),
			Self::EventVariants(t) => t.next(txn),
			Self::Handlers(t) => t.next(txn),
			Self::Tags(t) => t.next(txn),
			Self::TagVariants(t) => t.next(txn),
			Self::Series(t) => t.next(txn),
			Self::Identities(t) => t.next(txn),
			Self::Roles(t) => t.next(txn),
			Self::GrantedRoles(t) => t.next(txn),
			Self::Policies(t) => t.next(txn),
			Self::ProceduresRql(t) => t.next(txn),
			Self::ProceduresTest(t) => t.next(txn),
			Self::ProceduresNative(t) => t.next(txn),
			Self::ProceduresFfi(t) => t.next(txn),
			Self::ProceduresWasm(t) => t.next(txn),
			Self::BindingsHttp(t) => t.next(txn),
			Self::BindingsGrpc(t) => t.next(txn),
			Self::BindingsWs(t) => t.next(txn),
			Self::PolicyOperations(t) => t.next(txn),
			Self::Migrations(t) => t.next(txn),
			Self::Configs(t) => t.next(txn),
			Self::Subscriptions(t) => t.next(txn),
			Self::VirtualTableColumns(t) => t.next(txn),
			Self::UserDefined {
				data_fn,
				params: stored_params,
				exhausted,
				..
			} => {
				if *exhausted {
					return Ok(None);
				}

				let default_params = Params::default();
				let params_ref = stored_params.as_ref().unwrap_or(&default_params);
				let columns = data_fn(params_ref);

				*exhausted = true;
				Ok(Some(Batch {
					columns,
				}))
			}
		}
	}
}
