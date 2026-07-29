// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::util::ioc::IocContainer;
use reifydb_engine::test_harness::create_test_admin_transaction;
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::params::Params;

use super::{
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
	flow_node_types::SystemFlowNodeTypes,
	flow_nodes::SystemFlowNodes,
	flow_watermarks::SystemFlowWatermarks,
	flows::SystemFlows,
	granted_roles::SystemGrantedRoles,
	handlers::SystemHandlers,
	identities::SystemIdentities,
	identity_attribute_values::SystemIdentityAttributeValues,
	identity_attributes::SystemIdentityAttributes,
	metrics::{MetricsObject, cdc::SystemMetricsCdc, storage::SystemMetricsStorage},
	migrations::SystemMigrations,
	namespaces::SystemNamespaces,
	node_horizon_store::NodeHorizonStore,
	operator_inputs::SystemOperatorInputs,
	operator_outputs::SystemOperatorOutputs,
	operator_store::OperatorStore,
	operators::SystemOperators,
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
};
use crate::{
	catalog::Catalog,
	metrics::storage::metrics::MetricsReader,
	system::SystemCatalog,
	vtable::{BaseVTable, VTableContext},
};

fn all_system_vtables() -> Vec<Box<dyn BaseVTable>> {
	let ioc = IocContainer::new();
	let catalog = Catalog::testing();
	let operators = OperatorStore::new();
	let metrics = MetricsReader::new(SingleStore::testing_memory());

	let metrics_storage = [
		(SystemCatalog::get_system_metrics_storage_table_table(), MetricsObject::Table),
		(SystemCatalog::get_system_metrics_storage_view_table(), MetricsObject::View),
		(SystemCatalog::get_system_metrics_storage_table_virtual_table(), MetricsObject::TableVirtual),
		(SystemCatalog::get_system_metrics_storage_ringbuffer_table(), MetricsObject::RingBuffer),
		(SystemCatalog::get_system_metrics_storage_dictionary_table(), MetricsObject::Dictionary),
		(SystemCatalog::get_system_metrics_storage_series_table(), MetricsObject::Series),
		(SystemCatalog::get_system_metrics_storage_flow_table(), MetricsObject::Flow),
		(SystemCatalog::get_system_metrics_storage_flow_node_table(), MetricsObject::FlowNode),
		(SystemCatalog::get_system_metrics_storage_system_table(), MetricsObject::System),
	];

	let metrics_cdc = [
		(SystemCatalog::get_system_metrics_cdc_table_table(), MetricsObject::Table),
		(SystemCatalog::get_system_metrics_cdc_view_table(), MetricsObject::View),
		(SystemCatalog::get_system_metrics_cdc_table_virtual_table(), MetricsObject::TableVirtual),
		(SystemCatalog::get_system_metrics_cdc_ringbuffer_table(), MetricsObject::RingBuffer),
		(SystemCatalog::get_system_metrics_cdc_dictionary_table(), MetricsObject::Dictionary),
		(SystemCatalog::get_system_metrics_cdc_series_table(), MetricsObject::Series),
		(SystemCatalog::get_system_metrics_cdc_flow_table(), MetricsObject::Flow),
		(SystemCatalog::get_system_metrics_cdc_flow_node_table(), MetricsObject::FlowNode),
		(SystemCatalog::get_system_metrics_cdc_system_table(), MetricsObject::System),
	];

	let mut result: Vec<Box<dyn BaseVTable>> = vec![
		Box::new(SystemSequences::new()),
		Box::new(SystemNamespaces::new()),
		Box::new(SystemTables::new()),
		Box::new(SystemViews::new()),
		Box::new(SystemFlows::new()),
		Box::new(SystemFlowWatermarks::new(ioc.clone())),
		Box::new(SystemSubscriptionWatermarks::new(ioc.clone())),
		Box::new(SystemFlowNodes::new(NodeHorizonStore::new())),
		Box::new(SystemFlowEdges::new()),
		Box::new(SystemColumnsTable::new()),
		Box::new(SystemPrimaryKeys::new()),
		Box::new(SystemPrimaryKeyColumns::new()),
		Box::new(SystemColumnProperties::new()),
		Box::new(SystemVersions::new(ioc.clone())),
		Box::new(SystemCdcConsumers::new()),
		Box::new(SystemOperators::new(operators.clone())),
		Box::new(SystemOperatorInputs::new(operators.clone())),
		Box::new(SystemOperatorOutputs::new(operators.clone())),
		Box::new(SystemDictionaries::new()),
		Box::new(SystemTablesVirtual::new(catalog.clone())),
		Box::new(SystemTypes::new()),
		Box::new(SystemFlowNodeTypes::new()),
		Box::new(SystemRingBuffers::new()),
		Box::new(SystemRowShapes::new(catalog.clone())),
		Box::new(SystemRowShapeFields::new(catalog.clone())),
		Box::new(SystemEnums::new()),
		Box::new(SystemEnumVariants::new()),
		Box::new(SystemEvents::new()),
		Box::new(SystemEventVariants::new()),
		Box::new(SystemHandlers::new(catalog.clone())),
		Box::new(SystemTags::new()),
		Box::new(SystemTagVariants::new()),
		Box::new(SystemSeries::new()),
		Box::new(SystemIdentities::new()),
		Box::new(SystemIdentityAttributes::new()),
		Box::new(SystemIdentityAttributeValues::new()),
		Box::new(SystemRoles::new()),
		Box::new(SystemGrantedRoles::new()),
		Box::new(SystemPolicies::new()),
		Box::new(SystemPolicyOperations::new()),
		Box::new(SystemMigrations::new()),
		Box::new(SystemAuthentications::new()),
		Box::new(SystemConfigs::new(ioc.clone())),
		Box::new(SystemSubscriptions::new(ioc.clone())),
		Box::new(SystemVirtualTableColumns::new(catalog.clone())),
		Box::new(SystemProceduresRql::new()),
		Box::new(SystemProceduresTest::new()),
		Box::new(SystemProceduresNative::new(catalog.clone())),
		Box::new(SystemProceduresFFI::new(catalog.clone())),
		Box::new(SystemProceduresWasm::new(catalog.clone())),
		Box::new(SystemBindingsHttp::new()),
		Box::new(SystemBindingsGrpc::new()),
		Box::new(SystemBindingsWs::new()),
	];

	for (vtable, object) in metrics_storage {
		result.push(Box::new(SystemMetricsStorage::new(vtable, object, metrics.clone())));
	}

	for (vtable, object) in metrics_cdc {
		result.push(Box::new(SystemMetricsCdc::new(vtable, object, metrics.clone())));
	}

	result
}

#[test]
fn every_system_vtable_emits_the_columns_its_schema_declares() {
	let mut txn = create_test_admin_transaction();
	let mut txn = Transaction::Admin(&mut txn);

	let mut failures = Vec::new();
	let mut checked = 0usize;

	for mut handler in all_system_vtables() {
		let table = handler.vtable().name.clone();
		let declared: Vec<String> = handler.vtable().columns.iter().map(|c| c.name.clone()).collect();

		handler.initialize(
			&mut txn,
			VTableContext::Basic {
				params: Params::default(),
			},
		)
		.unwrap_or_else(|e| panic!("{table}: initialize failed: {e:?}"));

		checked += 1;

		let mut batches = 0usize;
		loop {
			match handler.next(&mut txn) {
				Ok(None) => break,
				Ok(Some(batch)) => {
					let emitted: Vec<String> =
						batch.columns.names.iter().map(|n| n.text().to_string()).collect();

					if emitted != declared {
						failures.push(format!(
							"{table} (batch {batches}): emitted columns do not match the \
							 declared schema\n    declared: {declared:?}\n    emitted:  \
							 {emitted:?}"
						));
					}

					batches += 1;
				}
				Err(e) => {
					failures.push(format!(
						"{table}: next() failed, so the declared schema {declared:?} is \
						 unreachable: {}",
						e.diagnostic().message
					));
					break;
				}
			}
		}
	}

	assert!(failures.is_empty(), "system vtable schema drift:\n  {}", failures.join("\n  "));

	assert_eq!(checked, 71, "expected every system vtable handler to be exercised");
}

#[test]
fn every_system_vtable_emits_the_types_its_schema_declares() {
	let mut txn = create_test_admin_transaction();
	let mut txn = Transaction::Admin(&mut txn);

	let mut failures = Vec::new();

	for mut handler in all_system_vtables() {
		let table = handler.vtable().name.clone();
		let declared: Vec<_> =
			handler.vtable().columns.iter().map(|c| (c.name.clone(), c.constraint.get_type())).collect();

		handler.initialize(
			&mut txn,
			VTableContext::Basic {
				params: Params::default(),
			},
		)
		.unwrap_or_else(|e| panic!("{table}: initialize failed: {e:?}"));

		while let Ok(Some(batch)) = handler.next(&mut txn) {
			for (index, (name, expected)) in declared.iter().enumerate() {
				let Some(emitted) = batch.columns.columns.get(index) else {
					continue;
				};

				if emitted.get_type() != *expected {
					failures.push(format!(
						"{table}.{name}: builder allocates a {:?} buffer but the schema \
						 declares {expected:?}",
						emitted.get_type()
					));
				}
			}
		}
	}

	assert!(failures.is_empty(), "system vtable type drift:\n  {}", failures.join("\n  "));
}
