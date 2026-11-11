// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::util::ioc::IocContainer;
use reifydb_test_harness::engine::create_test_admin_transaction;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::column_view::ColumnView};

use super::{
	authentication::{credentials::SystemAuthenticationCredentials, methods::SystemAuthenticationMethods},
	bindings::{grpc::SystemBindingGrpc, http::SystemBindingHttp, list::SystemBindings, ws::SystemBindingWs},
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
	operator_libary::OperatorLibrary,
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
};
use crate::{
	catalog::Catalog,
	vtable::{BaseVTable, VTableContext, VTableRegistry},
};

fn all_system_vtables() -> Vec<Box<dyn BaseVTable>> {
	let ioc = IocContainer::new();
	let catalog = Catalog::testing();
	let operators = OperatorLibrary::new();

	let result: Vec<Box<dyn BaseVTable>> = vec![
		Box::new(SystemSequences::new()),
		Box::new(SystemNamespaces::new()),
		Box::new(SystemTables::new()),
		Box::new(SystemViews::new()),
		Box::new(SystemFlowFlows::new()),
		Box::new(SystemFlowWatermarks::new(ioc.clone())),
		Box::new(SystemFlowUnits::new(ioc.clone())),
		Box::new(SystemSubscriptionWatermarks::new(ioc.clone())),
		Box::new(SystemOperators::new()),
		Box::new(SystemFlowEdges::new()),
		Box::new(SystemColumnsTable::new()),
		Box::new(SystemPrimaryKeys::new()),
		Box::new(SystemPrimaryKeyColumns::new()),
		Box::new(SystemColumnProperties::new()),
		Box::new(SystemVersions::new(ioc.clone())),
		Box::new(SystemOperatorLibraries::new(operators.clone())),
		Box::new(SystemOperatorLibraryInputs::new(operators.clone())),
		Box::new(SystemOperatorLibraryOutputs::new(operators.clone())),
		Box::new(SystemDictionaries::new()),
		Box::new(SystemTablesVirtual::new(catalog.clone())),
		Box::new(SystemTypes::new()),
		Box::new(SystemOperatorTypes::new()),
		Box::new(SystemRingBuffers::new()),
		Box::new(SystemRowShapes::new(catalog.clone())),
		Box::new(SystemRowShapeFields::new(catalog.clone())),
		Box::new(SystemEnums::new()),
		Box::new(SystemEnumVariants::new()),
		Box::new(SystemEvents::new()),
		Box::new(SystemEventVariants::new()),
		Box::new(SystemHandlers::new(catalog.clone())),
		Box::new(SystemSeries::new()),
		Box::new(SystemIdentities::new()),
		Box::new(SystemIdentityAttributes::new()),
		Box::new(SystemIdentityAttributeValues::new()),
		Box::new(SystemRoles::new()),
		Box::new(SystemGrantedRoles::new()),
		Box::new(SystemPolicies::new()),
		Box::new(SystemPolicyOperations::new()),
		Box::new(SystemMigrations::new()),
		Box::new(SystemAuthenticationCredentials::new()),
		Box::new(SystemAuthenticationMethods::new()),
		Box::new(SystemConfigs::new(ioc.clone())),
		Box::new(SystemSubscriptions::new(ioc.clone())),
		Box::new(SystemVirtualTableColumns::new(catalog.clone())),
		Box::new(SystemProcedureRql::new()),
		Box::new(SystemProcedureTest::new()),
		Box::new(SystemProcedureInProcess::new(catalog.clone())),
		Box::new(SystemProcedureExternC::new(catalog.clone())),
		Box::new(SystemProcedureExternWasm::new(catalog.clone())),
		Box::new(SystemBindingHttp::new()),
		Box::new(SystemBindingGrpc::new()),
		Box::new(SystemBindingWs::new()),
		Box::new(SystemProcedures::new(catalog.clone())),
		Box::new(SystemBindings::new()),
		Box::new(SystemQueueQueues::new()),
		Box::new(SystemQueuePartitions::new()),
		Box::new(SystemRelationships::new()),
		Box::new(SystemWindowedViewGroups::new()),
		Box::new(SystemWindowedViewGroupMembers::new()),
		Box::new(SystemFunctions::new(vec![FunctionRow {
			name: "math::total".to_string(),
			namespace: Some("math".to_string()),
			kind: "aggregate",
			min_args: 1,
			max_args: Some(2),
			description: Some("sum alias".to_string()),
			alias_of: Some("math::sum".to_string()),
		}])),
	];

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
					let emitted: Vec<String> = batch
						.batch
						.schema_ref()
						.fields()
						.iter()
						.map(|f| f.name().to_string())
						.collect();

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

	assert_eq!(checked, 60, "expected every system vtable handler to be exercised");
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
				let Some(array) = batch.batch.columns().get(index) else {
					continue;
				};
				let emitted = ColumnView::try_from((array, batch.batch.schema_ref().field(index)))
					.unwrap_or_else(|e| panic!("{table}.{name}: unreadable column: {e:?}"));

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

#[test]
fn every_system_vtable_handler_is_listed_in_system_virtual_tables() {
	let mut txn = create_test_admin_transaction();
	let mut txn = Transaction::Admin(&mut txn);

	let handlers: BTreeMap<u64, String> =
		all_system_vtables().iter().map(|h| (h.vtable().id.0, h.vtable().name.clone())).collect();
	let listed_vtables = VTableRegistry::list_vtables(&mut txn).expect("list_vtables must succeed");
	let listed: BTreeMap<u64, String> = listed_vtables.iter().map(|v| (v.id.0, v.name.clone())).collect();

	assert_eq!(listed_vtables.len(), listed.len(), "a vtable id is listed twice");
	let missing: Vec<_> = handlers.iter().filter(|(id, _)| !listed.contains_key(id)).collect();
	let unhandled: Vec<_> = listed.iter().filter(|(id, _)| !handlers.contains_key(id)).collect();
	assert!(missing.is_empty(), "handlers not listed in system::virtual_tables: {missing:?}");
	assert!(unhandled.is_empty(), "listed without a conformance handler: {unhandled:?}");
}
