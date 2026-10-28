// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	bootstrap::bootstrap_system_objects,
	catalog::{
		Catalog,
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	},
};
use reifydb_core::{
	common::TimeSource,
	durability::Durability,
	event::EventBus,
	interface::catalog::id::{ColumnId, NamespaceId, TableId},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{interceptor::interceptors::Interceptors, transaction::admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, identity::IdentityId, value_type::ValueType},
};

fn column(name: &str, ty: ValueType) -> TableColumnToCreate {
	TableColumnToCreate {
		name: Fragment::internal(name),
		fragment: Fragment::internal(name),
		constraint: TypeConstraint::unconstrained(ty),
		properties: vec![],
		auto_increment: false,
		dictionary_id: None,
	}
}

#[test]
#[should_panic(expected = "system::source::completeness has no primary key on object_id")]
fn a_store_whose_completeness_table_predates_the_primary_key_refuses_to_start() {
	// Without the primary key index the keyed advance inserts a second row per object, so bootstrap must stop.
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();
	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	let catalog_api = Catalog::new(catalog_cache.clone());
	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
		Durability::Async,
	)
	.expect("admin transaction");
	let namespace = catalog_api
		.create_namespace_with_id(
			&mut admin,
			NamespaceId::SYSTEM_SOURCE,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "system::source".to_string(),
				local_name: "source".to_string(),
				parent_id: NamespaceId::SYSTEM,
				token: None,
				grpc: None,
			},
		)
		.expect("system::source namespace");
	catalog_api
		.create_table_with_id(
			&mut admin,
			TableId::SOURCE_COMPLETENESS,
			TableToCreate {
				name: Fragment::internal("completeness"),
				namespace: namespace.id(),
				columns: vec![
					column("object_id", ValueType::Uint8),
					column("complete_through", ValueType::DateTime),
				],
				primary_key_columns: None,
				partition_by: vec![],
				time: TimeSource::None,
			},
			&ColumnId::SOURCE_COMPLETENESS_COLUMNS,
		)
		.expect("pre-primary-key completeness table");
	admin.commit().expect("commit old layout");

	let _ = bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus);
}
