// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Regression test for the system/user catalog ID split.
//!
//! Bootstrap-created system series (profiler + runtime `snapshots`) must take hardcoded IDs from the reserved
//! `< 16385` block instead of drawing from the shared dynamic `SOURCE_KEY`/`COLUMN_KEY` counters. Otherwise every
//! added system series shifts user source/column IDs upward - the bug this guards against. The invariant we assert
//! is that, after a full bootstrap, the first user-created table and column still land on the reserved boundary
//! (16385), independent of how many system series exist.

use reifydb_catalog::{
	bootstrap::bootstrap_system_objects,
	catalog::{
		Catalog,
		namespace::NamespaceToCreate,
		table::{TableColumnToCreate, TableToCreate},
	},
};
use reifydb_core::{
	event::EventBus,
	interface::catalog::id::{ColumnId, NamespaceId, SeriesId, TableId},
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, identity::IdentityId, value_type::ValueType},
};

#[test]
fn system_series_use_reserved_ids_and_first_user_source_starts_at_16385() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	let catalog_api = Catalog::new(catalog_cache);
	let mut admin = AdminTransaction::new(
		multi.clone(),
		single.clone(),
		eventbus.clone(),
		Interceptors::default(),
		IdentityId::system(),
		Clock::Real,
	)
	.expect("admin transaction");

	let profiler_query = catalog_api
		.find_series_by_name(
			&mut Transaction::Admin(&mut admin),
			NamespaceId::SYSTEM_METRICS_PROFILER_QUERY,
			"snapshots",
		)
		.expect("lookup")
		.expect("profiler query snapshots series must exist");
	assert_eq!(profiler_query.id, SeriesId::PROFILER_QUERY_SNAPSHOTS, "profiler series must use its reserved id");
	assert_eq!(
		profiler_query.columns.first().expect("ts column").id,
		ColumnId::PROFILER_QUERY_SNAPSHOTS_TS,
		"profiler series columns must use their reserved ids"
	);

	let runtime_memory = catalog_api
		.find_series_by_name(
			&mut Transaction::Admin(&mut admin),
			NamespaceId::SYSTEM_METRICS_RUNTIME_MEMORY,
			"snapshots",
		)
		.expect("lookup")
		.expect("runtime memory snapshots series must exist");
	assert_eq!(runtime_memory.id, SeriesId::RUNTIME_MEMORY_SNAPSHOTS, "runtime series must use its reserved id");

	let namespace = catalog_api
		.create_namespace(
			&mut admin,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "app".to_string(),
				local_name: "app".to_string(),
				parent_id: NamespaceId::ROOT,
				grpc: None,
				token: None,
			},
		)
		.expect("create user namespace");

	let table = catalog_api
		.create_table(
			&mut admin,
			TableToCreate {
				name: Fragment::internal("events"),
				namespace: namespace.id(),
				columns: vec![TableColumnToCreate {
					name: Fragment::internal("id"),
					fragment: Fragment::internal("id"),
					constraint: TypeConstraint::unconstrained(ValueType::Int4),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				}],
				retention_strategy: None,
				primary_key_columns: None,
				partition_by: vec![],
				underlying: false,
			},
		)
		.expect("create user table");

	assert_eq!(
		table.id,
		TableId(16385),
		"first user table must start at the reserved boundary regardless of how many system series exist"
	);
	assert!(
		table.columns[0].id.0 >= 16385,
		"first user column must fall in the user range, got {}",
		table.columns[0].id.0
	);
}
