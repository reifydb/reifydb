// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Read-buffer metrics domain: catalog surface and vtable registration.
//!
//! Bootstraps the system objects (which must create the read_buffer parent namespace, the three child
//! namespaces, and their unpopulated snapshots series), registers the three CurrentVTables over a store
//! without a read tier, and asserts every surface is queryable by its RQL path. The testing store has no
//! persistent tier and therefore no read buffer, so every current table must be empty rather than erroring;
//! the snapshots series must exist and stay empty because nothing writes them yet. The column specs are
//! pinned to the approved schema widths so the vtable and the snapshots series cannot drift apart silently.

use reifydb_catalog::bootstrap::bootstrap_system_objects;
use reifydb_core::event::EventBus;
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::context::clock::Clock;
use reifydb_store_multi::MultiStore;
use reifydb_sub_metric::{
	domains::read_buffer::{ReadBufferDomain, read_buffer_sources},
	framework::current::CurrentVTable,
};

#[test]
fn read_buffer_current_and_snapshots_are_queryable_after_bootstrap() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	let store = MultiStore::testing_memory();
	for source in read_buffer_sources(&store) {
		let namespace = source.namespace();
		engine.register_virtual_table(namespace, "current", CurrentVTable::new(source, Clock::Real))
			.expect("register read_buffer current vtable");
	}

	for table in ["shards", "warms", "reads"] {
		let current = test_engine.query(&format!("from system::metrics::read_buffer::{table}::current"));
		assert_eq!(
			TestEngine::row_count(&current),
			0,
			"{table}::current must be queryable and empty for a store without a read tier"
		);

		let snapshots = test_engine.query(&format!("from system::metrics::read_buffer::{table}::snapshots"));
		assert_eq!(
			TestEngine::row_count(&snapshots),
			0,
			"{table}::snapshots must exist from bootstrap and stay unpopulated"
		);
	}
}

#[test]
fn read_buffer_column_specs_match_the_snapshot_schemas() {
	// The snapshots series widths are fixed at compile time by the
	// ColumnId arrays (13/10/8); the current vtables must declare the
	// same shape or the two surfaces of one domain would disagree.
	let widths = [(ReadBufferDomain::Shards, 13), (ReadBufferDomain::Warms, 10), (ReadBufferDomain::Reads, 8)];
	for (domain, expected) in widths {
		let columns = domain.columns();
		assert_eq!(columns.len(), expected, "{domain:?} column count");
		assert_eq!(columns[0].name, "ts");
		assert_eq!(columns[1].name, "domain");
		assert_eq!(columns[2].name, "shard");
	}
}
