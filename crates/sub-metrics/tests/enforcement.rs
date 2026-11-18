// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The gauge rule is enforced where tables are born: registration of a `current` table carrying a Counter
//! column must fail at boot, not surface wrong numbers at query time.

use arrow_array::RecordBatch;
use reifydb_catalog::{
	bootstrap::bootstrap_system_objects,
	vtable::user::{UserVTable, UserVTableColumn},
};
use reifydb_core::{
	event::EventBus, interface::catalog::id::NamespaceId, metrics::sample::MetricKind, value::batch::empty_batch,
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::value_type::ValueType;

#[derive(Clone)]
struct CounterTable;

impl UserVTable for CounterTable {
	fn vtable(&self) -> Vec<UserVTableColumn> {
		vec![
			UserVTableColumn::new("ts", ValueType::DateTime),
			UserVTableColumn::measure("evictions", ValueType::Uint8, MetricKind::Counter),
		]
	}

	fn get(&self) -> RecordBatch {
		empty_batch()
	}
}

#[test]
fn a_current_table_with_a_counter_column_is_rejected_at_registration() {
	// Failing at boot beats CI: a Counter in ::current is exactly the partially-sums-up
	// inconsistency the redesign removed, and no code path may reintroduce it silently.
	let engine = TestEngine::new();
	let result = (*engine).register_virtual_table(NamespaceId::SYSTEM_METRIC, "current", CounterTable);
	assert!(result.is_err(), "a Counter column in a table named 'current' must be rejected");
}

#[test]
fn the_same_columns_register_fine_under_any_other_name() {
	// The rule is about the ::current contract, not about counters in general; ::total is
	// exactly where cumulative counters belong.
	let engine = TestEngine::new();
	let services = engine.services();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");
	bootstrap_system_objects(
		engine.multi(),
		engine.single(),
		&services.catalog.cache().clone(),
		&eventbus,
		&services.runtime_context,
	)
	.expect("bootstrap must succeed");
	let result = (*engine).register_virtual_table(NamespaceId::SYSTEM_METRIC, "total", CounterTable);
	assert!(result.is_ok(), "a Counter column outside 'current' must register: {:?}", result.err());
}
