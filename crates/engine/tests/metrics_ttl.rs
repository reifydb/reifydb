// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Bootstrap must seed a row TTL onto every internal metrics snapshot series so the
//! append-only telemetry is evicted instead of growing without bound. Runtime series
//! default to a 7-day retention and profiler series to 1 hour, both in Drop mode (the
//! only mode the row-TTL GC supports). The seeding is apply-once: it only writes row
//! settings for a series that has none yet, so a restart - or a later operator change -
//! is never overwritten.

use reifydb_catalog::{
	bootstrap::bootstrap_system_objects, catalog::Catalog, store::row_settings::create::create_row_settings,
};
use reifydb_core::{
	event::EventBus,
	interface::catalog::{id::SeriesId, shape::ShapeId},
	row::{RowSettings, Ttl, TtlCleanupMode},
};
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	interceptor::interceptors::Interceptors,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::value::{duration::Duration, identity::IdentityId};

fn ttl_of(catalog: &Catalog, admin: &mut AdminTransaction, series: SeriesId) -> Ttl {
	catalog.find_row_settings(&mut Transaction::Admin(admin), ShapeId::Series(series))
		.expect("lookup")
		.expect("metrics series must have row settings seeded at bootstrap")
		.ttl
		.expect("metrics series row settings must carry a ttl")
}

#[test]
fn bootstrap_seeds_metrics_series_ttl_with_per_family_defaults() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("bootstrap must succeed");

	let catalog = Catalog::new(catalog_cache);
	let mut admin = AdminTransaction::new(
		multi,
		single,
		eventbus,
		Interceptors::default(),
		IdentityId::system(),
		Clock::Real,
	)
	.expect("admin transaction");

	// Runtime snapshots retain a week; both runtime series share the family default.
	let memory = ttl_of(&catalog, &mut admin, SeriesId::RUNTIME_MEMORY_SNAPSHOTS);
	assert_eq!(memory.duration, Duration::from_days(7).unwrap(), "runtime memory retention");
	assert_eq!(memory.cleanup_mode, TtlCleanupMode::Drop);

	let watermarks = ttl_of(&catalog, &mut admin, SeriesId::RUNTIME_WATERMARKS_SNAPSHOTS);
	assert_eq!(watermarks.duration, Duration::from_days(7).unwrap(), "runtime watermarks retention");

	// Profiler snapshots are far noisier and retain only an hour.
	let profiler = ttl_of(&catalog, &mut admin, SeriesId::PROFILER_QUERY_SNAPSHOTS);
	assert_eq!(profiler.duration, Duration::from_hours(1).unwrap(), "profiler retention");
	assert_eq!(profiler.cleanup_mode, TtlCleanupMode::Drop);
}

#[test]
fn bootstrap_does_not_overwrite_existing_metrics_series_ttl() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");

	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("first bootstrap");

	let catalog = Catalog::new(catalog_cache.clone());

	// Simulate an operator retuning one series' retention after the initial bootstrap.
	let changed = RowSettings {
		ttl: Some(Ttl {
			duration: Duration::from_minutes(30).unwrap(),
			cleanup_mode: TtlCleanupMode::Drop,
		}),
		persistent: true,
	};
	{
		let mut admin = AdminTransaction::new(
			multi.clone(),
			single.clone(),
			eventbus.clone(),
			Interceptors::default(),
			IdentityId::system(),
			Clock::Real,
		)
		.expect("admin transaction");
		create_row_settings(&mut admin, ShapeId::Series(SeriesId::RUNTIME_MEMORY_SNAPSHOTS), &changed)
			.expect("override row settings");
		admin.commit().expect("commit override");
	}

	// A second bootstrap (a restart) must leave the operator's value untouched.
	bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus).expect("second bootstrap");

	let mut admin = AdminTransaction::new(
		multi,
		single,
		eventbus,
		Interceptors::default(),
		IdentityId::system(),
		Clock::Real,
	)
	.expect("admin transaction");
	let memory = ttl_of(&catalog, &mut admin, SeriesId::RUNTIME_MEMORY_SNAPSHOTS);
	assert_eq!(
		memory.duration,
		Duration::from_minutes(30).unwrap(),
		"a restart must not overwrite an already-seeded metrics series TTL"
	);
}
