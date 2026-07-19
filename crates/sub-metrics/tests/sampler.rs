// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! With a sample interval configured, each metrics domain must accumulate timestamped rows in its
//! system::metrics::runtime::<domain>::snapshots series.

use std::thread;

use reifydb_core::{event::EventBus, metrics::registry::MetricsRegistry};
use reifydb_engine::test_harness::TestEngine;
use reifydb_runtime::{actor::system::ActorSpawner, context::clock::Clock};
use reifydb_sub_metrics::{
	domains::runtime::{SampleReader, collect::Collectors},
	sampler::MetricsSamplerActor,
};
use reifydb_value::value::duration::Duration;

#[test]
fn sampler_appends_rows_to_the_memory_snapshots_series() {
	let test_engine = TestEngine::new();
	let engine = (*test_engine).clone();

	let services = engine.services();
	let multi = engine.multi().clone();
	let single = engine.single().clone();
	let catalog_cache = services.catalog.cache().clone();
	let eventbus: EventBus = services.ioc.resolve::<EventBus>().expect("EventBus must be in TestEngine IoC");
	let spawner: ActorSpawner =
		services.ioc.resolve::<ActorSpawner>().expect("ActorSpawner must be in TestEngine IoC");

	reifydb_catalog::bootstrap::bootstrap_system_objects(&multi, &single, &catalog_cache, &eventbus)
		.expect("bootstrap must succeed");

	let collectors = Collectors {
		engine: engine.clone(),
		registry: MetricsRegistry::new(),
	};
	let reader = SampleReader::new(collectors);

	let actor =
		MetricsSamplerActor::new(engine.clone(), reader, Clock::Real, Duration::from_milliseconds(10).unwrap());
	spawner.spawn_coordination("metrics-sampler", actor);

	thread::sleep(Duration::from_milliseconds(200).unwrap().to_std());

	let memory = test_engine.query("from system::metrics::runtime::memory::snapshots");
	assert!(
		TestEngine::row_count(&memory) > 0,
		"the sampler must append process/derived memory samples to the memory snapshots series"
	);
}
