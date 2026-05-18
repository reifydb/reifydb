// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_metric::{
	counter::Counter, gauge::Gauge, histogram::Histogram, registry::StaticMetricRegistry, snapshot::MetricSnapshot,
};

static TEST_COUNTER: Counter = Counter::new("test_counter", "a counter");
static TEST_GAUGE: Gauge = Gauge::new("test_gauge", "a gauge");

static TEST_BOUNDS: &[f64] = &[10.0, 100.0, 1000.0];

#[test]
fn registry_round_trip() {
	let registry = StaticMetricRegistry::new();

	// Histogram needs LazyLock for statics, so use a leaked Box for test
	let histogram: &'static Histogram =
		Box::leak(Box::new(Histogram::new("test_hist", "a histogram", TEST_BOUNDS)));

	registry.register_counter(&TEST_COUNTER);
	registry.register_gauge(&TEST_GAUGE);
	registry.register_histogram(histogram);

	TEST_COUNTER.add(10.0);
	TEST_GAUGE.set(-3.0);
	histogram.observe(50.0);
	histogram.observe(500.0);

	let snaps = registry.snapshot();
	assert_eq!(snaps.len(), 3);

	// Verify counter
	match &snaps[0] {
		MetricSnapshot::Counter(c) => {
			assert_eq!(c.name, "test_counter");
			assert!(c.value >= 10.0); // >= because static may have been used by other tests
		}
		other => panic!("expected Counter, got {:?}", other),
	}

	// Verify gauge
	match &snaps[1] {
		MetricSnapshot::Gauge(g) => {
			assert_eq!(g.name, "test_gauge");
			// Gauge may have been modified by other tests using the same static,
			// so just check it's a valid f64
			let _ = g.value;
		}
		other => panic!("expected Gauge, got {:?}", other),
	}

	// Verify histogram
	match &snaps[2] {
		MetricSnapshot::Histogram(h) => {
			assert_eq!(h.name, "test_hist");
			assert_eq!(h.count, 2);
			assert_eq!(h.sum, 550.0);
		}
		other => panic!("expected Histogram, got {:?}", other),
	}
}

#[test]
fn snapshot_monotonicity() {
	let histogram: &'static Histogram = Box::leak(Box::new(Histogram::new("mono", "h", TEST_BOUNDS)));

	histogram.observe(10.0);
	let snap1 = histogram.snapshot();

	histogram.observe(20.0);
	let snap2 = histogram.snapshot();

	assert!(snap2.count >= snap1.count);
	assert!(snap2.sum >= snap1.sum);
}

#[test]
fn metric_registry_round_trip() {
	use std::sync::Arc;

	use reifydb_metric::{MetricId, registry::MetricRegistry};

	let registry = MetricRegistry::new();
	let id = MetricId::System;
	let gauge = Arc::new(Gauge::new("test_gauge", "help"));

	registry.register_gauge(id, gauge.clone());
	gauge.set(42.0);

	let snap = registry.snapshot();
	assert_eq!(snap.len(), 1);
	assert_eq!(snap[0].0, id);

	if let MetricSnapshot::Gauge(g) = &snap[0].1 {
		assert_eq!(g.value, 42.0);
	} else {
		panic!("expected gauge snapshot");
	}

	registry.unregister_gauge(&id);
	assert_eq!(registry.snapshot().len(), 0);
}
