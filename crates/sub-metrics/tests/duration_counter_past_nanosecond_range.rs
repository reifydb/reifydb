// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::metrics::sample::{MetricKind, Reading};
use reifydb_sub_metrics::framework::{
	accumulator::{Measure, MetricsAccumulator, MetricsRow},
	spec::{MetricsDomain, Surface},
};
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration};

fn published(accumulator: &mut MetricsAccumulator, domain: MetricsDomain, surface: Surface, metric: &str) -> Value {
	let surfaces = accumulator.roll(DateTime::from_nanos(0)).expect("the roll succeeds");
	let columns = &surfaces
		.iter()
		.find(|published| published.domain == domain && published.surface == surface)
		.expect("the surface is published")
		.columns;
	columns.iter().find(|c| c.name().text() == metric).expect("the measure column").data().get_value(0)
}

#[test]
fn a_duration_counter_delta_past_the_nanosecond_range_publishes_the_exact_duration_not_zero() {
	// A counter total past the i64 nanosecond range must publish its exact delta, never a delta of zero.
	let total = Duration::from_micros_infallible(10_000_000_000_000_000);
	let mut accumulator = MetricsAccumulator::new([MetricsDomain::ProfilerSpans.spec()]);
	accumulator.push(
		MetricsDomain::ProfilerSpans,
		Surface::Current,
		vec![MetricsRow {
			dimensions: ["query", "s", "", ""].into_iter().map(|d| Value::Utf8(d.to_string())).collect(),
			measures: vec![Measure {
				metric: "lock_wait",
				reading: Reading::Duration(total),
				kind: MetricKind::Counter,
			}],
		}],
	);
	let lock_wait = published(&mut accumulator, MetricsDomain::ProfilerSpans, Surface::Current, "lock_wait");
	assert_eq!(lock_wait, Value::Duration(total));
}
