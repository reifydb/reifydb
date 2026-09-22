// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::metrics::sample::{MetricKind, Reading};
use reifydb_profiler::{category::ProfilerCategory, percentile::PercentileHistogram, record::AggregateRecord};
use reifydb_sub_metrics::{
	framework::{
		accumulator::{Measure, MetricsAccumulator, MetricsRow},
		spec::{MetricsDomain, Surface},
	},
	profiler::publish::spans_columns,
};
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration};

#[test]
fn span_lock_wait_past_the_nanosecond_range_publishes_the_exact_duration() {
	// A lock wait sum past the i64 nanosecond range must publish its exact value, never a capped one or zero.
	let mut records = vec![AggregateRecord {
		category: ProfilerCategory::Query,
		span_name: "s".to_string(),
		dimensions: Vec::new(),
		calls: 1,
		total_us: 1,
		self_us: 1,
		histogram: PercentileHistogram::default(),
		extras_sum: [0, 0, 10_000_000_000_000_000, 0],
	}];
	let columns = spans_columns(&mut records, DateTime::from_nanos(0));
	let lock_wait = columns.iter().find(|c| c.name().text() == "lock_wait").expect("a lock_wait column");
	let exact = Duration::new(0, 115_740, 64_000_000_000_000).expect("10^16 microseconds");
	assert_eq!(lock_wait.data().get_value(0), Value::Duration(exact));
}

#[test]
fn a_negative_reading_on_a_duration_measure_fails_the_roll_instead_of_publishing_zero() {
	// A reading that cannot become a Duration must fail the roll, never publish zero or panic on overflow.
	let mut accumulator = MetricsAccumulator::new([MetricsDomain::ProcProcessSched.spec()]);
	accumulator.push(
		MetricsDomain::ProcProcessSched,
		Surface::Current,
		vec![MetricsRow {
			dimensions: Vec::new(),
			measures: vec![Measure {
				metric: "user_time",
				reading: Reading::Ratio(-1e19),
				kind: MetricKind::Cumulative,
			}],
		}],
	);
	assert!(accumulator.roll(DateTime::from_nanos(0)).is_err());
}
