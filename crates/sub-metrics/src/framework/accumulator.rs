// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{
	metrics::sample::{MetricKind, Reading},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_value::{
	byte_size::ByteSize,
	count::Count,
	fragment::Fragment,
	reifydb_assertions,
	value::{Value, datetime::DateTime, duration::Duration, value_type::ValueType},
};

use crate::framework::spec::{DomainShape, DomainSpec, MeasureSpec, MetricsDomain, Surface};

#[derive(Clone, Debug)]
pub struct Measure {
	pub metric: &'static str,
	pub reading: Reading,
	pub kind: MetricKind,
}

#[derive(Clone, Debug)]
pub struct MetricsRow {
	pub dimensions: Vec<Value>,
	pub measures: Vec<Measure>,
}

pub struct PublishedSurface {
	pub domain: MetricsDomain,
	pub surface: Surface,
	pub columns: Columns,
}

pub struct MetricsAccumulator {
	domains: BTreeMap<MetricsDomain, DomainState>,
}

struct DomainState {
	spec: DomainSpec,
	rows: BTreeMap<Vec<Value>, RowState>,
}

#[derive(Default)]
struct RowState {
	measures: BTreeMap<&'static str, MeasureState>,
}

enum MeasureState {
	Level {
		latest: Reading,
	},
	Counter {
		total: Reading,
		baseline: Option<f64>,
	},
	Distribution {
		current: Option<Reading>,
		total: Option<Reading>,
	},
}

impl MetricsAccumulator {
	pub fn new(specs: impl IntoIterator<Item = DomainSpec>) -> Self {
		Self {
			domains: specs
				.into_iter()
				.map(|spec| {
					(
						spec.domain,
						DomainState {
							spec,
							rows: BTreeMap::new(),
						},
					)
				})
				.collect(),
		}
	}

	pub fn push(&mut self, domain: MetricsDomain, surface: Surface, rows: Vec<MetricsRow>) {
		let Some(state) = self.domains.get_mut(&domain) else {
			reifydb_assertions! {
				assert!(false, "push for domain {:?} which the accumulator was not built with", domain);
			}
			return;
		};
		let expected_dimensions = match state.spec.shape {
			DomainShape::Long => 1,
			DomainShape::Wide => state.spec.dimensions.len(),
		};
		for row in rows {
			reifydb_assertions! {
				assert!(
					row.dimensions.len() == expected_dimensions,
					"row for domain {:?} carries {} dimensions, spec declares {}",
					domain,
					row.dimensions.len(),
					expected_dimensions
				);
			}
			let row_state = state.rows.entry(row.dimensions).or_default();
			for measure in row.measures {
				row_state.apply(measure, surface);
			}
		}
	}

	pub fn roll(&mut self, now: DateTime) -> Vec<PublishedSurface> {
		let mut published = Vec::new();
		for state in self.domains.values_mut() {
			published.push(PublishedSurface {
				domain: state.spec.domain,
				surface: Surface::Current,
				columns: build_surface(state, now, Surface::Current),
			});
			if state.spec.has_total {
				published.push(PublishedSurface {
					domain: state.spec.domain,
					surface: Surface::Total,
					columns: build_surface(state, now, Surface::Total),
				});
			}
			advance_window(state);
		}
		published
	}
}

impl RowState {
	fn apply(&mut self, measure: Measure, surface: Surface) {
		reifydb_assertions! {
			assert!(
				!matches!(measure.kind, MetricKind::Delta | MetricKind::Dimension),
				"producers push Level, Counter or Distribution; got {:?} for metric {}",
				measure.kind,
				measure.metric
			);
		}
		match measure.kind {
			MetricKind::Level => {
				self.measures.insert(
					measure.metric,
					MeasureState::Level {
						latest: measure.reading,
					},
				);
			}
			MetricKind::Counter => {
				match self.measures.entry(measure.metric).or_insert_with(|| MeasureState::Counter {
					total: measure.reading,
					baseline: None,
				}) {
					MeasureState::Counter {
						total,
						..
					} => *total = measure.reading,
					other => {
						*other = MeasureState::Counter {
							total: measure.reading,
							baseline: None,
						}
					}
				}
			}
			MetricKind::Distribution => {
				let slot = match self.measures.entry(measure.metric).or_insert_with(|| {
					MeasureState::Distribution {
						current: None,
						total: None,
					}
				}) {
					MeasureState::Distribution {
						current,
						total,
					} => match surface {
						Surface::Current => current,
						Surface::Total => total,
					},
					other => {
						*other = MeasureState::Distribution {
							current: None,
							total: None,
						};
						let MeasureState::Distribution {
							current,
							total,
						} = other
						else {
							unreachable!()
						};
						match surface {
							Surface::Current => current,
							Surface::Total => total,
						}
					}
				};
				*slot = Some(measure.reading);
			}
			MetricKind::Delta | MetricKind::Dimension => {}
		}
	}
}

fn advance_window(state: &mut DomainState) {
	for row in state.rows.values_mut() {
		for measure in row.measures.values_mut() {
			match measure {
				MeasureState::Counter {
					total,
					baseline,
				} => *baseline = Some(total.as_f64()),
				MeasureState::Distribution {
					current,
					..
				} => *current = None,
				MeasureState::Level {
					..
				} => {}
			}
		}
	}
}

fn build_surface(state: &DomainState, now: DateTime, surface: Surface) -> Columns {
	match state.spec.shape {
		DomainShape::Long => build_long(state, now, surface),
		DomainShape::Wide => build_wide(state, now, surface),
	}
}

fn build_long(state: &DomainState, now: DateTime, surface: Surface) -> Columns {
	let mut ts = ColumnBuffer::datetime_with_capacity(0);
	let mut scope = ColumnBuffer::utf8_with_capacity(0);
	let mut metric = ColumnBuffer::utf8_with_capacity(0);
	let mut value = ColumnBuffer::float8_with_capacity(0);
	let mut unit = ColumnBuffer::utf8_with_capacity(0);
	let mut kind = ColumnBuffer::utf8_with_capacity(0);

	for (dimensions, row) in &state.rows {
		let Some(Value::Utf8(row_scope)) = dimensions.first() else {
			continue;
		};
		for (name, measure) in &row.measures {
			let published = match (measure, surface) {
				(
					MeasureState::Level {
						latest,
					},
					Surface::Current,
				) => Some((*latest, MetricKind::Level)),
				(
					MeasureState::Counter {
						total,
						baseline,
					},
					Surface::Current,
				) => Some((delta_reading(total, *baseline), MetricKind::Delta)),
				(
					MeasureState::Counter {
						total,
						..
					},
					Surface::Total,
				) => Some((*total, MetricKind::Counter)),
				(
					MeasureState::Distribution {
						current,
						..
					},
					Surface::Current,
				) => current.map(|reading| (reading, MetricKind::Distribution)),
				(
					MeasureState::Distribution {
						total,
						..
					},
					Surface::Total,
				) => total.map(|reading| (reading, MetricKind::Distribution)),
				(
					MeasureState::Level {
						..
					},
					Surface::Total,
				) => None,
			};
			if let Some((reading, published_kind)) = published {
				reifydb_assertions! {
					assert!(
						!(surface == Surface::Current
							&& published_kind == MetricKind::Counter),
						"a long-format ::current publish must never contain a Counter row (scope {}, metric {})",
						row_scope,
						name
					);
				}
				ts.push(now);
				scope.push(row_scope.as_str());
				metric.push(*name);
				value.push(reading.as_f64());
				unit.push(reading.unit());
				kind.push(published_kind.name());
			}
		}
	}

	Columns::new(vec![
		ColumnWithName::new(Fragment::internal("ts"), ts),
		ColumnWithName::new(Fragment::internal("scope"), scope),
		ColumnWithName::new(Fragment::internal("metric"), metric),
		ColumnWithName::new(Fragment::internal("value"), value),
		ColumnWithName::new(Fragment::internal("unit"), unit),
		ColumnWithName::new(Fragment::internal("kind"), kind),
	])
}

fn build_wide(state: &DomainState, now: DateTime, surface: Surface) -> Columns {
	let spec = &state.spec;
	let measures = spec.surface_measures(surface);
	let capacity = state.rows.len();

	let mut ts = ColumnBuffer::datetime_with_capacity(capacity);
	let mut dimension_buffers: Vec<ColumnBuffer> = spec
		.dimensions
		.iter()
		.map(|dimension| ColumnBuffer::with_capacity(dimension.buffer_type(), capacity))
		.collect();
	let mut measure_buffers: Vec<ColumnBuffer> =
		measures.iter().map(|measure| ColumnBuffer::with_capacity(measure.buffer_type(), capacity)).collect();

	for (dimensions, row) in &state.rows {
		ts.push(now);
		for (buffer, value) in dimension_buffers.iter_mut().zip(dimensions) {
			buffer.push_value(value.clone());
		}
		for (buffer, measure) in measure_buffers.iter_mut().zip(&measures) {
			buffer.push_value(wide_value(row.measures.get(measure.name), measure, surface));
		}
	}

	let mut out = vec![ColumnWithName::new(Fragment::internal("ts"), ts)];
	for (dimension, buffer) in spec.dimensions.iter().zip(dimension_buffers) {
		out.push(ColumnWithName::new(Fragment::internal(dimension.name), buffer));
	}
	for (measure, buffer) in measures.iter().zip(measure_buffers) {
		out.push(ColumnWithName::new(Fragment::internal(measure.name), buffer));
	}
	Columns::new(out)
}

fn wide_value(state: Option<&MeasureState>, spec: &MeasureSpec, surface: Surface) -> Value {
	let reading = match (state, surface) {
		(
			Some(MeasureState::Level {
				latest,
			}),
			Surface::Current,
		) => Some(*latest),
		(
			Some(MeasureState::Counter {
				total,
				baseline,
			}),
			Surface::Current,
		) => Some(delta_reading(total, *baseline)),
		(
			Some(MeasureState::Counter {
				total,
				..
			}),
			Surface::Total,
		) => Some(*total),
		(
			Some(MeasureState::Distribution {
				current,
				..
			}),
			Surface::Current,
		) => *current,
		(
			Some(MeasureState::Distribution {
				total,
				..
			}),
			Surface::Total,
		) => *total,
		_ => None,
	};
	match reading {
		Some(reading) => reading_value(&reading, &spec.data_type),
		None => {
			if spec.optional {
				Value::none_of(spec.data_type.clone())
			} else {
				zero_value(&spec.data_type)
			}
		}
	}
}

fn reading_value(reading: &Reading, target: &ValueType) -> Value {
	match target {
		ValueType::Uint2 => Value::Uint2(reading.as_f64() as u16),
		ValueType::Uint4 => Value::Uint4(reading.as_f64() as u32),
		ValueType::Uint8 => Value::Uint8(reading.as_f64() as u64),
		ValueType::Duration => match reading {
			Reading::Duration(duration) => Value::Duration(*duration),
			other => Value::Duration(
				Duration::from_microseconds(other.as_f64().min(9.0e15) as i64).unwrap_or_default(),
			),
		},
		_ => Value::float8(reading.as_f64()),
	}
}

fn zero_value(target: &ValueType) -> Value {
	match target {
		ValueType::Uint2 => Value::Uint2(0),
		ValueType::Uint4 => Value::Uint4(0),
		ValueType::Uint8 => Value::Uint8(0),
		ValueType::Duration => Value::Duration(Duration::zero()),
		ValueType::Utf8 => Value::Utf8(String::new()),
		_ => Value::float8(0.0),
	}
}

fn delta_reading(total: &Reading, baseline: Option<f64>) -> Reading {
	let delta = (total.as_f64() - baseline.unwrap_or(0.0)).max(0.0);
	match total {
		Reading::Heap(_) => Reading::Heap(ByteSize::from_bytes(delta as u64)),
		Reading::Bytes(_) => Reading::Bytes(ByteSize::from_bytes(delta as u64)),
		Reading::Count(_) => Reading::Count(Count::new(delta as u64)),
		Reading::Ratio(_) => Reading::Ratio(delta),
		Reading::Version(_) => Reading::Version(delta as u64),
		Reading::Duration(_) => {
			Reading::Duration(Duration::from_microseconds(delta.min(9.0e15) as i64).unwrap_or_default())
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		metrics::sample::{MetricKind, Reading},
		value::column::columns::Columns,
	};
	use reifydb_value::{
		byte_size::ByteSize,
		count::Count,
		value::{Value, datetime::DateTime, value_type::ValueType},
	};

	use super::{Measure, MetricsAccumulator, MetricsRow, PublishedSurface};
	use crate::framework::spec::{MetricsDomain, Surface};

	fn now(millis: u64) -> DateTime {
		DateTime::from_timestamp_millis(millis).unwrap()
	}

	fn counter_row(scope: &str, metric: &'static str, value: u64) -> MetricsRow {
		MetricsRow {
			dimensions: vec![Value::Utf8(scope.to_string())],
			measures: vec![Measure {
				metric,
				reading: Reading::Count(Count::new(value)),
				kind: MetricKind::Counter,
			}],
		}
	}

	fn level_row(scope: &str, metric: &'static str, value: u64) -> MetricsRow {
		MetricsRow {
			dimensions: vec![Value::Utf8(scope.to_string())],
			measures: vec![Measure {
				metric,
				reading: Reading::Count(Count::new(value)),
				kind: MetricKind::Level,
			}],
		}
	}

	fn surface(published: &[PublishedSurface], domain: MetricsDomain, surface: Surface) -> &Columns {
		&published.iter().find(|p| p.domain == domain && p.surface == surface).unwrap().columns
	}

	fn column_values(columns: &Columns, name: &str) -> Vec<Value> {
		let column = columns.iter().find(|c| c.name().text() == name).unwrap();
		(0..column.data().len()).map(|i| column.data().get_value(i)).collect()
	}

	fn long_value(columns: &Columns, scope: &str, metric: &str) -> Option<(f64, String)> {
		// Long-format rows are (scope, metric) keyed; return (value, kind) for the match.
		let scopes = column_values(columns, "scope");
		let metrics = column_values(columns, "metric");
		let values = column_values(columns, "value");
		let kinds = column_values(columns, "kind");
		for i in 0..scopes.len() {
			if scopes[i] == Value::Utf8(scope.to_string()) && metrics[i] == Value::Utf8(metric.to_string())
			{
				let Value::Float8(v) = values[i] else {
					panic!("value column must be float8")
				};
				let Value::Utf8(k) = kinds[i].clone() else {
					panic!("kind column must be utf8")
				};
				return Some((v.value(), k));
			}
		}
		None
	}

	fn operators_accumulator() -> MetricsAccumulator {
		MetricsAccumulator::new([MetricsDomain::RuntimeOperators.spec()])
	}

	#[test]
	fn counter_first_window_is_the_full_value() {
		// Before the first roll there is no baseline; publishing zero would hide everything
		// that happened between boot and the first tick.
		let mut acc = operators_accumulator();
		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![counter_row("n1", "evictions", 10)]);
		let published = acc.roll(now(1_000));

		let current = surface(&published, MetricsDomain::RuntimeOperators, Surface::Current);
		assert_eq!(long_value(current, "n1", "evictions"), Some((10.0, "delta".to_string())));
		let total = surface(&published, MetricsDomain::RuntimeOperators, Surface::Total);
		assert_eq!(long_value(total, "n1", "evictions"), Some((10.0, "counter".to_string())));
	}

	#[test]
	fn counter_windows_are_deltas_not_since_boot() {
		// 10 -> 30 must publish 20 in ::current; publishing 30 there is exactly the
		// summed-up-in-current disease this redesign removes.
		let mut acc = operators_accumulator();
		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![counter_row("n1", "evictions", 10)]);
		acc.roll(now(1_000));
		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![counter_row("n1", "evictions", 30)]);
		let published = acc.roll(now(2_000));

		let current = surface(&published, MetricsDomain::RuntimeOperators, Surface::Current);
		assert_eq!(long_value(current, "n1", "evictions"), Some((20.0, "delta".to_string())));
		let total = surface(&published, MetricsDomain::RuntimeOperators, Surface::Total);
		assert_eq!(long_value(total, "n1", "evictions"), Some((30.0, "counter".to_string())));
	}

	#[test]
	fn counter_regression_publishes_zero_and_resets_the_baseline() {
		// A producer restart makes the cumulative value go backwards; subtracting the old
		// baseline would wrap into a huge delta.
		let mut acc = operators_accumulator();
		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![counter_row("n1", "evictions", 30)]);
		acc.roll(now(1_000));
		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![counter_row("n1", "evictions", 5)]);
		let published = acc.roll(now(2_000));
		let current = surface(&published, MetricsDomain::RuntimeOperators, Surface::Current);
		assert_eq!(long_value(current, "n1", "evictions"), Some((0.0, "delta".to_string())));

		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![counter_row("n1", "evictions", 8)]);
		let published = acc.roll(now(3_000));
		let current = surface(&published, MetricsDomain::RuntimeOperators, Surface::Current);
		assert_eq!(long_value(current, "n1", "evictions"), Some((3.0, "delta".to_string())));
	}

	#[test]
	fn level_passes_through_and_never_reaches_total() {
		// A level is the answer as-is; folding it into ::total would sum a gauge.
		let mut acc = operators_accumulator();
		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![level_row("n1", "state_bytes", 42)]);
		let published = acc.roll(now(1_000));

		let current = surface(&published, MetricsDomain::RuntimeOperators, Surface::Current);
		assert_eq!(long_value(current, "n1", "state_bytes"), Some((42.0, "level".to_string())));
		let total = surface(&published, MetricsDomain::RuntimeOperators, Surface::Total);
		assert_eq!(long_value(total, "n1", "state_bytes"), None);

		// Without a fresh push the level persists: the last reading stays the answer.
		let published = acc.roll(now(2_000));
		let current = surface(&published, MetricsDomain::RuntimeOperators, Surface::Current);
		assert_eq!(long_value(current, "n1", "state_bytes"), Some((42.0, "level".to_string())));
	}

	#[test]
	fn distribution_routes_by_surface_and_the_window_clears_on_roll() {
		// The window stream feeds ::current and must vanish when its window ends, otherwise a
		// span that stopped firing looks alive forever; the long-horizon stream persists.
		let mut acc = MetricsAccumulator::new([MetricsDomain::Instruments.spec()]);
		let dist = |value: u64| MetricsRow {
			dimensions: vec![Value::Utf8("h".to_string())],
			measures: vec![Measure {
				metric: "p50",
				reading: Reading::Count(Count::new(value)),
				kind: MetricKind::Distribution,
			}],
		};
		acc.push(MetricsDomain::Instruments, Surface::Current, vec![dist(5)]);
		acc.push(MetricsDomain::Instruments, Surface::Total, vec![dist(7)]);
		let published = acc.roll(now(1_000));

		let current = surface(&published, MetricsDomain::Instruments, Surface::Current);
		assert_eq!(long_value(current, "h", "p50"), Some((5.0, "distribution".to_string())));
		let total = surface(&published, MetricsDomain::Instruments, Surface::Total);
		assert_eq!(long_value(total, "h", "p50"), Some((7.0, "distribution".to_string())));

		let published = acc.roll(now(2_000));
		let current = surface(&published, MetricsDomain::Instruments, Surface::Current);
		assert_eq!(long_value(current, "h", "p50"), None, "window distribution must clear on roll");
		let total = surface(&published, MetricsDomain::Instruments, Surface::Total);
		assert_eq!(long_value(total, "h", "p50"), Some((7.0, "distribution".to_string())));
	}

	#[test]
	fn wide_pivot_keeps_dimensions_aligned_with_measures() {
		// Two shards with distinct values; a misaligned pivot would attribute shard 1's
		// bytes to shard 0.
		let mut acc = MetricsAccumulator::new([MetricsDomain::ReadBuffer.spec()]);
		let shard_row = |shard: u16, used: u64, warms: u64| MetricsRow {
			dimensions: vec![Value::Uint2(shard)],
			measures: vec![
				Measure {
					metric: "used",
					reading: Reading::Bytes(ByteSize::from_bytes(used)),
					kind: MetricKind::Level,
				},
				Measure {
					metric: "warms_started",
					reading: Reading::Count(Count::new(warms)),
					kind: MetricKind::Counter,
				},
			],
		};
		acc.push(MetricsDomain::ReadBuffer, Surface::Current, vec![shard_row(0, 100, 3), shard_row(1, 200, 9)]);
		let published = acc.roll(now(1_000));

		let current = surface(&published, MetricsDomain::ReadBuffer, Surface::Current);
		assert_eq!(column_values(current, "shard"), vec![Value::Uint2(0), Value::Uint2(1)]);
		assert_eq!(column_values(current, "used"), vec![Value::Uint8(100), Value::Uint8(200)]);
		assert_eq!(column_values(current, "warms_started"), vec![Value::Uint8(3), Value::Uint8(9)]);

		let total = surface(&published, MetricsDomain::ReadBuffer, Surface::Total);
		assert_eq!(column_values(total, "warms_started"), vec![Value::Uint8(3), Value::Uint8(9)]);
		assert!(
			!total.iter().any(|c| c.name().text() == "used"),
			"levels must not appear in a wide ::total surface"
		);
	}

	#[test]
	fn optional_wide_measure_publishes_none_when_absent() {
		// Lifecycle rows without a freelist must read as none, not as a fabricated zero.
		let mut acc = MetricsAccumulator::new([MetricsDomain::Lifecycle.spec()]);
		acc.push(
			MetricsDomain::Lifecycle,
			Surface::Current,
			vec![MetricsRow {
				dimensions: vec![Value::Utf8("short".to_string()), Value::none_of(ValueType::Utf8)],
				measures: vec![
					Measure {
						metric: "floor_version",
						reading: Reading::Version(12),
						kind: MetricKind::Level,
					},
					Measure {
						metric: "backlog_hint",
						reading: Reading::Count(Count::new(4)),
						kind: MetricKind::Level,
					},
					Measure {
						metric: "work_done",
						reading: Reading::Count(Count::new(7)),
						kind: MetricKind::Counter,
					},
					Measure {
						metric: "slices",
						reading: Reading::Count(Count::new(2)),
						kind: MetricKind::Counter,
					},
					Measure {
						metric: "stuck_slices",
						reading: Reading::Count(Count::new(0)),
						kind: MetricKind::Counter,
					},
					Measure {
						metric: "budget_exhausted_slices",
						reading: Reading::Count(Count::new(0)),
						kind: MetricKind::Counter,
					},
					Measure {
						metric: "gated_slices",
						reading: Reading::Count(Count::new(0)),
						kind: MetricKind::Counter,
					},
				],
			}],
		);
		let published = acc.roll(now(1_000));
		let current = surface(&published, MetricsDomain::Lifecycle, Surface::Current);
		assert_eq!(column_values(current, "floor_version"), vec![Value::Uint8(12)]);
		assert!(
			matches!(column_values(current, "freelist_pages")[0], Value::None { .. }),
			"an absent optional measure must publish none"
		);
	}

	#[test]
	fn ts_is_the_roll_timestamp_on_every_row() {
		// Freshness is part of the contract: a reader must be able to tell when the row was
		// published.
		let mut acc = operators_accumulator();
		acc.push(
			MetricsDomain::RuntimeOperators,
			Surface::Current,
			vec![counter_row("n1", "evictions", 10), level_row("n2", "state_bytes", 5)],
		);
		let stamp = now(123_000);
		let published = acc.roll(stamp);
		for p in &published {
			for value in column_values(&p.columns, "ts") {
				assert_eq!(value, Value::DateTime(stamp));
			}
		}
	}
}
