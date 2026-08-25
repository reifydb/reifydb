// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, BTreeSet};

use reifydb_core::{
	internal_error,
	metrics::sample::{MetricKind, Reading},
	value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
};
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	error::Error,
	fragment::Fragment,
	reifydb_assertions,
	value::{Value, datetime::DateTime, duration::Duration, value_type::ValueType},
};

use crate::framework::spec::{DomainShape, DomainSpec, MeasureSpec, MetricsDomain, PushKind, Surface};

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
	Cumulative {
		latest: Reading,
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
		if domain.push_kind() == PushKind::Census && surface == Surface::Current {
			let present: BTreeSet<&Vec<Value>> = rows.iter().map(|row| &row.dimensions).collect();
			state.rows.retain(|dimensions, _| present.contains(dimensions));
		}
		for row in rows {
			let expected_dimensions = match state.spec.shape {
				DomainShape::Long => 1,
				DomainShape::Wide => state.spec.dimensions.len(),
			};
			assert!(
				row.dimensions.len() == expected_dimensions,
				"row for domain {:?} carries {} dimensions, spec declares {}",
				domain,
				row.dimensions.len(),
				expected_dimensions
			);
			let row_state = state.rows.entry(row.dimensions).or_default();
			for measure in row.measures {
				row_state.apply(measure, surface);
			}
		}
	}

	pub fn roll(&mut self, now: DateTime) -> Result<Vec<PublishedSurface>> {
		let mut published = Vec::new();
		for state in self.domains.values_mut() {
			published.push(PublishedSurface {
				domain: state.spec.domain,
				surface: Surface::Current,
				columns: build_surface(state, now, Surface::Current)?,
			});
			if state.spec.has_total {
				published.push(PublishedSurface {
					domain: state.spec.domain,
					surface: Surface::Total,
					columns: build_surface(state, now, Surface::Total)?,
				});
			}
			advance_window(state);
		}
		Ok(published)
	}
}

impl RowState {
	fn apply(&mut self, measure: Measure, surface: Surface) {
		reifydb_assertions! {
			assert!(
				!matches!(measure.kind, MetricKind::Delta | MetricKind::Dimension),
				"producers push Level, Counter, Cumulative or Distribution; got {:?} for metric {}",
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
			MetricKind::Cumulative => {
				self.measures.insert(
					measure.metric,
					MeasureState::Cumulative {
						latest: measure.reading,
					},
				);
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
				MeasureState::Cumulative {
					..
				} => {}
			}
		}
	}
}

fn build_surface(state: &DomainState, now: DateTime, surface: Surface) -> Result<Columns> {
	match state.spec.shape {
		DomainShape::Long => Ok(build_long(state, now, surface)),
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
					MeasureState::Cumulative {
						latest,
					},
					Surface::Current,
				) => Some((*latest, MetricKind::Cumulative)),
				(
					MeasureState::Cumulative {
						..
					},
					Surface::Total,
				) => None,
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

fn build_wide(state: &DomainState, now: DateTime, surface: Surface) -> Result<Columns> {
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
		for ((dimension, buffer), value) in
			spec.dimensions.iter().zip(dimension_buffers.iter_mut()).zip(dimensions)
		{
			buffer.push_typed(value.clone(), &dimension.buffer_type()).map_err(|cause| {
				column_error(spec.domain, surface, "dimension", dimension.name, cause)
			})?;
		}
		for (buffer, measure) in measure_buffers.iter_mut().zip(&measures) {
			let value = wide_value(row.measures.get(measure.name), measure, surface);
			buffer.push_typed(value, &measure.buffer_type())
				.map_err(|cause| column_error(spec.domain, surface, "measure", measure.name, cause))?;
		}
	}

	let mut out = vec![ColumnWithName::new(Fragment::internal("ts"), ts)];
	for (dimension, buffer) in spec.dimensions.iter().zip(dimension_buffers) {
		out.push(ColumnWithName::new(Fragment::internal(dimension.name), buffer));
	}
	for (measure, buffer) in measures.iter().zip(measure_buffers) {
		out.push(ColumnWithName::new(Fragment::internal(measure.name), buffer));
	}
	Ok(Columns::new(out))
}

fn column_error(
	domain: MetricsDomain,
	surface: Surface,
	role: &'static str,
	name: &'static str,
	cause: Error,
) -> Error {
	internal_error!("metrics {:?} {:?} {} column {}: {}", domain, surface, role, name, cause)
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
			Some(MeasureState::Cumulative {
				latest,
			}),
			Surface::Current,
		) => Some(*latest),
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
		DateTime::from_epoch_millis(millis).unwrap()
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
		let published = acc.roll(now(1_000)).unwrap();

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
		acc.roll(now(1_000)).unwrap();
		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![counter_row("n1", "evictions", 30)]);
		let published = acc.roll(now(2_000)).unwrap();

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
		acc.roll(now(1_000)).unwrap();
		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![counter_row("n1", "evictions", 5)]);
		let published = acc.roll(now(2_000)).unwrap();
		let current = surface(&published, MetricsDomain::RuntimeOperators, Surface::Current);
		assert_eq!(long_value(current, "n1", "evictions"), Some((0.0, "delta".to_string())));

		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![counter_row("n1", "evictions", 8)]);
		let published = acc.roll(now(3_000)).unwrap();
		let current = surface(&published, MetricsDomain::RuntimeOperators, Surface::Current);
		assert_eq!(long_value(current, "n1", "evictions"), Some((3.0, "delta".to_string())));
	}

	#[test]
	fn level_passes_through_and_never_reaches_total() {
		// A level is the answer as-is; folding it into ::total would sum a gauge.
		let mut acc = operators_accumulator();
		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![level_row("n1", "state_bytes", 42)]);
		let published = acc.roll(now(1_000)).unwrap();

		let current = surface(&published, MetricsDomain::RuntimeOperators, Surface::Current);
		assert_eq!(long_value(current, "n1", "state_bytes"), Some((42.0, "level".to_string())));
		let total = surface(&published, MetricsDomain::RuntimeOperators, Surface::Total);
		assert_eq!(long_value(total, "n1", "state_bytes"), None);

		// Without a fresh push the level persists: the last reading stays the answer.
		let published = acc.roll(now(2_000)).unwrap();
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
		let published = acc.roll(now(1_000)).unwrap();

		let current = surface(&published, MetricsDomain::Instruments, Surface::Current);
		assert_eq!(long_value(current, "h", "p50"), Some((5.0, "distribution".to_string())));
		let total = surface(&published, MetricsDomain::Instruments, Surface::Total);
		assert_eq!(long_value(total, "h", "p50"), Some((7.0, "distribution".to_string())));

		let published = acc.roll(now(2_000)).unwrap();
		let current = surface(&published, MetricsDomain::Instruments, Surface::Current);
		assert_eq!(long_value(current, "h", "p50"), None, "window distribution must clear on roll");
		let total = surface(&published, MetricsDomain::Instruments, Surface::Total);
		assert_eq!(long_value(total, "h", "p50"), Some((7.0, "distribution".to_string())));
	}

	#[test]
	fn an_optional_dimension_publishes_one_type_whatever_order_its_rows_arrive_in() {
		// The lifecycle binding dimension is present for some classes and none for others; inference demoted
		// the buffer to a bare Utf8 on a present-first roll, so the published column type swung with row order.
		fn lifecycle_row(class: &str, binding: Option<&str>) -> MetricsRow {
			MetricsRow {
				dimensions: vec![
					Value::Utf8(class.to_string()),
					match binding {
						Some(term) => Value::Utf8(term.to_string()),
						None => Value::none_of(ValueType::Utf8),
					},
				],
				measures: vec![Measure {
					metric: "backlog_hint",
					reading: Reading::Count(Count::new(1)),
					kind: MetricKind::Level,
				}],
			}
		}

		let declared = ValueType::Option(Box::new(ValueType::Utf8));
		for rows in [
			vec![lifecycle_row("a", Some("x")), lifecycle_row("b", Some("y"))],
			vec![lifecycle_row("a", Some("x")), lifecycle_row("b", None)],
			vec![lifecycle_row("a", None), lifecycle_row("b", Some("x"))],
		] {
			let mut acc = MetricsAccumulator::new(MetricsDomain::ALL.map(MetricsDomain::spec));
			acc.push(MetricsDomain::Lifecycle, Surface::Current, rows);
			let published = acc.roll(now(1_000)).unwrap();
			let columns = surface(&published, MetricsDomain::Lifecycle, Surface::Current);
			let binding = columns.iter().find(|c| c.name().text() == "binding").unwrap();
			assert_eq!(
				binding.data().get_type(),
				declared,
				"binding must publish Option(Utf8) regardless of which row lands first"
			);
			assert_eq!(binding.data().len(), 2);
		}
	}

	#[test]
	fn wide_pivot_keeps_dimensions_aligned_with_measures() {
		// Two shards with distinct values; a misaligned pivot would attribute shard 1's
		// bytes to shard 0.
		let mut acc = MetricsAccumulator::new([MetricsDomain::StoreMultiRead.spec()]);
		let shard_row = |shard: u16, used: u64, installs: u64| MetricsRow {
			dimensions: vec![Value::Uint2(shard)],
			measures: vec![
				Measure {
					metric: "used",
					reading: Reading::Bytes(ByteSize::from_bytes(used)),
					kind: MetricKind::Level,
				},
				Measure {
					metric: "installs",
					reading: Reading::Count(Count::new(installs)),
					kind: MetricKind::Counter,
				},
			],
		};
		acc.push(
			MetricsDomain::StoreMultiRead,
			Surface::Current,
			vec![shard_row(0, 100, 3), shard_row(1, 200, 9)],
		);
		let published = acc.roll(now(1_000)).unwrap();

		let current = surface(&published, MetricsDomain::StoreMultiRead, Surface::Current);
		assert_eq!(column_values(current, "shard"), vec![Value::Uint2(0), Value::Uint2(1)]);
		assert_eq!(column_values(current, "used"), vec![Value::Uint8(100), Value::Uint8(200)]);
		assert_eq!(column_values(current, "installs"), vec![Value::Uint8(3), Value::Uint8(9)]);

		let total = surface(&published, MetricsDomain::StoreMultiRead, Surface::Total);
		assert_eq!(column_values(total, "installs"), vec![Value::Uint8(3), Value::Uint8(9)]);
		assert!(
			!total.iter().any(|c| c.name().text() == "used"),
			"levels must not appear in a wide ::total surface"
		);
	}

	#[test]
	fn a_census_drops_rows_that_stop_being_reported() {
		// A dropped object vanishes from the census; keeping its row reports deleted bytes as live.
		let mut acc = MetricsAccumulator::new([MetricsDomain::Storage.spec()]);
		let storage_row = |id: u64, bytes: u64| MetricsRow {
			dimensions: vec![
				Value::Utf8("table".to_string()),
				Value::Uint8(id),
				Value::Uint8(1),
				Value::Utf8("buffer".to_string()),
			],
			measures: vec![Measure {
				metric: "live_bytes",
				reading: Reading::Bytes(ByteSize::from_bytes(bytes)),
				kind: MetricKind::Level,
			}],
		};

		acc.push(MetricsDomain::Storage, Surface::Current, vec![storage_row(1, 100), storage_row(2, 200)]);
		let published = acc.roll(now(1_000)).unwrap();
		let current = surface(&published, MetricsDomain::Storage, Surface::Current);
		assert_eq!(column_values(current, "id"), vec![Value::Uint8(1), Value::Uint8(2)]);
		assert_eq!(column_values(current, "live_bytes"), vec![Value::Uint8(100), Value::Uint8(200)]);

		acc.push(MetricsDomain::Storage, Surface::Current, vec![storage_row(2, 200)]);
		let published = acc.roll(now(2_000)).unwrap();
		let current = surface(&published, MetricsDomain::Storage, Surface::Current);
		assert_eq!(
			column_values(current, "id"),
			vec![Value::Uint8(2)],
			"a gauge row that stops being reported must not survive the next push"
		);
	}

	#[test]
	fn a_surviving_census_row_keeps_measures_the_new_push_omits() {
		// Eviction is per vanished row, never a wipe: state a census omits for a live row must survive.
		let mut acc = MetricsAccumulator::new([MetricsDomain::Storage.spec()]);
		let dimensions = vec![
			Value::Utf8("table".to_string()),
			Value::Uint8(1),
			Value::Uint8(1),
			Value::Utf8("buffer".to_string()),
		];
		let measure = |metric: &'static str, bytes: u64| Measure {
			metric,
			reading: Reading::Bytes(ByteSize::from_bytes(bytes)),
			kind: MetricKind::Level,
		};

		acc.push(
			MetricsDomain::Storage,
			Surface::Current,
			vec![MetricsRow {
				dimensions: dimensions.clone(),
				measures: vec![measure("live_bytes", 100), measure("total_bytes", 300)],
			}],
		);
		acc.roll(now(1_000)).unwrap();

		acc.push(
			MetricsDomain::Storage,
			Surface::Current,
			vec![MetricsRow {
				dimensions,
				measures: vec![measure("live_bytes", 150)],
			}],
		);
		let published = acc.roll(now(2_000)).unwrap();
		let current = surface(&published, MetricsDomain::Storage, Surface::Current);
		assert_eq!(column_values(current, "live_bytes"), vec![Value::Uint8(150)]);
		assert_eq!(
			column_values(current, "total_bytes"),
			vec![Value::Uint8(300)],
			"a wipe would republish this as zero; only per-row eviction keeps it"
		);
	}

	#[test]
	fn an_update_domain_keeps_rows_that_stop_being_reported() {
		// A quiet producer is not a vanished one, so its last level must stand.
		let mut acc = operators_accumulator();
		acc.push(
			MetricsDomain::RuntimeOperators,
			Surface::Current,
			vec![level_row("n1", "state_bytes", 42), level_row("n2", "state_bytes", 7)],
		);
		acc.roll(now(1_000)).unwrap();

		acc.push(MetricsDomain::RuntimeOperators, Surface::Current, vec![level_row("n1", "state_bytes", 42)]);
		let published = acc.roll(now(2_000)).unwrap();
		let current = surface(&published, MetricsDomain::RuntimeOperators, Surface::Current);
		assert_eq!(
			long_value(current, "n2", "state_bytes"),
			Some((7.0, "level".to_string())),
			"a folding domain must hold a level whose producer went quiet"
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
		let published = acc.roll(stamp).unwrap();
		for p in &published {
			for value in column_values(&p.columns, "ts") {
				assert_eq!(value, Value::DateTime(stamp));
			}
		}
	}
}
