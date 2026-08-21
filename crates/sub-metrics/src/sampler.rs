// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
	key::operator_state::Keyspace,
	lifecycle::metrics::RetentionMetrics,
	metrics::sample::{MetricKind, MetricsSample, Reading},
	value::column::columns::Columns,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		reply::Reply,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_store_multi::{MultiStore, tier::read::ReadBufferShardMetrics};
use reifydb_store_operator::{
	store::OperatorStore,
	tier::read::{OperatorReadBufferKeyspaceMetrics, OperatorReadBufferShardMetrics},
};
use reifydb_store_single::SingleStore;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	params::Params,
	value::{Value, datetime::DateTime, duration::Duration, identity::IdentityId, value_type::ValueType},
};
use tracing::error;

use crate::{
	domains::{
		epoch::EpochGauge,
		proc::{
			cgroup_cpu_rows, cgroup_io_rows, cgroup_memory_rows, cgroup_pressure_rows, process_io_rows,
			process_memory_rows, process_sched_rows,
		},
		runtime::collect::{Collectors, collect_memory, collect_operators, collect_watermarks},
	},
	framework::{
		accumulator::{Measure, MetricsAccumulator, MetricsRow, PublishedSurface},
		spec::{MetricsDomain, Surface},
		surfaces::MetricsSurfaces,
	},
};

pub enum SamplerMessage {
	Tick {
		ack: Option<Reply<()>>,
	},
	Push {
		domain: MetricsDomain,
		surface: Surface,
		rows: Vec<MetricsRow>,
	},
}

pub struct MetricsSamplerActor {
	collectors: Collectors,
	multi_store: MultiStore,
	single_store: SingleStore,
	operator_store: OperatorStore,
	retention_metrics: RetentionMetrics,
	epoch_gauge: Arc<EpochGauge>,
	surfaces: Arc<MetricsSurfaces>,
	clock: Clock,
	interval: Duration,
	snapshot_interval: Option<Duration>,
}

pub struct SamplerState {
	accumulator: MetricsAccumulator,
	last_snapshot: Option<DateTime>,
}

impl MetricsSamplerActor {
	#[allow(clippy::too_many_arguments)]
	pub fn new(
		collectors: Collectors,
		multi_store: MultiStore,
		single_store: SingleStore,
		operator_store: OperatorStore,
		retention_metrics: RetentionMetrics,
		epoch_gauge: Arc<EpochGauge>,
		surfaces: Arc<MetricsSurfaces>,
		clock: Clock,
		interval: Duration,
		snapshot_interval: Option<Duration>,
	) -> Self {
		Self {
			collectors,
			multi_store,
			single_store,
			operator_store,
			retention_metrics,
			epoch_gauge,
			surfaces,
			clock,
			interval,
			snapshot_interval,
		}
	}

	fn snapshot_due(&self, state: &SamplerState, now: DateTime) -> bool {
		let Some(interval) = self.snapshot_interval else {
			return false;
		};
		match state.last_snapshot {
			None => true,
			Some(last) => {
				now.to_nanos().saturating_sub(last.to_nanos()) >= interval.to_std().as_nanos() as u64
			}
		}
	}

	fn append_snapshot(&self, published: &PublishedSurface) {
		let Some(path) = published.domain.snapshots_path() else {
			return;
		};
		let rows = snapshot_rows(&published.columns);
		if rows.is_empty() {
			return;
		}
		let mut builder = self.collectors.engine.bulk_insert_unchecked(IdentityId::system());
		builder.series(path).rows(rows).done();
		if let Err(e) = builder.execute() {
			error!("Failed to append {} snapshot: {}", path, e);
		}
	}

	fn sample_and_publish(&self, state: &mut SamplerState) -> Result<()> {
		let accumulator = &mut state.accumulator;
		accumulator.push(
			MetricsDomain::RuntimeMemory,
			Surface::Current,
			long_rows(collect_memory(&self.collectors)),
		);
		accumulator.push(
			MetricsDomain::RuntimeWatermarks,
			Surface::Current,
			long_rows(collect_watermarks(&self.collectors)),
		);
		accumulator.push(
			MetricsDomain::RuntimeOperators,
			Surface::Current,
			long_rows(collect_operators(&self.collectors)),
		);
		accumulator.push(MetricsDomain::ProcProcessIo, Surface::Current, process_io_rows());
		accumulator.push(MetricsDomain::ProcProcessMemory, Surface::Current, process_memory_rows());
		accumulator.push(MetricsDomain::ProcProcessSched, Surface::Current, process_sched_rows());
		accumulator.push(MetricsDomain::ProcCgroupIo, Surface::Current, cgroup_io_rows());
		accumulator.push(MetricsDomain::ProcCgroupMemory, Surface::Current, cgroup_memory_rows());
		accumulator.push(MetricsDomain::ProcCgroupCpu, Surface::Current, cgroup_cpu_rows());
		accumulator.push(MetricsDomain::ProcCgroupPressure, Surface::Current, cgroup_pressure_rows());
		accumulator.push(
			MetricsDomain::Instruments,
			Surface::Current,
			long_rows(self.collectors.registry.read_reporters_windowed()),
		);
		accumulator.push(
			MetricsDomain::Instruments,
			Surface::Total,
			long_rows(self.collectors.registry.read_reporters()),
		);
		accumulator.push(
			MetricsDomain::StoreMultiCommit,
			Surface::Current,
			multi_commit_rows(&self.multi_store),
		);
		accumulator.push(MetricsDomain::StoreMultiRead, Surface::Current, multi_read_rows(&self.multi_store));
		accumulator.push(
			MetricsDomain::StoreMultiPersistent,
			Surface::Current,
			multi_persistent_rows(&self.multi_store),
		);
		accumulator.push(
			MetricsDomain::StoreSingleCommit,
			Surface::Current,
			single_commit_rows(&self.single_store),
		);
		accumulator.push(
			MetricsDomain::StoreSinglePersistent,
			Surface::Current,
			single_persistent_rows(&self.single_store),
		);
		accumulator.push(
			MetricsDomain::StoreOperatorRead,
			Surface::Current,
			operator_read_rows(&self.operator_store),
		);
		accumulator.push(
			MetricsDomain::StoreOperatorReadKeyspace,
			Surface::Current,
			operator_read_keyspace_rows(&self.operator_store),
		);
		accumulator.push(
			MetricsDomain::StoreOperatorPersistent,
			Surface::Current,
			operator_persistent_rows(&self.operator_store),
		);
		accumulator.push(
			MetricsDomain::Epoch,
			Surface::Current,
			epoch_rows(&self.collectors.engine, &self.epoch_gauge),
		);
		accumulator.push(MetricsDomain::Lifecycle, Surface::Current, lifecycle_rows(&self.retention_metrics));

		let now = self.clock.now();
		let snapshot_due = self.snapshot_due(state, now);
		let rolled = state.accumulator.roll(now)?;
		for published in rolled {
			if snapshot_due && published.surface == Surface::Current {
				self.append_snapshot(&published);
			}
			self.surfaces.store(published);
		}
		if snapshot_due {
			state.last_snapshot = Some(now);
		}
		Ok(())
	}
}

fn snapshot_rows(columns: &Columns) -> Vec<Params> {
	let row_count = columns.get(0).map(|column| column.data().len()).unwrap_or(0);
	(0..row_count)
		.map(|index| {
			let mut row = HashMap::new();
			for column in columns.iter() {
				let value = column.data().get_value(index);
				if !matches!(value, Value::None { .. }) {
					row.insert(column.name().text().to_string(), value);
				}
			}
			Params::Named(Arc::new(row))
		})
		.collect()
}

impl Actor for MetricsSamplerActor {
	type Message = SamplerMessage;
	type State = SamplerState;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(self.interval, || SamplerMessage::Tick {
			ack: None,
		});
		SamplerState {
			accumulator: MetricsAccumulator::new(MetricsDomain::ALL.map(MetricsDomain::spec)),
			last_snapshot: None,
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			SamplerMessage::Tick {
				ack,
			} => {
				self.sample_and_publish(state).expect(
					"metrics sample must publish; a rejected column means a domain writes a type its spec does not declare",
				);
				ctx.schedule_once(self.interval, || SamplerMessage::Tick {
					ack: None,
				});
				if let Some(ack) = ack {
					ack.send(());
				}
			}
			SamplerMessage::Push {
				domain,
				surface,
				rows,
			} => state.accumulator.push(domain, surface, rows),
		}
		Directive::Continue
	}

	fn post_stop(&self) {}
}

fn long_rows(samples: Vec<MetricsSample>) -> Vec<MetricsRow> {
	samples.into_iter()
		.map(|sample| MetricsRow {
			dimensions: vec![Value::Utf8(sample.scope.into_owned())],
			measures: vec![Measure {
				metric: sample.metric,
				reading: sample.reading,
				kind: sample.kind,
			}],
		})
		.collect()
}

fn level_count(metric: &'static str, value: u64) -> Measure {
	Measure {
		metric,
		reading: Reading::Count(Count::new(value)),
		kind: MetricKind::Level,
	}
}

fn counter_count(metric: &'static str, value: u64) -> Measure {
	Measure {
		metric,
		reading: Reading::Count(Count::new(value)),
		kind: MetricKind::Counter,
	}
}

fn level_bytes(metric: &'static str, bytes: ByteSize) -> Measure {
	Measure {
		metric,
		reading: Reading::Bytes(bytes),
		kind: MetricKind::Level,
	}
}

fn multi_read_rows(store: &MultiStore) -> Vec<MetricsRow> {
	store.read_buffer_shard_metrics().iter().map(multi_read_row).collect()
}

fn multi_read_row(metrics: &ReadBufferShardMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Uint2(metrics.shard as u16)],
		measures: vec![
			level_bytes("used", metrics.state.used),
			level_bytes("limit", metrics.state.limit),
			level_count("pages", metrics.state.pages as u64),
			level_count("page_cap", metrics.state.page_cap as u64),
			level_bytes("payload", metrics.state.payload),
			level_count("entries", metrics.state.entries as u64),
			level_count("hot_pages", metrics.state.hot_pages as u64),
			level_count("complete_pages", metrics.state.complete_pages as u64),
			level_count("blocked_pages", metrics.state.blocked_pages as u64),
			level_count("warming", metrics.state.warming as u64),
			counter_count("warms_started", metrics.warms.warms_started),
			counter_count("warms_completed", metrics.warms.warms_completed),
			counter_count("warms_dirty_aborted", metrics.warms.warms_dirty_aborted),
			counter_count("warms_aborted", metrics.warms.warms_aborted),
			counter_count("pages_warm_blocked", metrics.warms.pages_warm_blocked),
			counter_count("pages_evicted", metrics.warms.pages_evicted),
			counter_count("complete_pages_invalidated", metrics.warms.complete_pages_invalidated),
			counter_count("point_hits", metrics.reads.point_hits),
			counter_count("previous_hits", metrics.reads.previous_hits),
			counter_count("point_misses", metrics.reads.point_misses),
			counter_count("range_served", metrics.reads.range_served),
			counter_count("range_gaps", metrics.reads.range_gaps),
		],
	}
}

fn multi_commit_rows(store: &MultiStore) -> Vec<MetricsRow> {
	let metrics = store.commit_metrics();
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_bytes("current_bytes", metrics.current_bytes),
			level_bytes("historical_bytes", metrics.historical_bytes),
			level_count("table_count", metrics.table_count.as_u64()),
			level_count("current_entries", metrics.current_entries.as_u64()),
		],
	}]
}

fn single_commit_rows(store: &SingleStore) -> Vec<MetricsRow> {
	let Some(metrics) = store.commit_metrics() else {
		return Vec::new();
	};
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_count("resident_entries", metrics.resident_entries.as_u64()),
			level_bytes("resident_bytes", metrics.resident_bytes),
		],
	}]
}

fn operator_read_rows(store: &OperatorStore) -> Vec<MetricsRow> {
	store.read_buffer_shard_metrics().iter().map(operator_read_row).collect()
}

fn operator_read_row(metrics: &OperatorReadBufferShardMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Uint2(metrics.shard as u16)],
		measures: vec![
			level_bytes("used", metrics.used),
			level_bytes("limit", metrics.limit),
			level_count("buckets", metrics.buckets as u64),
			level_count("entries", metrics.entries as u64),
			level_count("complete_buckets", metrics.complete_buckets as u64),
			counter_count("hits", metrics.counters.hits),
			counter_count("misses", metrics.counters.misses),
			counter_count("evictions", metrics.counters.evictions),
			counter_count("fills_started", metrics.counters.fills_started),
			counter_count("fills_dirty_aborted", metrics.counters.fills_dirty_aborted),
			counter_count("fills_duplicate", metrics.counters.fills_duplicate),
		],
	}
}

fn operator_read_keyspace_rows(store: &OperatorStore) -> Vec<MetricsRow> {
	store.read_buffer_keyspace_metrics().iter().map(operator_read_keyspace_row).collect()
}

fn keyspace_label(keyspace: Keyspace) -> String {
	let name = keyspace.name();
	if name == "CUSTOM" && keyspace != Keyspace::CUSTOM {
		format!("CUSTOM_0x{:02X}", keyspace.0)
	} else {
		name.to_string()
	}
}

fn operator_read_keyspace_row(metrics: &OperatorReadBufferKeyspaceMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Utf8(keyspace_label(metrics.keyspace))],
		measures: vec![
			level_bytes("used", metrics.used),
			level_count("buckets", metrics.buckets as u64),
			level_count("entries", metrics.entries as u64),
			level_count("complete_buckets", metrics.complete_buckets as u64),
			counter_count("hits", metrics.counters.hits),
			counter_count("misses", metrics.counters.misses),
			counter_count("evictions", metrics.counters.evictions),
			counter_count("fills_started", metrics.counters.fills_started),
			counter_count("fills_dirty_aborted", metrics.counters.fills_dirty_aborted),
			counter_count("fills_duplicate", metrics.counters.fills_duplicate),
		],
	}
}

fn multi_persistent_rows(store: &MultiStore) -> Vec<MetricsRow> {
	let Some(metrics) = store.persistent_page_cache_metrics() else {
		return Vec::new();
	};
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_bytes("used", metrics.used),
			level_count("connections_sampled", metrics.connections_sampled.as_u64()),
			level_count("connections_total", metrics.connections_total.as_u64()),
			counter_count("hits", metrics.hits.as_u64()),
			counter_count("misses", metrics.misses.as_u64()),
		],
	}]
}

fn single_persistent_rows(store: &SingleStore) -> Vec<MetricsRow> {
	let Some(metrics) = store.persistent_page_cache_metrics() else {
		return Vec::new();
	};
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_bytes("used", metrics.used),
			level_count("connections_sampled", metrics.connections_sampled.as_u64()),
			level_count("connections_total", metrics.connections_total.as_u64()),
			counter_count("hits", metrics.hits.as_u64()),
			counter_count("misses", metrics.misses.as_u64()),
		],
	}]
}

fn operator_persistent_rows(store: &OperatorStore) -> Vec<MetricsRow> {
	let Some(metrics) = store.persistent_page_cache_metrics() else {
		return Vec::new();
	};
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_bytes("used", metrics.used),
			level_count("connections_sampled", metrics.connections_sampled.as_u64()),
			level_count("connections_total", metrics.connections_total.as_u64()),
			counter_count("hits", metrics.hits.as_u64()),
			counter_count("misses", metrics.misses.as_u64()),
		],
	}]
}

fn epoch_rows(engine: &StandardEngine, gauge: &EpochGauge) -> Vec<MetricsRow> {
	let epoch = engine.version_epoch();
	let stats = epoch.stats();
	let guaranteed = epoch.retention().guaranteed_coverage();
	let (durable_samples, pruned) = gauge.read();
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_count("samples", stats.samples as u64),
			level_count("durable_samples", durable_samples),
			Measure {
				metric: "coverage",
				reading: Reading::Duration(stats.coverage.to_duration()),
				kind: MetricKind::Level,
			},
			Measure {
				metric: "guaranteed_coverage",
				reading: Reading::Duration(guaranteed.to_duration()),
				kind: MetricKind::Level,
			},
			counter_count("pruned", pruned),
			counter_count("floor_none_returns", stats.floor_none_returns),
		],
	}]
}

fn lifecycle_rows(metrics: &RetentionMetrics) -> Vec<MetricsRow> {
	metrics.report()
		.into_iter()
		.map(|(class, snapshot)| {
			let binding = match snapshot.binding {
				Some(term) => Value::Utf8(term.to_string()),
				None => Value::none_of(ValueType::Utf8),
			};
			let measures = vec![
				Measure {
					metric: "floor_version",
					reading: Reading::Version(snapshot.floor_version),
					kind: MetricKind::Level,
				},
				level_count("backlog_hint", snapshot.backlog_hint),
				counter_count("work_done", snapshot.work_done),
				counter_count("slices", snapshot.slices),
				counter_count("stuck_slices", snapshot.stuck_slices),
				counter_count("budget_exhausted_slices", snapshot.budget_exhausted_slices),
				counter_count("gated_slices", snapshot.gated_slices),
			];
			MetricsRow {
				dimensions: vec![Value::Utf8(class.name().to_string()), binding],
				measures,
			}
		})
		.collect()
}

#[cfg(test)]
mod tests {
	use reifydb_store_operator::tier::read::OperatorReadBufferMetrics;

	use super::*;

	fn sample() -> OperatorReadBufferKeyspaceMetrics {
		OperatorReadBufferKeyspaceMetrics {
			keyspace: Keyspace::SOURCE_WATERMARK,
			used: ByteSize::from_bytes(12_401),
			buckets: 115,
			entries: 231,
			complete_buckets: 97,
			counters: OperatorReadBufferMetrics {
				hits: 367_918,
				misses: 2_944,
				evictions: 51,
				fills_started: 3_001,
				fills_dirty_aborted: 7,
				fills_duplicate: 13,
			},
		}
	}

	#[test]
	fn keyspace_row_names_the_keyspace_rather_than_numbering_it() {
		let row = operator_read_keyspace_row(&sample());
		assert_eq!(
			row.dimensions,
			vec![Value::Utf8("SOURCE_WATERMARK".to_string())],
			"the dimension must be the keyspace name; a raw u8 breaks the moment a constant is renumbered"
		);
	}

	#[test]
	fn two_unnamed_keyspaces_never_collapse_into_one_row() {
		// name() answers "CUSTOM" for every value it has no constant for, so labelling rows with it alone
		// merges distinct keyspaces into a single accumulator key and silently sums their counters.
		let mut first = sample();
		first.keyspace = Keyspace(0x50);
		let mut second = sample();
		second.keyspace = Keyspace(0x51);

		let first = operator_read_keyspace_row(&first);
		let second = operator_read_keyspace_row(&second);

		assert_eq!(first.dimensions, vec![Value::Utf8("CUSTOM_0x50".to_string())]);
		assert_eq!(second.dimensions, vec![Value::Utf8("CUSTOM_0x51".to_string())]);
		assert_ne!(first.dimensions, second.dimensions);
	}

	#[test]
	fn the_real_custom_keyspace_keeps_its_plain_name() {
		// Keyspace::CUSTOM is a declared constant, not a gap: relabelling it CUSTOM_0x40 would rename a
		// keyspace that every other surface still calls CUSTOM.
		let mut metrics = sample();
		metrics.keyspace = Keyspace::CUSTOM;
		let row = operator_read_keyspace_row(&metrics);
		assert_eq!(row.dimensions, vec![Value::Utf8("CUSTOM".to_string())]);
	}

	#[test]
	fn keyspace_row_carries_every_declared_measure_exactly_once() {
		let row = operator_read_keyspace_row(&sample());
		let declared: Vec<&str> =
			MetricsDomain::StoreOperatorReadKeyspace.spec().measures.iter().map(|m| m.name).collect();
		let built: Vec<&str> = row.measures.iter().map(|m| m.metric).collect();
		assert_eq!(built, declared, "a declared measure the row omits publishes as none forever");
	}

	#[test]
	fn keyspace_row_maps_each_field_to_its_own_measure() {
		let metrics = sample();
		let row = operator_read_keyspace_row(&metrics);
		let find = |name: &str| {
			row.measures.iter().find(|m| m.metric == name).unwrap_or_else(|| panic!("missing {name}"))
		};

		assert_eq!(find("used").reading, Reading::Bytes(ByteSize::from_bytes(12_401)));
		assert_eq!(find("buckets").reading, Reading::Count(Count::new(115)));
		assert_eq!(find("entries").reading, Reading::Count(Count::new(231)));
		assert_eq!(find("complete_buckets").reading, Reading::Count(Count::new(97)));
		assert_eq!(find("hits").reading, Reading::Count(Count::new(367_918)));
		assert_eq!(find("misses").reading, Reading::Count(Count::new(2_944)));
		assert_eq!(find("evictions").reading, Reading::Count(Count::new(51)));
		assert_eq!(find("fills_started").reading, Reading::Count(Count::new(3_001)));
		assert_eq!(find("fills_dirty_aborted").reading, Reading::Count(Count::new(7)));
		assert_eq!(find("fills_duplicate").reading, Reading::Count(Count::new(13)));
	}

	#[test]
	fn keyspace_row_kinds_match_the_declared_spec() {
		let row = operator_read_keyspace_row(&sample());
		let spec = MetricsDomain::StoreOperatorReadKeyspace.spec();
		for measure in &row.measures {
			let declared = spec.measures.iter().find(|m| m.name == measure.metric).expect("declared");
			assert_eq!(
				measure.kind, declared.kind,
				"{} must be pushed with the kind the spec declares, otherwise a level accumulates as a counter",
				measure.metric
			);
		}
	}

	#[test]
	fn keyspace_rows_report_nothing_for_a_store_without_a_read_tier() {
		assert!(
			operator_read_keyspace_rows(&OperatorStore::testing_memory()).is_empty(),
			"an absent read tier must publish no rows, never one row of zeros claiming a perfect cache"
		);
	}
}
