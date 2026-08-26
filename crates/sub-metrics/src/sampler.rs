// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb_core::{
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
use reifydb_store_cdc::{
	store::CdcStore,
	tier::{commit::CdcCommitMetrics, persistent::CdcPersistentMetrics, read::CdcReadShardMetrics},
};
use reifydb_store_multi::{
	MultiStore,
	tier::{point::MultiPointShardMetrics, range::MultiRangeShardMetrics},
};
use reifydb_store_operator::{
	store::OperatorStore,
	tier::{
		point::{OperatorPointKeyspaceMetrics, OperatorPointShardMetrics},
		range::{OperatorRangeKeyspaceMetrics, OperatorRangeShardMetrics},
	},
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
	cdc_store: CdcStore,
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
		cdc_store: CdcStore,
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
			cdc_store,
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
		accumulator.push(MetricsDomain::StoreMultiRange, Surface::Current, multi_range_rows(&self.multi_store));
		accumulator.push(MetricsDomain::StoreMultiPoint, Surface::Current, multi_point_rows(&self.multi_store));
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
			MetricsDomain::StoreOperatorPoint,
			Surface::Current,
			operator_point_rows(&self.operator_store),
		);
		accumulator.push(
			MetricsDomain::StoreOperatorPointKeyspace,
			Surface::Current,
			operator_point_keyspace_rows(&self.operator_store),
		);
		accumulator.push(
			MetricsDomain::StoreOperatorRange,
			Surface::Current,
			operator_range_rows(&self.operator_store),
		);
		accumulator.push(
			MetricsDomain::StoreOperatorRangeKeyspace,
			Surface::Current,
			operator_range_keyspace_rows(&self.operator_store),
		);
		accumulator.push(
			MetricsDomain::StoreOperatorPersistent,
			Surface::Current,
			operator_persistent_rows(&self.operator_store),
		);
		accumulator.push(MetricsDomain::StoreCdcCommit, Surface::Current, cdc_commit_rows(&self.cdc_store));
		accumulator.push(MetricsDomain::StoreCdcRead, Surface::Current, cdc_read_rows(&self.cdc_store));
		accumulator.push(
			MetricsDomain::StoreCdcPersistent,
			Surface::Current,
			cdc_persistent_rows(&self.cdc_store),
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

fn level_ratio(metric: &'static str, ratio: f64) -> Measure {
	Measure {
		metric,
		reading: Reading::Ratio(ratio),
		kind: MetricKind::Level,
	}
}

fn level_bytes(metric: &'static str, bytes: ByteSize) -> Measure {
	Measure {
		metric,
		reading: Reading::Bytes(bytes),
		kind: MetricKind::Level,
	}
}

fn multi_range_rows(store: &MultiStore) -> Vec<MetricsRow> {
	store.range_shard_metrics().iter().map(multi_range_row).collect()
}

fn multi_range_row(metrics: &MultiRangeShardMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Uint2(metrics.shard as u16)],
		measures: vec![
			level_bytes("used", metrics.used),
			level_bytes("limit", metrics.limit),
			level_count("partitions", metrics.partitions as u64),
			level_count("entries", metrics.entries as u64),
			level_count("complete_partitions", metrics.complete_partitions as u64),
			counter_count("hits", metrics.counters.hits),
			counter_count("misses", metrics.counters.misses),
			counter_count("materializes", metrics.counters.materializes),
			counter_count("materializes_refused", metrics.counters.materializes_refused),
			counter_count("materializes_raced", metrics.counters.materializes_raced),
			counter_count("evictions", metrics.counters.evictions),
			counter_count("point_hits", metrics.counters.point_hits),
			counter_count("point_misses", metrics.counters.point_misses),
			counter_count("served", metrics.serve.served),
			counter_count("rows", metrics.serve.rows),
			counter_count("head_advances", metrics.serve.head_advances),
		],
	}
}

fn multi_point_rows(store: &MultiStore) -> Vec<MetricsRow> {
	store.point_shard_metrics().iter().map(multi_point_row).collect()
}

fn multi_point_row(metrics: &MultiPointShardMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Uint2(metrics.shard as u16)],
		measures: vec![
			level_bytes("used", metrics.used),
			level_bytes("limit", metrics.limit),
			level_count("entries", metrics.entries as u64),
			counter_count("hits", metrics.reads.hits),
			counter_count("previous_hits", metrics.reads.previous_hits),
			counter_count("misses", metrics.reads.misses),
			counter_count("insertions", metrics.counters.insertions),
			counter_count("evictions", metrics.counters.evictions),
			counter_count("fills_started", metrics.counters.fills_started),
			counter_count("fills_dirty_aborted", metrics.counters.fills_dirty_aborted),
			counter_count("fills_duplicate", metrics.counters.fills_duplicate),
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

fn operator_point_rows(store: &OperatorStore) -> Vec<MetricsRow> {
	store.point_shard_metrics().iter().map(operator_point_row).collect()
}

fn operator_point_row(metrics: &OperatorPointShardMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Uint2(metrics.shard as u16)],
		measures: vec![
			level_bytes("used", metrics.used),
			level_bytes("limit", metrics.limit),
			level_count("entries", metrics.entries as u64),
			counter_count("hits", metrics.counters.hits),
			counter_count("misses", metrics.counters.misses),
			counter_count("insertions", metrics.counters.insertions),
			counter_count("evictions", metrics.counters.evictions),
			counter_count("fills_started", metrics.counters.fills_started),
			counter_count("fills_dirty_aborted", metrics.counters.fills_dirty_aborted),
			counter_count("fills_duplicate", metrics.counters.fills_duplicate),
		],
	}
}

fn operator_point_keyspace_rows(store: &OperatorStore) -> Vec<MetricsRow> {
	store.point_keyspace_metrics().iter().map(operator_point_keyspace_row).collect()
}

fn operator_point_keyspace_row(metrics: &OperatorPointKeyspaceMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Utf8(metrics.slot.name().to_string())],
		measures: vec![
			level_bytes("used", metrics.used),
			level_count("entries", metrics.entries as u64),
			counter_count("hits", metrics.counters.hits),
			counter_count("misses", metrics.counters.misses),
			counter_count("insertions", metrics.counters.insertions),
			counter_count("evictions", metrics.counters.evictions),
			counter_count("fills_started", metrics.counters.fills_started),
			counter_count("fills_dirty_aborted", metrics.counters.fills_dirty_aborted),
			counter_count("fills_duplicate", metrics.counters.fills_duplicate),
		],
	}
}

fn operator_range_rows(store: &OperatorStore) -> Vec<MetricsRow> {
	store.range_shard_metrics().iter().map(operator_range_row).collect()
}

fn operator_range_row(metrics: &OperatorRangeShardMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Uint2(metrics.shard as u16)],
		measures: vec![
			level_bytes("used", metrics.used),
			level_bytes("limit", metrics.limit),
			level_count("partitions", metrics.partitions as u64),
			level_count("entries", metrics.entries as u64),
			counter_count("hits", metrics.counters.hits),
			counter_count("misses", metrics.counters.misses),
			counter_count("materializes", metrics.counters.materializes),
			counter_count("materializes_refused", metrics.counters.materializes_refused),
			counter_count("materializes_raced", metrics.counters.materializes_raced),
			counter_count("evictions", metrics.counters.evictions),
			counter_count("point_hits", metrics.counters.point_hits),
			counter_count("point_misses", metrics.counters.point_misses),
		],
	}
}

fn operator_range_keyspace_rows(store: &OperatorStore) -> Vec<MetricsRow> {
	store.range_keyspace_metrics().iter().map(operator_range_keyspace_row).collect()
}

fn operator_range_keyspace_row(metrics: &OperatorRangeKeyspaceMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Utf8(metrics.slot.name().to_string())],
		measures: vec![
			level_bytes("used", metrics.used),
			level_count("partitions", metrics.partitions as u64),
			level_count("intervals", metrics.intervals as u64),
			level_count("entries", metrics.entries as u64),
			counter_count("hits", metrics.counters.hits),
			counter_count("misses", metrics.counters.misses),
			counter_count("materializes", metrics.counters.materializes),
			counter_count("materializes_refused", metrics.counters.materializes_refused),
			counter_count("materializes_raced", metrics.counters.materializes_raced),
			counter_count("evictions", metrics.counters.evictions),
			counter_count("point_hits", metrics.counters.point_hits),
			counter_count("point_misses", metrics.counters.point_misses),
		],
	}
}

fn multi_persistent_rows(store: &MultiStore) -> Vec<MetricsRow> {
	let Some(metrics) = store.persistent_page_cache_metrics() else {
		return Vec::new();
	};
	let probe = store.persistent_probe_metrics().unwrap_or_default();
	let filter = store.persistent_filter_metrics().unwrap_or_default();
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_bytes("used", metrics.used),
			level_count("connections_sampled", metrics.connections_sampled.as_u64()),
			level_count("connections_total", metrics.connections_total.as_u64()),
			counter_count("hits", metrics.hits.as_u64()),
			counter_count("misses", metrics.misses.as_u64()),
			counter_count("persistent_probes", probe.persistent_probes.as_u64()),
			counter_count("persistent_absent", probe.persistent_absent.as_u64()),
			level_ratio("filter_fill_ratio", filter.fill_ratio),
			level_count("filter_estimated_keys", filter.estimated_keys),
			level_count("filter_rejected", filter.rejected),
			level_count("filter_enabled", filter.enabled as u64),
			level_count("filter_rebuilds", filter.rebuilds),
		],
	}]
}

fn single_persistent_rows(store: &SingleStore) -> Vec<MetricsRow> {
	let Some(metrics) = store.persistent_page_cache_metrics() else {
		return Vec::new();
	};
	let probe = store.persistent_probe_metrics().unwrap_or_default();
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_bytes("used", metrics.used),
			level_count("connections_sampled", metrics.connections_sampled.as_u64()),
			level_count("connections_total", metrics.connections_total.as_u64()),
			counter_count("hits", metrics.hits.as_u64()),
			counter_count("misses", metrics.misses.as_u64()),
			counter_count("persistent_probes", probe.persistent_probes.as_u64()),
			counter_count("persistent_absent", probe.persistent_absent.as_u64()),
		],
	}]
}

fn operator_persistent_rows(store: &OperatorStore) -> Vec<MetricsRow> {
	let Some(metrics) = store.persistent_page_cache_metrics() else {
		return Vec::new();
	};
	let filter = store.persistent_filter_metrics().unwrap_or_default();
	vec![MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_bytes("used", metrics.used),
			level_count("connections_sampled", metrics.connections_sampled.as_u64()),
			level_count("connections_total", metrics.connections_total.as_u64()),
			counter_count("hits", metrics.hits.as_u64()),
			counter_count("misses", metrics.misses.as_u64()),
			level_ratio("filter_fill_ratio", filter.fill_ratio),
			level_count("filter_estimated_keys", filter.estimated_keys),
			level_count("filter_rejected", filter.rejected),
			level_count("filter_enabled", filter.enabled as u64),
			level_count("filter_rebuilds", filter.rebuilds),
		],
	}]
}

fn cdc_commit_rows(store: &CdcStore) -> Vec<MetricsRow> {
	vec![cdc_commit_row(&store.commit_metrics())]
}

fn cdc_commit_row(metrics: &CdcCommitMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_bytes("resident_bytes", metrics.resident_bytes),
			level_count("entries", metrics.entries.as_u64()),
			counter_count("blocks_cut", metrics.blocks_cut),
			counter_count("stalls", metrics.stalls),
		],
	}
}

fn cdc_read_rows(store: &CdcStore) -> Vec<MetricsRow> {
	store.read_buffer_shard_metrics().iter().map(cdc_read_row).collect()
}

fn cdc_read_row(metrics: &CdcReadShardMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Uint2(metrics.shard as u16)],
		measures: vec![
			level_bytes("used", metrics.used),
			level_bytes("limit", metrics.limit),
			level_count("blocks", metrics.blocks as u64),
			counter_count("hits", metrics.counters.hits),
			counter_count("misses", metrics.counters.misses),
			counter_count("insertions", metrics.counters.insertions),
			counter_count("evictions", metrics.counters.evictions),
		],
	}
}

fn cdc_persistent_rows(store: &CdcStore) -> Vec<MetricsRow> {
	vec![cdc_persistent_row(&store.persistent_metrics())]
}

fn cdc_persistent_row(metrics: &CdcPersistentMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: Vec::new(),
		measures: vec![
			level_count("blocks", metrics.blocks),
			level_bytes("stored_bytes", metrics.stored_bytes),
			counter_count("appends", metrics.appends),
			counter_count("loads", metrics.loads),
			counter_count("drops", metrics.drops),
		],
	}
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
	use reifydb_core::key::operator_state::Keyspace;
	use reifydb_store_cdc::tier::read::CdcReadMetrics;
	use reifydb_store_operator::tier::{point::OperatorPointMetrics, range::OperatorRangeMetrics};

	use super::*;

	fn point_sample() -> OperatorPointKeyspaceMetrics {
		OperatorPointKeyspaceMetrics {
			slot: Keyspace::SOURCE_WATERMARK,
			used: ByteSize::from_bytes(12_401),
			entries: 231,
			counters: OperatorPointMetrics {
				hits: 367_918,
				misses: 2_944,
				insertions: 1_884,
				evictions: 51,
				fills_started: 3_001,
				fills_dirty_aborted: 7,
				fills_duplicate: 13,
			},
		}
	}

	fn range_sample() -> OperatorRangeKeyspaceMetrics {
		OperatorRangeKeyspaceMetrics {
			slot: Keyspace::SOURCE_WATERMARK,
			used: ByteSize::from_bytes(20_733),
			partitions: 115,
			intervals: 203,
			entries: 419,
			counters: OperatorRangeMetrics {
				hits: 1_207,
				misses: 89,
				materializes: 41,
				materializes_refused: 5,
				materializes_raced: 2,
				evictions: 63,
				point_hits: 704,
				point_misses: 22,
			},
		}
	}

	#[test]
	fn point_keyspace_row_names_the_keyspace_rather_than_numbering_it() {
		let row = operator_point_keyspace_row(&point_sample());
		assert_eq!(
			row.dimensions,
			vec![Value::Utf8("SOURCE_WATERMARK".to_string())],
			"the dimension must be the keyspace name; a raw u8 breaks the moment a constant is renumbered"
		);
	}

	#[test]
	fn range_keyspace_row_names_the_keyspace_rather_than_numbering_it() {
		let row = operator_range_keyspace_row(&range_sample());
		assert_eq!(
			row.dimensions,
			vec![Value::Utf8("SOURCE_WATERMARK".to_string())],
			"the dimension must be the keyspace name; a raw u8 breaks the moment a constant is renumbered"
		);
	}

	#[test]
	fn the_real_custom_keyspace_keeps_its_plain_name() {
		// CUSTOM_NOT_CACHED and CUSTOM_CACHED are declared constants, not gaps: relabelling either as
		// CUSTOM_0x40 hides which admission side a keyspace sits on.
		let mut metrics = point_sample();
		metrics.slot = Keyspace::CUSTOM_NOT_CACHED;
		let row = operator_point_keyspace_row(&metrics);
		assert_eq!(row.dimensions, vec![Value::Utf8("CUSTOM_NOT_CACHED".to_string())]);

		let mut metrics = point_sample();
		metrics.slot = Keyspace::CUSTOM_CACHED;
		let row = operator_point_keyspace_row(&metrics);
		assert_eq!(row.dimensions, vec![Value::Utf8("CUSTOM_CACHED".to_string())]);

		let mut metrics = range_sample();
		metrics.slot = Keyspace::CUSTOM_NOT_CACHED;
		let row = operator_range_keyspace_row(&metrics);
		assert_eq!(row.dimensions, vec![Value::Utf8("CUSTOM_NOT_CACHED".to_string())]);
	}

	#[test]
	fn point_keyspace_row_carries_every_declared_measure_exactly_once() {
		let row = operator_point_keyspace_row(&point_sample());
		let declared: Vec<&str> =
			MetricsDomain::StoreOperatorPointKeyspace.spec().measures.iter().map(|m| m.name).collect();
		let built: Vec<&str> = row.measures.iter().map(|m| m.metric).collect();
		assert_eq!(built, declared, "a declared measure the row omits publishes as none forever");
	}

	#[test]
	fn range_keyspace_row_carries_every_declared_measure_exactly_once() {
		let row = operator_range_keyspace_row(&range_sample());
		let declared: Vec<&str> =
			MetricsDomain::StoreOperatorRangeKeyspace.spec().measures.iter().map(|m| m.name).collect();
		let built: Vec<&str> = row.measures.iter().map(|m| m.metric).collect();
		assert_eq!(built, declared, "a declared measure the row omits publishes as none forever");
	}

	#[test]
	fn point_shard_row_carries_every_declared_measure_exactly_once() {
		let row = operator_point_row(&OperatorPointShardMetrics {
			shard: 3,
			used: ByteSize::from_bytes(900),
			limit: ByteSize::from_bytes(4_096),
			entries: 12,
			counters: point_sample().counters,
		});
		let declared: Vec<&str> =
			MetricsDomain::StoreOperatorPoint.spec().measures.iter().map(|m| m.name).collect();
		let built: Vec<&str> = row.measures.iter().map(|m| m.metric).collect();
		assert_eq!(built, declared, "a declared measure the row omits publishes as none forever");
		assert!(
			built.iter().all(|name| *name != "partitions"),
			"the point tier is flat and owns no partitions"
		);
	}

	#[test]
	fn range_shard_row_carries_every_declared_measure_exactly_once() {
		let row = operator_range_row(&OperatorRangeShardMetrics {
			shard: 3,
			used: ByteSize::from_bytes(900),
			limit: ByteSize::from_bytes(4_096),
			partitions: 5,
			entries: 12,
			counters: range_sample().counters,
		});
		let declared: Vec<&str> =
			MetricsDomain::StoreOperatorRange.spec().measures.iter().map(|m| m.name).collect();
		let built: Vec<&str> = row.measures.iter().map(|m| m.metric).collect();
		assert_eq!(built, declared, "a declared measure the row omits publishes as none forever");
	}

	#[test]
	fn point_keyspace_row_maps_each_field_to_its_own_measure() {
		let row = operator_point_keyspace_row(&point_sample());
		let find = |name: &str| {
			row.measures.iter().find(|m| m.metric == name).unwrap_or_else(|| panic!("missing {name}"))
		};

		assert_eq!(find("used").reading, Reading::Bytes(ByteSize::from_bytes(12_401)));
		assert_eq!(find("entries").reading, Reading::Count(Count::new(231)));
		assert_eq!(find("hits").reading, Reading::Count(Count::new(367_918)));
		assert_eq!(find("misses").reading, Reading::Count(Count::new(2_944)));
		assert_eq!(find("insertions").reading, Reading::Count(Count::new(1_884)));
		assert_eq!(find("evictions").reading, Reading::Count(Count::new(51)));
		assert_eq!(find("fills_started").reading, Reading::Count(Count::new(3_001)));
		assert_eq!(find("fills_dirty_aborted").reading, Reading::Count(Count::new(7)));
		assert_eq!(find("fills_duplicate").reading, Reading::Count(Count::new(13)));
	}

	#[test]
	fn range_keyspace_row_maps_each_field_to_its_own_measure() {
		// hits/misses are range hits and misses; point_hits/point_misses count point reads a whole
		// resident bucket answered, so crossing the two pairs reports the wrong cache entirely.
		let row = operator_range_keyspace_row(&range_sample());
		let find = |name: &str| {
			row.measures.iter().find(|m| m.metric == name).unwrap_or_else(|| panic!("missing {name}"))
		};

		assert_eq!(find("used").reading, Reading::Bytes(ByteSize::from_bytes(20_733)));
		assert_eq!(find("partitions").reading, Reading::Count(Count::new(115)));
		assert_eq!(find("entries").reading, Reading::Count(Count::new(419)));
		assert_eq!(find("hits").reading, Reading::Count(Count::new(1_207)));
		assert_eq!(find("misses").reading, Reading::Count(Count::new(89)));
		assert_eq!(find("materializes").reading, Reading::Count(Count::new(41)));
		assert_eq!(find("materializes_refused").reading, Reading::Count(Count::new(5)));
		assert_eq!(find("materializes_raced").reading, Reading::Count(Count::new(2)));
		assert_eq!(find("evictions").reading, Reading::Count(Count::new(63)));
		assert_eq!(find("point_hits").reading, Reading::Count(Count::new(704)));
		assert_eq!(find("point_misses").reading, Reading::Count(Count::new(22)));
	}

	#[test]
	fn point_keyspace_row_kinds_match_the_declared_spec() {
		let row = operator_point_keyspace_row(&point_sample());
		let spec = MetricsDomain::StoreOperatorPointKeyspace.spec();
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
	fn range_keyspace_row_kinds_match_the_declared_spec() {
		let row = operator_range_keyspace_row(&range_sample());
		let spec = MetricsDomain::StoreOperatorRangeKeyspace.spec();
		for measure in &row.measures {
			let declared = spec.measures.iter().find(|m| m.name == measure.metric).expect("declared");
			assert_eq!(
				measure.kind, declared.kind,
				"{} must be pushed with the kind the spec declares, otherwise a level accumulates as a counter",
				measure.metric
			);
		}
	}

	fn cdc_commit_sample() -> CdcCommitMetrics {
		CdcCommitMetrics {
			resident_bytes: ByteSize::from_bytes(8_192),
			entries: Count::new(64),
			blocks_cut: 12,
			stalls: 3,
		}
	}

	fn cdc_read_sample() -> CdcReadShardMetrics {
		CdcReadShardMetrics {
			shard: 5,
			used: ByteSize::from_bytes(4_096),
			limit: ByteSize::from_bytes(32_768),
			blocks: 9,
			counters: CdcReadMetrics {
				hits: 811,
				misses: 47,
				insertions: 52,
				evictions: 6,
			},
		}
	}

	fn cdc_persistent_sample() -> CdcPersistentMetrics {
		CdcPersistentMetrics {
			blocks: 140,
			stored_bytes: ByteSize::from_bytes(1_048_576),
			appends: 141,
			loads: 88,
			drops: 1,
		}
	}

	fn measure<'a>(row: &'a MetricsRow, name: &str) -> &'a Measure {
		row.measures.iter().find(|m| m.metric == name).unwrap_or_else(|| panic!("missing {name}"))
	}

	#[test]
	fn cdc_commit_row_carries_every_declared_measure_exactly_once() {
		let row = cdc_commit_row(&cdc_commit_sample());
		let declared: Vec<&str> =
			MetricsDomain::StoreCdcCommit.spec().measures.iter().map(|m| m.name).collect();
		let built: Vec<&str> = row.measures.iter().map(|m| m.metric).collect();
		assert_eq!(built, declared, "a declared measure the row omits publishes as none forever");
		assert!(row.dimensions.is_empty(), "the commit tier is a singleton and carries no dimension");
	}

	#[test]
	fn cdc_read_row_carries_every_declared_measure_exactly_once() {
		let row = cdc_read_row(&cdc_read_sample());
		let declared: Vec<&str> = MetricsDomain::StoreCdcRead.spec().measures.iter().map(|m| m.name).collect();
		let built: Vec<&str> = row.measures.iter().map(|m| m.metric).collect();
		assert_eq!(built, declared, "a declared measure the row omits publishes as none forever");
		assert_eq!(
			row.dimensions,
			vec![Value::Uint2(5)],
			"the shard index is the only dimension; dropping it collapses every shard onto one row"
		);
	}

	#[test]
	fn cdc_persistent_row_carries_every_declared_measure_exactly_once() {
		let row = cdc_persistent_row(&cdc_persistent_sample());
		let declared: Vec<&str> =
			MetricsDomain::StoreCdcPersistent.spec().measures.iter().map(|m| m.name).collect();
		let built: Vec<&str> = row.measures.iter().map(|m| m.metric).collect();
		assert_eq!(built, declared, "a declared measure the row omits publishes as none forever");
	}

	#[test]
	fn cdc_rows_map_each_field_to_its_own_measure() {
		// blocks_cut counts flushes and stalls counts writer back-pressure waits; crossing the two reports a
		// healthy flush cadence as a stalling writer.
		let commit = cdc_commit_row(&cdc_commit_sample());
		assert_eq!(measure(&commit, "resident_bytes").reading, Reading::Bytes(ByteSize::from_bytes(8_192)));
		assert_eq!(measure(&commit, "entries").reading, Reading::Count(Count::new(64)));
		assert_eq!(measure(&commit, "blocks_cut").reading, Reading::Count(Count::new(12)));
		assert_eq!(measure(&commit, "stalls").reading, Reading::Count(Count::new(3)));

		let read = cdc_read_row(&cdc_read_sample());
		assert_eq!(measure(&read, "used").reading, Reading::Bytes(ByteSize::from_bytes(4_096)));
		assert_eq!(measure(&read, "limit").reading, Reading::Bytes(ByteSize::from_bytes(32_768)));
		assert_eq!(measure(&read, "blocks").reading, Reading::Count(Count::new(9)));
		assert_eq!(measure(&read, "hits").reading, Reading::Count(Count::new(811)));
		assert_eq!(measure(&read, "misses").reading, Reading::Count(Count::new(47)));
		assert_eq!(measure(&read, "insertions").reading, Reading::Count(Count::new(52)));
		assert_eq!(measure(&read, "evictions").reading, Reading::Count(Count::new(6)));

		let persistent = cdc_persistent_row(&cdc_persistent_sample());
		assert_eq!(measure(&persistent, "blocks").reading, Reading::Count(Count::new(140)));
		assert_eq!(
			measure(&persistent, "stored_bytes").reading,
			Reading::Bytes(ByteSize::from_bytes(1_048_576))
		);
		assert_eq!(measure(&persistent, "appends").reading, Reading::Count(Count::new(141)));
		assert_eq!(measure(&persistent, "loads").reading, Reading::Count(Count::new(88)));
		assert_eq!(measure(&persistent, "drops").reading, Reading::Count(Count::new(1)));
	}

	#[test]
	fn cdc_row_kinds_match_the_declared_spec() {
		let cases = [
			(MetricsDomain::StoreCdcCommit, cdc_commit_row(&cdc_commit_sample())),
			(MetricsDomain::StoreCdcRead, cdc_read_row(&cdc_read_sample())),
			(MetricsDomain::StoreCdcPersistent, cdc_persistent_row(&cdc_persistent_sample())),
		];
		for (domain, row) in cases {
			let spec = domain.spec();
			for m in &row.measures {
				let declared = spec.measures.iter().find(|d| d.name == m.metric).expect("declared");
				assert_eq!(
					m.kind, declared.kind,
					"{} must be pushed with the kind the spec declares, otherwise a level accumulates as a counter",
					m.metric
				);
			}
		}
	}

	#[test]
	fn keyspace_rows_report_nothing_for_a_store_without_a_cache_tier() {
		let store = OperatorStore::testing_memory();
		assert!(
			operator_point_keyspace_rows(&store).is_empty(),
			"an absent point tier must publish no rows, never one row of zeros claiming a perfect cache"
		);
		assert!(
			operator_range_keyspace_rows(&store).is_empty(),
			"an absent range tier must publish no rows, never one row of zeros claiming a perfect cache"
		);
	}
}
