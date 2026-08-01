// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	lifecycle::metrics::RetentionMetrics,
	metrics::sample::{MetricKind, MetricsSample, Reading},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_store_multi::{MultiStore, tier::read::ReadBufferShardMetrics};
use reifydb_value::{
	count::Count,
	value::{Value, duration::Duration, value_type::ValueType},
};

use crate::{
	domains::{
		epoch::EpochGauge,
		runtime::collect::{Collectors, collect_memory, collect_operators, collect_watermarks},
	},
	framework::{
		accumulator::{Measure, MetricsAccumulator, MetricsRow},
		spec::{MetricsDomain, Surface},
		surfaces::MetricsSurfaces,
	},
};

#[derive(Clone, Debug)]
pub enum SamplerMessage {
	Tick,
	Push {
		domain: MetricsDomain,
		surface: Surface,
		rows: Vec<MetricsRow>,
	},
}

pub struct MetricsSamplerActor {
	collectors: Collectors,
	multi_store: MultiStore,
	retention_metrics: RetentionMetrics,
	epoch_gauge: Arc<EpochGauge>,
	surfaces: Arc<MetricsSurfaces>,
	clock: Clock,
	interval: Duration,
}

pub struct SamplerState {
	accumulator: MetricsAccumulator,
}

impl MetricsSamplerActor {
	pub fn new(
		collectors: Collectors,
		multi_store: MultiStore,
		retention_metrics: RetentionMetrics,
		epoch_gauge: Arc<EpochGauge>,
		surfaces: Arc<MetricsSurfaces>,
		clock: Clock,
		interval: Duration,
	) -> Self {
		Self {
			collectors,
			multi_store,
			retention_metrics,
			epoch_gauge,
			surfaces,
			clock,
			interval,
		}
	}

	fn sample_and_publish(&self, state: &mut SamplerState) {
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
		let reporters = self.collectors.registry.read_reporters();
		accumulator.push(MetricsDomain::Instruments, Surface::Current, long_rows(reporters.clone()));
		accumulator.push(MetricsDomain::Instruments, Surface::Total, long_rows(reporters));
		accumulator.push(MetricsDomain::ReadBuffer, Surface::Current, read_buffer_rows(&self.multi_store));
		accumulator.push(
			MetricsDomain::Epoch,
			Surface::Current,
			epoch_rows(&self.collectors.engine, &self.epoch_gauge),
		);
		accumulator.push(MetricsDomain::Lifecycle, Surface::Current, lifecycle_rows(&self.retention_metrics));

		for published in accumulator.roll(self.clock.now()) {
			self.surfaces.store(published);
		}
	}
}

impl Actor for MetricsSamplerActor {
	type Message = SamplerMessage;
	type State = SamplerState;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(self.interval, || SamplerMessage::Tick);
		SamplerState {
			accumulator: MetricsAccumulator::new(MetricsDomain::ALL.map(MetricsDomain::spec)),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			SamplerMessage::Tick => {
				self.sample_and_publish(state);
				ctx.schedule_once(self.interval, || SamplerMessage::Tick);
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

fn level_bytes(metric: &'static str, bytes: reifydb_value::byte_size::ByteSize) -> Measure {
	Measure {
		metric,
		reading: Reading::Bytes(bytes),
		kind: MetricKind::Level,
	}
}

fn read_buffer_rows(store: &MultiStore) -> Vec<MetricsRow> {
	store.read_buffer_shard_metrics().iter().map(read_buffer_row).collect()
}

fn read_buffer_row(metrics: &ReadBufferShardMetrics) -> MetricsRow {
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
			let mut measures = vec![
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
			if let Some(freelist) = snapshot.freelist {
				measures.push(level_count("freelist_pages", freelist.freelist_pages));
				measures.push(level_count("page_count", freelist.page_count));
			}
			MetricsRow {
				dimensions: vec![Value::Utf8(class.name().to_string()), binding],
				measures,
			}
		})
		.collect()
}
