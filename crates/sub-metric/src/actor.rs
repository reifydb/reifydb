// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#[allow(unused_imports)]
use std::{collections::HashMap, collections::HashSet, mem, sync::Arc, time::Duration as StdDuration};

use reifydb_core::{
	actors::metric::MetricMessage,
	common::CommitVersion,
	encoded::{key::EncodedKey, shape::RowShape},
	event::{
		EventBus,
		metric::{
			CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, MultiDelete, MultiDrop, MultiWrite,
			ProfilerAggregateRow, ProfilerSnapshotEvent, RequestExecutedEvent,
		},
		store::StatsProcessedEvent,
	},
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			ringbuffer::RingBuffer,
		},
		store::Tier,
	},
	profiler::ProfilerCategoryId,
};
use reifydb_metric::{
	accumulator::StatementStatsAccumulator,
	registry::{MetricRegistry, StaticMetricRegistry},
	storage::{cdc::CdcStatsWriter, multi::StorageStatsWriter},
};
use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, Directive},
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_value::value::datetime::DateTime;
use tracing::{error, trace};

use crate::profiler_gauges;

const DEFAULT_FLUSH_INTERVAL: StdDuration = StdDuration::from_secs(10);

#[allow(dead_code)]
pub struct MetricCollectorActor {
	registry: Arc<MetricRegistry>,
	static_registry: Arc<StaticMetricRegistry>,
	accumulator: Arc<StatementStatsAccumulator>,
	event_bus: EventBus,
	single_store: SingleStore,
	resolver: MultiStore,
	config: Option<Arc<dyn GetConfig>>,
	flush_interval_override: Option<StdDuration>,
}

impl MetricCollectorActor {
	pub fn new(
		registry: Arc<MetricRegistry>,
		static_registry: Arc<StaticMetricRegistry>,
		accumulator: Arc<StatementStatsAccumulator>,
		event_bus: EventBus,
		single_store: SingleStore,
		resolver: MultiStore,
	) -> Self {
		Self {
			registry,
			static_registry,
			accumulator,
			event_bus,
			single_store,
			resolver,
			config: None,
			flush_interval_override: None,
		}
	}

	pub fn with_flush_interval(mut self, interval: StdDuration) -> Self {
		self.flush_interval_override = Some(interval);
		self
	}

	pub fn with_config(mut self, config: Arc<dyn GetConfig>) -> Self {
		self.config = Some(config);
		self
	}

	fn effective_interval(&self) -> StdDuration {
		self.flush_interval_override
			.or_else(|| self.config.as_ref().map(|c| c.get_config_duration(ConfigKey::MetricFlushInterval)))
			.unwrap_or(DEFAULT_FLUSH_INTERVAL)
	}

	fn process_multi_committed(&self, state: &mut MetricActorState, event: MultiCommittedEvent) {
		let version = *event.version();
		let writes = event.writes();
		let deletes = event.deletes();
		let drops = event.drops();
		trace!(
			"Processing multi ops for version {:?}: {} writes, {} deletes, {} drops",
			version,
			writes.len(),
			deletes.len(),
			drops.len(),
		);

		let dropped_keys: HashSet<_> = drops.iter().map(|d| d.key.clone()).collect();
		self.record_writes(state, writes, &dropped_keys, version);
		record_deletes(state, deletes);
		record_drops(state, drops);
		advance_max_version(&mut state.max_version, version);
	}

	#[inline]
	fn record_writes(
		&self,
		state: &mut MetricActorState,
		writes: &[MultiWrite],
		dropped_keys: &HashSet<EncodedKey>,
		version: CommitVersion,
	) {
		let pre_sizes = self.read_prior_sizes(writes, dropped_keys, version);
		record_each_write(state, writes, dropped_keys, &pre_sizes);
	}

	#[inline]
	fn read_prior_sizes(
		&self,
		writes: &[MultiWrite],
		dropped_keys: &HashSet<EncodedKey>,
		version: CommitVersion,
	) -> HashMap<EncodedKey, u64> {
		let mut pre_sizes: HashMap<EncodedKey, u64> = HashMap::new();
		if version.0 > 0 {
			let lookup_keys: Vec<EncodedKey> = writes
				.iter()
				.filter(|w| !dropped_keys.contains(&w.key))
				.map(|w| w.key.clone())
				.collect();
			if !lookup_keys.is_empty() {
				match self.resolver.get_many(&lookup_keys, CommitVersion(version.0 - 1)) {
					Ok(rows) => {
						for (key, row) in rows {
							pre_sizes.insert(key, row.row.len() as u64);
						}
					}
					Err(e) => error!("Failed to read previous versions for write metrics: {}", e),
				}
			}
		}
		pre_sizes
	}

	fn process_cdc_written(&self, state: &mut MetricActorState, event: CdcWrittenEvent) {
		let version = *event.version();
		let entries = event.entries();
		trace!("Processing {} CDC ops for version {:?}", entries.len(), version);
		for entry in entries {
			if let Err(e) = state.cdc_writer.record_cdc(entry.key.as_ref(), entry.value_bytes) {
				error!("Failed to record cdc: {}", e);
			}
		}
		advance_max_version(&mut state.max_version, version);
	}

	fn process_cdc_evicted(&self, state: &mut MetricActorState, event: CdcEvictedEvent) {
		let version = *event.version();
		let entries = event.entries();
		trace!("Processing {} CDC drop ops for version {:?}", entries.len(), version);
		for entry in entries {
			if let Err(e) = state.cdc_writer.record_drop(entry.key.as_ref(), entry.value_bytes) {
				error!("Failed to record cdc drop: {}", e);
			}
		}
		advance_max_version(&mut state.max_version, version);
	}

	fn process_profile_snapshot(&self, event: ProfilerSnapshotEvent) {
		let by_category = roll_up_by_category(event.rows());
		self.publish_category_gauges(by_category);
	}

	#[inline]
	fn publish_category_gauges(&self, by_category: HashMap<u8, CategoryRollup>) {
		for (cat_byte, rollup) in by_category {
			let cat_id = ProfilerCategoryId(cat_byte);
			profiler_gauges::ensure_registered(&self.registry, cat_id);
			if let Some(g) = profiler_gauges::gauges_for(cat_id) {
				g.calls.set(rollup.calls as f64);
				g.p50.set(rollup.p50 as f64);
				g.p75.set(rollup.p75 as f64);
				g.p90.set(rollup.p90 as f64);
				g.p95.set(rollup.p95 as f64);
				g.p99.set(rollup.p99 as f64);
			}
		}
	}
}

#[derive(Default)]
struct CategoryRollup {
	calls: u64,
	p50: u32,
	p75: u32,
	p90: u32,
	p95: u32,
	p99: u32,
}

#[inline]
fn roll_up_by_category(rows: &[ProfilerAggregateRow]) -> HashMap<u8, CategoryRollup> {
	let mut by_category: HashMap<u8, CategoryRollup> = HashMap::new();
	for row in rows {
		let entry = by_category.entry(row.category.0).or_default();
		entry.calls = entry.calls.saturating_add(row.calls);
		if row.p50_us > entry.p50 {
			entry.p50 = row.p50_us;
		}
		if row.p75_us > entry.p75 {
			entry.p75 = row.p75_us;
		}
		if row.p90_us > entry.p90 {
			entry.p90 = row.p90_us;
		}
		if row.p95_us > entry.p95 {
			entry.p95 = row.p95_us;
		}
		if row.p99_us > entry.p99 {
			entry.p99 = row.p99_us;
		}
	}
	by_category
}

#[inline]
fn record_each_write(
	state: &mut MetricActorState,
	writes: &[MultiWrite],
	dropped_keys: &HashSet<EncodedKey>,
	pre_sizes: &HashMap<EncodedKey, u64>,
) {
	for write in writes {
		let pre_value_bytes = if dropped_keys.contains(&write.key) {
			None
		} else {
			pre_sizes.get(&write.key).copied()
		};
		if let Err(e) = state.storage_writer.record_write(
			Tier::Buffer,
			write.key.as_ref(),
			write.value_bytes,
			pre_value_bytes,
		) {
			error!("Failed to record write: {}", e);
		}
	}
}

#[inline]
fn record_deletes(state: &mut MetricActorState, deletes: &[MultiDelete]) {
	for delete in deletes {
		if let Err(e) =
			state.storage_writer.record_delete(Tier::Buffer, delete.key.as_ref(), Some(delete.value_bytes))
		{
			error!("Failed to record delete: {}", e);
		}
	}
}

#[inline]
fn record_drops(state: &mut MetricActorState, drops: &[MultiDrop]) {
	for drop in drops {
		if let Err(e) = state.storage_writer.record_drop(Tier::Buffer, drop.key.as_ref(), drop.value_bytes) {
			error!("Failed to record drop: {}", e);
		}
	}
}

#[inline]
fn advance_max_version(max_version: &mut CommitVersion, version: CommitVersion) {
	if version > *max_version {
		*max_version = version;
	}
}

#[allow(dead_code)]
pub struct MetricActorState {
	storage_writer: StorageStatsWriter<SingleStore>,
	cdc_writer: CdcStatsWriter<SingleStore>,
	max_version: CommitVersion,
	request_history_rb: Option<(RingBuffer, RowShape)>,
	statement_stats_rb: Option<(RingBuffer, RowShape)>,
	pending: Vec<RequestExecutedEvent>,
}

impl MetricCollectorActor {
	#[inline]
	fn handle_tick(&self, state: &mut MetricActorState, ctx: &Context<MetricMessage>) {
		if let Err(e) = state.storage_writer.flush() {
			error!("Failed to flush storage stats: {}", e);
		}
		if let Err(e) = state.cdc_writer.flush() {
			error!("Failed to flush cdc stats: {}", e);
		}
		let _ = mem::take(&mut state.pending);
		emit_stats_processed(&self.event_bus, &mut state.max_version);
		ctx.schedule_once(self.effective_interval(), || MetricMessage::Tick(DateTime::from_nanos(0)));
	}
}

impl Actor for MetricCollectorActor {
	type Message = MetricMessage;
	type State = MetricActorState;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(self.effective_interval(), || MetricMessage::Tick(DateTime::from_nanos(0)));

		MetricActorState {
			storage_writer: StorageStatsWriter::new(self.single_store.clone()),
			cdc_writer: CdcStatsWriter::new(self.single_store.clone()),
			max_version: CommitVersion(0),
			request_history_rb: None,
			statement_stats_rb: None,
			pending: Vec::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			MetricMessage::Tick(_) => self.handle_tick(state, ctx),
			MetricMessage::RequestExecuted(event) => state.pending.push(event),
			MetricMessage::MultiCommitted(event) => self.process_multi_committed(state, event),
			MetricMessage::CdcWritten(event) => self.process_cdc_written(state, event),
			MetricMessage::CdcEvicted(event) => self.process_cdc_evicted(state, event),
			MetricMessage::ProfilerSnapshot(event) => self.process_profile_snapshot(event),
		}
		Directive::Continue
	}

	fn post_stop(&self) {}
}

fn emit_stats_processed(event_bus: &EventBus, max_version: &mut CommitVersion) {
	if max_version.0 > 0 {
		event_bus.emit(StatsProcessedEvent::new(*max_version));
		*max_version = CommitVersion(0);
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		actors::metric::MetricMessage,
		event::metric::{Request, RequestExecutedEvent},
		fingerprint::{RequestFingerprint, StatementFingerprint},
		metric::StatementMetric,
	};
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	#[test]
	fn test_metric_message_construction() {
		let event = RequestExecutedEvent::new(
			Request::Query {
				fingerprint: RequestFingerprint::default(),
				statements: vec![StatementMetric {
					fingerprint: StatementFingerprint::new(1),
					normalized_rql: "From x".to_string(),
					compile_duration_us: 0,
					execute_duration_us: 0,
					rows_affected: 1,
				}],
			},
			Duration::from_microseconds(100).unwrap(),
			Duration::from_microseconds(50).unwrap(),
			true,
			DateTime::from_timestamp_millis(1000).unwrap(),
		);

		let _tick = MetricMessage::Tick(DateTime::from_nanos(0));
		let _req = MetricMessage::RequestExecuted(event);
	}
}
