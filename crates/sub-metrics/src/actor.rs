// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	mem,
	sync::Arc,
};

use reifydb_catalog::metrics::storage::{cdc::CdcMetricsWriter, multi::StorageMetricsWriter};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	actors::metrics::MetricsMessage,
	common::CommitVersion,
	event::{
		EventBus,
		metric::{
			CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, MultiDelete, MultiDrop, MultiWrite,
			Request, RequestExecutedEvent,
		},
		store::MetricsProcessedEvent,
	},
	fingerprint::RequestFingerprint,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		store::Tier,
	},
	metrics::execution::StatementMetrics,
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_single::SingleStore;
use reifydb_value::{
	params::Params,
	value::{Value, datetime::DateTime, duration::Duration, identity::IdentityId},
};
use tracing::{error, trace};

use crate::{accumulator::StatementMetricsAccumulator, statement::StatementMetricsAggregate};

fn default_flush_interval() -> Duration {
	Duration::from_seconds(10).unwrap()
}

pub struct MetricsFlushActor {
	accumulator: Arc<StatementMetricsAccumulator>,
	event_bus: EventBus,
	single_store: SingleStore,
	resolver: MultiStore,
	drain: Option<(StandardEngine, Clock)>,
	config: Option<Arc<dyn GetConfig>>,
	flush_interval_override: Option<Duration>,
}

impl MetricsFlushActor {
	pub fn new(
		accumulator: Arc<StatementMetricsAccumulator>,
		event_bus: EventBus,
		single_store: SingleStore,
		resolver: MultiStore,
	) -> Self {
		Self {
			accumulator,
			event_bus,
			single_store,
			resolver,
			drain: None,
			config: None,
			flush_interval_override: None,
		}
	}

	pub fn with_drain(mut self, engine: StandardEngine, clock: Clock) -> Self {
		self.drain = Some((engine, clock));
		self
	}

	pub fn with_flush_interval(mut self, interval: Duration) -> Self {
		self.flush_interval_override = Some(interval);
		self
	}

	pub fn with_config(mut self, config: Arc<dyn GetConfig>) -> Self {
		self.config = Some(config);
		self
	}

	fn effective_interval(&self) -> Duration {
		self.flush_interval_override
			.or_else(|| {
				self.config.as_ref().map(|c| c.get_config_duration(ConfigKey::MetricsFlushInterval))
			})
			.unwrap_or_else(default_flush_interval)
	}

	fn process_multi_committed(&self, state: &mut MetricsFlushActorState, event: MultiCommittedEvent) {
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
		state: &mut MetricsFlushActorState,
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

	fn process_cdc_written(&self, state: &mut MetricsFlushActorState, event: CdcWrittenEvent) {
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

	fn process_cdc_evicted(&self, state: &mut MetricsFlushActorState, event: CdcEvictedEvent) {
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
}

#[inline]
fn record_each_write(
	state: &mut MetricsFlushActorState,
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
fn record_deletes(state: &mut MetricsFlushActorState, deletes: &[MultiDelete]) {
	for delete in deletes {
		if let Err(e) =
			state.storage_writer.record_delete(Tier::Buffer, delete.key.as_ref(), Some(delete.value_bytes))
		{
			error!("Failed to record delete: {}", e);
		}
	}
}

#[inline]
fn record_drops(state: &mut MetricsFlushActorState, drops: &[MultiDrop]) {
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

pub struct MetricsFlushActorState {
	storage_writer: StorageMetricsWriter<SingleStore>,
	cdc_writer: CdcMetricsWriter<SingleStore>,
	max_version: CommitVersion,
	pending: Vec<RequestExecutedEvent>,
}

impl MetricsFlushActor {
	#[inline]
	fn handle_tick(&self, state: &mut MetricsFlushActorState, ctx: &Context<MetricsMessage>) {
		if let Err(e) = state.storage_writer.flush() {
			error!("Failed to flush storage stats: {}", e);
		}
		if let Err(e) = state.cdc_writer.flush() {
			error!("Failed to flush cdc stats: {}", e);
		}
		let pending = mem::take(&mut state.pending);
		self.drain_request_history(pending);
		self.drain_statement_metrics();
		emit_stats_processed(&self.event_bus, &mut state.max_version);
		ctx.schedule_once(self.effective_interval(), || MetricsMessage::Tick(DateTime::from_nanos(0)));
	}
}

impl Actor for MetricsFlushActor {
	type Message = MetricsMessage;
	type State = MetricsFlushActorState;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_once(self.effective_interval(), || MetricsMessage::Tick(DateTime::from_nanos(0)));

		MetricsFlushActorState {
			storage_writer: StorageMetricsWriter::new(self.single_store.clone()),
			cdc_writer: CdcMetricsWriter::new(self.single_store.clone()),
			max_version: CommitVersion(0),
			pending: Vec::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			MetricsMessage::Tick(_) => self.handle_tick(state, ctx),
			MetricsMessage::RequestExecuted(event) => state.pending.push(event),
			MetricsMessage::MultiCommitted(event) => self.process_multi_committed(state, event),
			MetricsMessage::CdcWritten(event) => self.process_cdc_written(state, event),
			MetricsMessage::CdcEvicted(event) => self.process_cdc_evicted(state, event),
		}
		Directive::Continue
	}

	fn post_stop(&self) {}
}

impl MetricsFlushActor {
	fn drain_request_history(&self, pending: Vec<RequestExecutedEvent>) {
		let Some((engine, _)) = &self.drain else {
			return;
		};
		if pending.is_empty() {
			return;
		}
		let rows: Vec<Params> = pending.iter().map(request_history_row).collect();
		let mut builder = engine.bulk_insert_unchecked(IdentityId::system());
		builder.ringbuffer("system::metrics::request_history").rows(rows).done();
		if let Err(e) = builder.execute() {
			error!("Failed to drain request history: {}", e);
		}
	}

	fn drain_statement_metrics(&self) {
		let Some((engine, clock)) = &self.drain else {
			return;
		};
		let snapshot = self.accumulator.snapshot();
		if snapshot.is_empty() {
			return;
		}
		let now = DateTime::from_nanos(clock.now_nanos());
		let rows: Vec<Params> = snapshot
			.iter()
			.map(|(fingerprint, aggregate)| statement_metrics_row(now, fingerprint.to_hex(), aggregate))
			.collect();
		let mut builder = engine.bulk_insert_unchecked(IdentityId::system());
		builder.ringbuffer("system::metrics::statement_stats").rows(rows).done();
		if let Err(e) = builder.execute() {
			error!("Failed to drain statement metrics: {}", e);
		}
	}
}

fn request_parts(request: &Request) -> (&'static str, &RequestFingerprint, &[StatementMetrics]) {
	match request {
		Request::Query {
			fingerprint,
			statements,
		} => ("query", fingerprint, statements),
		Request::Command {
			fingerprint,
			statements,
		} => ("command", fingerprint, statements),
		Request::Admin {
			fingerprint,
			statements,
		} => ("admin", fingerprint, statements),
	}
}

fn request_history_row(event: &RequestExecutedEvent) -> Params {
	let (operation, fingerprint, statements) = request_parts(event.request());
	let normalized_rql = statements.iter().map(|s| s.normalized_rql.as_str()).collect::<Vec<&str>>().join("; ");
	let mut row = HashMap::new();
	row.insert("timestamp".to_string(), Value::DateTime(*event.timestamp()));
	row.insert("operation".to_string(), Value::Utf8(operation.to_string()));
	row.insert("fingerprint".to_string(), Value::Utf8(fingerprint.to_hex()));
	row.insert("total_duration".to_string(), Value::Duration(*event.total()));
	row.insert("compute_duration".to_string(), Value::Duration(*event.compute()));
	row.insert("success".to_string(), Value::Boolean(*event.success()));
	row.insert("statement_count".to_string(), Value::Int8(statements.len() as i64));
	row.insert("normalized_rql".to_string(), Value::Utf8(normalized_rql));
	Params::Named(Arc::new(row))
}

fn statement_metrics_row(now: DateTime, fingerprint: String, aggregate: &StatementMetricsAggregate) -> Params {
	let mut row = HashMap::new();
	row.insert("snapshot_timestamp".to_string(), Value::DateTime(now));
	row.insert("fingerprint".to_string(), Value::Utf8(fingerprint));
	row.insert("normalized_rql".to_string(), Value::Utf8(aggregate.normalized_rql().to_string()));
	row.insert("calls".to_string(), Value::Int8(aggregate.calls() as i64));
	row.insert("total_duration".to_string(), Value::Duration(aggregate.total_duration()));
	row.insert("mean_duration".to_string(), Value::Duration(aggregate.mean_duration()));
	row.insert("max_duration".to_string(), Value::Duration(aggregate.max_duration()));
	row.insert("min_duration".to_string(), Value::Duration(aggregate.min_duration()));
	row.insert("total_rows".to_string(), Value::Int8(aggregate.total_rows() as i64));
	row.insert("errors".to_string(), Value::Int8(aggregate.errors() as i64));
	Params::Named(Arc::new(row))
}

fn emit_stats_processed(event_bus: &EventBus, max_version: &mut CommitVersion) {
	if max_version.0 > 0 {
		event_bus.emit(MetricsProcessedEvent::new(*max_version));
		*max_version = CommitVersion(0);
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		actors::metrics::MetricsMessage,
		event::metric::{Request, RequestExecutedEvent},
		fingerprint::{RequestFingerprint, StatementFingerprint},
		metrics::execution::StatementMetrics,
	};
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	#[test]
	fn test_metric_message_construction() {
		let event = RequestExecutedEvent::new(
			Request::Query {
				fingerprint: RequestFingerprint::default(),
				statements: vec![StatementMetrics {
					fingerprint: StatementFingerprint::new(1),
					normalized_rql: "From x".to_string(),
					compile_duration: Duration::zero(),
					execute_duration: Duration::zero(),
					rows_affected: 1,
				}],
			},
			Duration::from_microseconds(100).unwrap(),
			Duration::from_microseconds(50).unwrap(),
			true,
			DateTime::from_timestamp_millis(1000).unwrap(),
		);

		let _tick = MetricsMessage::Tick(DateTime::from_nanos(0));
		let _req = MetricsMessage::RequestExecuted(event);
	}
}
