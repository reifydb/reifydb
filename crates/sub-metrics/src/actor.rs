// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem, sync::Arc};

use reifydb_catalog::{
	metrics::storage::{
		cdc::{CdcMetrics, CdcMetricsWriter},
		metrics::MetricsReader,
		multi::{MultiStorageMetrics, StorageMetricsWriter},
	},
	vtable::system::metrics::MetricsObject,
};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	actors::metrics::MetricsMessage,
	common::CommitVersion,
	event::{
		EventBus,
		metric::{
			CdcEvictedEvent, CdcWrittenEvent, MultiCommittedEvent, MultiDelete, MultiEviction,
			MultiPersist, MultiSweptEvent, MultiWrite, Request, RequestExecutedEvent,
		},
		store::MetricsProcessedEvent,
	},
	fingerprint::RequestFingerprint,
	interface::{
		catalog::config::{ConfigKey, GetConfig},
		store::Tier,
	},
	key::operator_state::Keyspace,
	metrics::{
		execution::StatementMetrics,
		sample::{MetricKind, Reading},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
};
use reifydb_store_multi::MultiStore;
use reifydb_store_operator::{
	store::OperatorStore,
	types::{ANCHOR_KEY_BYTES, ANCHOR_VALUE_BYTES},
};
use reifydb_store_single::SingleStore;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	count::Count,
	params::Params,
	value::{Value, datetime::DateTime, duration::Duration, identity::IdentityId},
};
use tracing::{error, trace};

use crate::{
	accumulator::StatementMetricsAccumulator,
	domains::epoch::EpochGauge,
	framework::{
		accumulator::{Measure, MetricsRow},
		spec::{MetricsDomain, Surface},
	},
	sampler::SamplerMessage,
	statement::StatementMetricsAggregate,
};

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
	epoch_gauge: Option<Arc<EpochGauge>>,
	sampler: Option<ActorRef<SamplerMessage>>,
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
			epoch_gauge: None,
			sampler: None,
		}
	}

	pub fn with_sampler(mut self, sampler: ActorRef<SamplerMessage>) -> Self {
		self.sampler = Some(sampler);
		self
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

	pub fn with_epoch_gauge(mut self, gauge: Arc<EpochGauge>) -> Self {
		self.epoch_gauge = Some(gauge);
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
		trace!(
			"Processing multi ops for version {:?}: {} writes, {} deletes",
			version,
			writes.len(),
			deletes.len(),
		);

		self.record_writes(state, writes, version);
		record_deletes(state, deletes);
		advance_max_version(&mut state.max_version, version);
	}

	fn process_multi_swept(&self, state: &mut MetricsFlushActorState, event: MultiSweptEvent) {
		let version = *event.version();
		let evictions = event.evictions();
		let persists = event.persists();
		trace!(
			"Processing multi sweep at version {:?}: {} evictions, {} persists",
			version,
			evictions.len(),
			persists.len(),
		);

		record_evictions(state, evictions);
		record_persists(state, persists);
		advance_max_version(&mut state.max_version, version);
	}

	#[inline]
	fn record_writes(&self, state: &mut MetricsFlushActorState, writes: &[MultiWrite], version: CommitVersion) {
		let pre_sizes = self.read_prior_sizes(writes, version);
		record_each_write(state, writes, &pre_sizes);
	}

	#[inline]
	fn read_prior_sizes(&self, writes: &[MultiWrite], version: CommitVersion) -> HashMap<EncodedKey, u64> {
		let mut pre_sizes: HashMap<EncodedKey, u64> = HashMap::new();
		if version.0 > 0 {
			let lookup_keys: Vec<EncodedKey> = writes.iter().map(|w| w.key.clone()).collect();
			if !lookup_keys.is_empty() {
				match self.resolver.get_many(&lookup_keys, CommitVersion(version.0 - 1)) {
					Ok(rows) => {
						for (key, row) in rows {
							pre_sizes.insert(key, row.bytes.len() as u64);
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
			if let Err(e) = state.cdc_writer.record_compaction(
				entry.id,
				entry.key_bytes,
				entry.value_bytes,
				entry.count,
			) {
				error!("Failed to record cdc drop: {}", e);
			}
		}
		advance_max_version(&mut state.max_version, version);
	}
}

fn record_each_write(state: &mut MetricsFlushActorState, writes: &[MultiWrite], pre_sizes: &HashMap<EncodedKey, u64>) {
	for write in writes {
		let pre_value_bytes = pre_sizes.get(&write.key).copied();
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
fn record_evictions(state: &mut MetricsFlushActorState, evictions: &[MultiEviction]) {
	for eviction in evictions {
		if let Err(e) = state.storage_writer.record_eviction(
			Tier::Buffer,
			eviction.key.as_ref(),
			eviction.value_bytes.as_bytes(),
			eviction.current,
		) {
			error!("Failed to record eviction: {}", e);
		}
	}
}

#[inline]
fn record_persists(state: &mut MetricsFlushActorState, persists: &[MultiPersist]) {
	for persist in persists {
		if let Err(e) = state.storage_writer.record_write(
			Tier::Persistent,
			persist.key.as_ref(),
			persist.value_bytes.as_bytes(),
			None,
		) {
			error!("Failed to record persist: {}", e);
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
	fn flush(&self, state: &mut MetricsFlushActorState) {
		if let Err(e) = state.storage_writer.flush() {
			error!("Failed to flush storage stats: {}", e);
		}
		if let Err(e) = state.cdc_writer.flush() {
			error!("Failed to flush cdc stats: {}", e);
		}
		let pending = mem::take(&mut state.pending);
		self.drain_request_history(pending);
		self.drain_statement_metrics();
		self.push_object_metrics();
		emit_stats_processed(&self.event_bus, &mut state.max_version);
	}

	fn push_object_metrics(&self) {
		let Some(sampler) = &self.sampler else {
			return;
		};
		let Some((engine, _)) = &self.drain else {
			return;
		};
		let mut query =
			engine.begin_query(IdentityId::system()).expect("metrics resolution transaction must begin");
		let mut txn = Transaction::Query(&mut query);
		let reader = MetricsReader::new(self.single_store.clone());
		let storage = storage_rows(&reader, &mut txn)
			.expect("storage census must complete; a partial one evicts live rows");
		let _ = sampler.send(SamplerMessage::Push {
			domain: MetricsDomain::Storage,
			surface: Surface::Current,
			rows: storage,
		});
		let cdc =
			cdc_rows(&reader, &mut txn).expect("cdc census must complete; a partial one evicts live rows");
		let _ = sampler.send(SamplerMessage::Push {
			domain: MetricsDomain::Cdc,
			surface: Surface::Current,
			rows: cdc,
		});
		let _ = sampler.send(SamplerMessage::Push {
			domain: MetricsDomain::FlowState,
			surface: Surface::Current,
			rows: flow_state_rows(&engine.operator_state()),
		});
	}

	#[inline]
	fn handle_tick(&self, state: &mut MetricsFlushActorState, ctx: &Context<MetricsMessage>) {
		self.flush(state);
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
			MetricsMessage::Flush => self.flush(state),
			MetricsMessage::RequestExecuted(event) => state.pending.push(event),
			MetricsMessage::MultiCommitted(event) => self.process_multi_committed(state, event),
			MetricsMessage::MultiSwept(event) => self.process_multi_swept(state, event),
			MetricsMessage::CdcWritten(event) => self.process_cdc_written(state, event),
			MetricsMessage::CdcEvicted(event) => self.process_cdc_evicted(state, event),
			MetricsMessage::VersionEpochSampled(event) => {
				if let Some(gauge) = &self.epoch_gauge {
					gauge.record(*event.durable_samples(), *event.pruned());
				}
			}
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
		let now = clock.now();
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

fn tier_name(tier: Tier) -> &'static str {
	match tier {
		Tier::Buffer => "buffer",
		Tier::Persistent => "persistent",
	}
}

fn level_bytes(metric: &'static str, bytes: u64) -> Measure {
	Measure {
		metric,
		reading: Reading::Bytes(ByteSize::from_bytes(bytes)),
		kind: MetricKind::Level,
	}
}

fn level_count(metric: &'static str, count: u64) -> Measure {
	Measure {
		metric,
		reading: Reading::Count(Count::new(count)),
		kind: MetricKind::Level,
	}
}

fn flow_state_rows(store: &OperatorStore) -> Vec<MetricsRow> {
	let mut rows: Vec<MetricsRow> = store
		.census()
		.into_iter()
		.map(|entry| MetricsRow {
			dimensions: vec![
				Value::Uint8(entry.operator.0),
				Value::Utf8(entry.keyspace.name().to_string()),
				Value::Utf8(phase_name(entry.keyspace).to_string()),
			],
			measures: vec![
				level_count("keys", entry.keys),
				level_bytes("key_bytes", entry.key_bytes),
				level_bytes("value_bytes", entry.value_bytes),
				level_bytes("total_bytes", entry.key_bytes + entry.value_bytes),
			],
		})
		.collect();
	rows.extend(store.anchor_census().into_iter().map(|entry| MetricsRow {
		dimensions: vec![
			Value::Uint8(entry.operator.0),
			Value::Utf8(Keyspace::SEAL_ANCHOR.name().to_string()),
			Value::Utf8(phase_name(Keyspace::SEAL_ANCHOR).to_string()),
		],
		measures: vec![
			level_count("keys", entry.keys),
			level_bytes("key_bytes", entry.keys * ANCHOR_KEY_BYTES),
			level_bytes("value_bytes", entry.keys * ANCHOR_VALUE_BYTES),
			level_bytes("total_bytes", entry.keys * (ANCHOR_KEY_BYTES + ANCHOR_VALUE_BYTES)),
		],
	}));
	rows
}

fn phase_name(keyspace: Keyspace) -> &'static str {
	if keyspace.is_data() {
		"data"
	} else {
		"identity"
	}
}

fn storage_rows(reader: &MetricsReader<SingleStore>, txn: &mut Transaction<'_>) -> Result<Vec<MetricsRow>> {
	let mut rows = Vec::new();
	for tier in [Tier::Buffer, Tier::Persistent] {
		for (metric_id, combined) in reader.scan_tier(tier)? {
			let Some(resolved) = MetricsObject::resolve(txn, metric_id)? else {
				continue;
			};
			rows.push(storage_row(
				resolved.object.name(),
				resolved.id,
				resolved.namespace_id,
				tier,
				&combined.storage,
			));
		}
	}
	Ok(rows)
}

fn storage_row(
	object_kind: &'static str,
	id: u64,
	namespace_id: u64,
	tier: Tier,
	storage: &MultiStorageMetrics,
) -> MetricsRow {
	MetricsRow {
		dimensions: vec![
			Value::Utf8(object_kind.to_string()),
			Value::Uint8(id),
			Value::Uint8(namespace_id),
			Value::Utf8(tier_name(tier).to_string()),
		],
		measures: vec![
			level_bytes("live_key_bytes", storage.current_key_bytes),
			level_bytes("live_value_bytes", storage.current_value_bytes),
			level_bytes("live_bytes", storage.current_bytes()),
			level_count("live_count", storage.current_count),
			level_bytes("superseded_key_bytes", storage.historical_key_bytes),
			level_bytes("superseded_value_bytes", storage.historical_value_bytes),
			level_bytes("superseded_bytes", storage.historical_bytes()),
			level_count("superseded_count", storage.historical_count),
			level_bytes("total_bytes", storage.total_bytes()),
		],
	}
}

fn cdc_rows(reader: &MetricsReader<SingleStore>, txn: &mut Transaction<'_>) -> Result<Vec<MetricsRow>> {
	let mut rows = Vec::new();
	for (metric_id, metrics) in reader.cdc_reader().scan_all()? {
		let Some(resolved) = MetricsObject::resolve(txn, metric_id)? else {
			continue;
		};
		rows.push(cdc_row(resolved.object.name(), resolved.id, resolved.namespace_id, &metrics));
	}
	Ok(rows)
}

fn cdc_row(object_kind: &'static str, id: u64, namespace_id: u64, metrics: &CdcMetrics) -> MetricsRow {
	MetricsRow {
		dimensions: vec![Value::Utf8(object_kind.to_string()), Value::Uint8(id), Value::Uint8(namespace_id)],
		measures: vec![
			level_bytes("key_bytes", metrics.key_bytes.as_bytes()),
			level_bytes("value_bytes", metrics.value_bytes.as_bytes()),
			level_bytes("total_bytes", metrics.total_bytes().as_bytes()),
			level_count("count", metrics.entry_count.as_u64()),
		],
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
			DateTime::from_epoch_millis(1000).unwrap(),
		);

		let _tick = MetricsMessage::Tick(DateTime::from_nanos(0));
		let _req = MetricsMessage::RequestExecuted(event);
	}
}
