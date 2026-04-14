// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

#[allow(unused_imports)]
use std::{collections::HashSet, mem, sync::Arc, time::Duration as StdDuration};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::metric::MetricMessage,
	common::CommitVersion,
	encoded::shape::RowShape,
	event::{EventBus, metric::RequestExecutedEvent, store::StatsProcessedEvent},
	interface::{
		catalog::ringbuffer::RingBuffer,
		store::{MultiVersionGetPrevious, Tier},
	},
};
use reifydb_engine::engine::StandardEngine;
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
use reifydb_type::value::datetime::DateTime;
use tracing::{error, trace};

#[allow(dead_code)]
pub struct MetricCollectorActor {
	registry: Arc<MetricRegistry>,
	static_registry: Arc<StaticMetricRegistry>,
	accumulator: Arc<StatementStatsAccumulator>,
	engine: StandardEngine,
	catalog: Catalog,
	event_bus: EventBus,
	single_store: SingleStore,
	resolver: MultiStore,
	flush_interval: StdDuration,
}

impl MetricCollectorActor {
	pub fn new(
		registry: Arc<MetricRegistry>,
		static_registry: Arc<StaticMetricRegistry>,
		accumulator: Arc<StatementStatsAccumulator>,
		engine: StandardEngine,
		catalog: Catalog,
		event_bus: EventBus,
		single_store: SingleStore,
		resolver: MultiStore,
	) -> Self {
		Self {
			registry,
			static_registry,
			accumulator,
			engine,
			catalog,
			event_bus,
			single_store,
			resolver,
			flush_interval: StdDuration::from_secs(10),
		}
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

impl Actor for MetricCollectorActor {
	type Message = MetricMessage;
	type State = MetricActorState;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_tick(self.flush_interval, |nanos| MetricMessage::Tick(DateTime::from_nanos(nanos)));

		MetricActorState {
			storage_writer: StorageStatsWriter::new(self.single_store.clone()),
			cdc_writer: CdcStatsWriter::new(self.single_store.clone()),
			max_version: CommitVersion(0),
			request_history_rb: None,
			statement_stats_rb: None,
			pending: Vec::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			MetricMessage::Tick(_) => {
				let _ = mem::take(&mut state.pending);
				emit_stats_processed(&self.event_bus, &mut state.max_version);
			}
			MetricMessage::RequestExecuted(event) => {
				state.pending.push(event);
			}
			MetricMessage::MultiCommitted(event) => {
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

				// Collect dropped keys first — if a key is dropped in this batch, any
				// write to that key is a fresh insert (not an update to the old entry).
				let dropped_keys: HashSet<_> = drops.iter().map(|d| d.key.clone()).collect();

				for write in writes {
					let pre_value_bytes = if dropped_keys.contains(&write.key) {
						None
					} else {
						self.resolver
							.get_previous_version(&write.key, version)
							.ok()
							.flatten()
							.map(|v| v.row.len() as u64)
					};

					if let Err(e) = state.storage_writer.record_write(
						Tier::Hot,
						write.key.as_ref(),
						write.value_bytes,
						pre_value_bytes,
					) {
						error!("Failed to record write: {}", e);
					}
				}
				for delete in deletes {
					if let Err(e) = state.storage_writer.record_delete(
						Tier::Hot,
						delete.key.as_ref(),
						Some(delete.value_bytes),
					) {
						error!("Failed to record delete: {}", e);
					}
				}
				for drop in drops {
					if let Err(e) = state.storage_writer.record_drop(
						Tier::Hot,
						drop.key.as_ref(),
						drop.value_bytes,
					) {
						error!("Failed to record drop: {}", e);
					}
				}
				if version > state.max_version {
					state.max_version = version;
				}
			}
			MetricMessage::CdcWritten(event) => {
				let version = *event.version();
				let entries = event.entries();
				trace!("Processing {} CDC ops for version {:?}", entries.len(), version);
				for entry in entries {
					if let Err(e) =
						state.cdc_writer.record_cdc(entry.key.as_ref(), entry.value_bytes)
					{
						error!("Failed to record cdc: {}", e);
					}
				}
				if version > state.max_version {
					state.max_version = version;
				}
			}
			MetricMessage::CdcEvicted(event) => {
				let version = *event.version();
				let entries = event.entries();
				trace!("Processing {} CDC drop ops for version {:?}", entries.len(), version);
				for entry in entries {
					if let Err(e) =
						state.cdc_writer.record_drop(entry.key.as_ref(), entry.value_bytes)
					{
						error!("Failed to record cdc drop: {}", e);
					}
				}
				if version > state.max_version {
					state.max_version = version;
				}
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		// Best-effort final emit; state is gone, so nothing further to flush.
	}
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
	use reifydb_type::value::{datetime::DateTime, duration::Duration};

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
