// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{mem, sync::Arc, time::Duration as StdDuration};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	actors::metric::MetricMessage,
	encoded::shape::RowShape,
	event::metric::{Request, RequestExecutedEvent},
	interface::catalog::ringbuffer::{RingBuffer, RingBufferMetadata},
	key::row::RowKey,
};
use reifydb_engine::{engine::StandardEngine, transaction::operation::ringbuffer::RingBufferOperations};
use reifydb_metric::{accumulator::StatementStatsAccumulator, registry::MetricRegistry};
use reifydb_runtime::actor::{
	context::Context,
	traits::{Actor, Directive},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{
	Result as TypeResult,
	value::{
		Value, datetime::DateTime, duration::Duration as ReifyDuration, identity::IdentityId,
		row_number::RowNumber,
	},
};
use tracing::{error, warn};

pub struct MetricActorState {
	request_history_rb: Option<(RingBuffer, RowShape)>,
	statement_stats_rb: Option<(RingBuffer, RowShape)>,
	pending: Vec<RequestExecutedEvent>,
}

pub struct MetricCollectorActor {
	registry: Arc<MetricRegistry>,
	accumulator: Arc<StatementStatsAccumulator>,
	engine: StandardEngine,
	catalog: Catalog,
	flush_interval: StdDuration,
}

impl MetricCollectorActor {
	pub fn new(
		registry: Arc<MetricRegistry>,
		accumulator: Arc<StatementStatsAccumulator>,
		engine: StandardEngine,
		catalog: Catalog,
	) -> Self {
		Self {
			registry,
			accumulator,
			engine,
			catalog,
			flush_interval: StdDuration::from_secs(10),
		}
	}

	fn lookup_ringbuffer(&self, name: &str) -> Option<(RingBuffer, RowShape)> {
		let mut query = match self.engine.begin_query(IdentityId::system()) {
			Ok(q) => q,
			Err(e) => {
				warn!("Failed to begin query for ring buffer lookup: {e}");
				return None;
			}
		};

		let ns = match self
			.catalog
			.find_namespace_by_path(&mut Transaction::Query(&mut query), "system::metrics")
		{
			Ok(Some(ns)) => ns,
			Ok(None) => {
				warn!(
					"system::metrics namespace not found — metrics ring buffers not bootstrapped yet"
				);
				return None;
			}
			Err(e) => {
				warn!("Failed to find system::metrics namespace: {e}");
				return None;
			}
		};

		match self.catalog.find_ringbuffer_by_name(&mut Transaction::Query(&mut query), ns.id(), name) {
			Ok(Some(rb)) => {
				let shape = RowShape::from(rb.columns.as_slice());
				Some((rb, shape))
			}
			Ok(None) => {
				warn!("Ring buffer '{name}' not found in system::metrics");
				None
			}
			Err(e) => {
				warn!("Failed to find ring buffer '{name}': {e}");
				None
			}
		}
	}

	fn enqueue(&self, events: &[RequestExecutedEvent], rb: &RingBuffer, shape: &RowShape) {
		let mut txn = match self.engine.begin_command(IdentityId::system()) {
			Ok(txn) => txn,
			Err(e) => {
				error!("Failed to persist request history: {e}");
				return;
			}
		};
		let now_nanos = self.engine.clock().now_nanos();

		let result: TypeResult<()> = (|| {
			let mut metadata = self
				.catalog
				.find_ringbuffer_metadata(&mut Transaction::Command(&mut txn), rb.id)?
				.unwrap_or_else(|| RingBufferMetadata::new(rb.id, rb.capacity));

			for event in events {
				let (operation, fingerprint, statements) = match event.request() {
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
				};

				let normalized_rql =
					statements.first().map(|s| s.normalized_rql.as_str()).unwrap_or("");

				let mut row = shape.allocate();
				shape.set_values(
					&mut row,
					&[
						Value::DateTime(*event.timestamp()),
						Value::Utf8(operation.to_string()),
						Value::Utf8(fingerprint.0.to_hex_string()),
						Value::Duration(*event.total()),
						Value::Duration(*event.compute()),
						Value::Boolean(*event.success()),
						Value::Int8(statements.len() as i64),
						Value::Utf8(normalized_rql.to_string()),
					],
				);
				row.set_timestamps(now_nanos, now_nanos);

				if metadata.is_full() {
					let evict_pos = metadata.head;
					txn.remove_from_ringbuffer(rb, RowNumber(evict_pos))?;
					metadata.head = evict_pos + 1;
					while metadata.head < metadata.tail {
						let key = RowKey::encoded(rb.id, RowNumber(metadata.head));
						if txn.get(&key)?.is_some() {
							break;
						}
						metadata.head += 1;
					}
					metadata.count -= 1;
				}

				let row_number = self.catalog.next_row_number_for_ringbuffer(&mut txn, rb.id)?;
				txn.insert_ringbuffer_at(rb, shape, row_number, row)?;

				if metadata.is_empty() {
					metadata.head = row_number.0;
				}
				metadata.count += 1;
				metadata.tail = row_number.0 + 1;
			}

			self.catalog.update_ringbuffer_metadata(&mut txn, metadata)?;
			txn.commit()?;
			Ok(())
		})();

		if let Err(e) = result {
			error!("Failed to persist request history: {e}");
		}
	}

	fn persist_stats(&self, rb: &RingBuffer, shape: &RowShape) {
		if let Err(e) = self.try_persist_stats(rb, shape) {
			error!("Failed to persist statement stats: {e}");
		}
	}

	fn try_persist_stats(&self, rb: &RingBuffer, shape: &RowShape) -> TypeResult<()> {
		let _registry_snap = self.registry.snapshot();
		let acc_snap = self.accumulator.snapshot();

		if acc_snap.is_empty() {
			return Ok(());
		}

		let mut txn = self.engine.begin_command(IdentityId::system())?;
		let now_nanos = self.engine.clock().now_nanos();
		let snapshot_timestamp = DateTime::from_timestamp_millis(self.engine.clock().now_millis()).unwrap();

		let mut metadata = self
			.catalog
			.find_ringbuffer_metadata(&mut Transaction::Command(&mut txn), rb.id)?
			.unwrap_or_else(|| RingBufferMetadata::new(rb.id, rb.capacity));

		for (_fingerprint, stats) in &acc_snap {
			// Evict if full
			if metadata.is_full() {
				let evict_pos = metadata.head;
				txn.remove_from_ringbuffer(rb, RowNumber(evict_pos))?;
				metadata.head = evict_pos + 1;
				while metadata.head < metadata.tail {
					let key = RowKey::encoded(rb.id, RowNumber(metadata.head));
					if txn.get(&key)?.is_some() {
						break;
					}
					metadata.head += 1;
				}
				metadata.count -= 1;
			}

			let mut row = shape.allocate();
			shape.set_values(
				&mut row,
				&[
					Value::DateTime(snapshot_timestamp),
					Value::Utf8(_fingerprint.0.to_hex_string()),
					Value::Utf8(stats.normalized_rql().to_string()),
					Value::Int8(stats.calls() as i64),
					Value::Duration(
						ReifyDuration::from_microseconds(stats.total_duration_us() as i64)
							.unwrap(),
					),
					Value::Duration(
						ReifyDuration::from_microseconds(stats.mean_duration_us() as i64)
							.unwrap(),
					),
					Value::Duration(
						ReifyDuration::from_microseconds(stats.max_duration_us() as i64)
							.unwrap(),
					),
					Value::Duration(
						ReifyDuration::from_microseconds(stats.min_duration_us() as i64)
							.unwrap(),
					),
					Value::Int8(stats.total_rows() as i64),
					Value::Int8(stats.errors() as i64),
				],
			);
			row.set_timestamps(now_nanos, now_nanos);

			let row_number = self.catalog.next_row_number_for_ringbuffer(&mut txn, rb.id)?;
			txn.insert_ringbuffer_at(rb, shape, row_number, row)?;

			if metadata.is_empty() {
				metadata.head = row_number.0;
			}
			metadata.count += 1;
			metadata.tail = row_number.0 + 1;
		}

		self.catalog.update_ringbuffer_metadata(&mut txn, metadata)?;
		txn.commit()?;

		Ok(())
	}
}

impl Actor for MetricCollectorActor {
	type Message = MetricMessage;
	type State = MetricActorState;

	fn init(&self, ctx: &Context<Self::Message>) -> Self::State {
		ctx.schedule_tick(self.flush_interval, |nanos| MetricMessage::Tick(DateTime::from_nanos(nanos)));

		MetricActorState {
			request_history_rb: self.lookup_ringbuffer("request_history"),
			statement_stats_rb: self.lookup_ringbuffer("statement_stats"),
			pending: Vec::new(),
		}
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			MetricMessage::Tick(_) => {
				if !state.pending.is_empty()
					&& let Some((ref rb, ref shape)) = state.request_history_rb
				{
					let events = mem::take(&mut state.pending);
					self.enqueue(&events, rb, shape);
				}
				if let Some((ref rb, ref shape)) = state.statement_stats_rb {
					self.persist_stats(rb, shape);
				}
			}
			MetricMessage::RequestExecuted(event) => {
				state.pending.push(event);
			}
		}
		Directive::Continue
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
