// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound, sync::Arc};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{
		pod::EncodedPodRow, queue_attempt::EncodedQueueAttemptRow,
		queue_deduplication::EncodedQueueDeduplicationRow,
	},
};
use reifydb_core::{
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			id::QueueId,
			queue::{
				Queue, QueueItemStatus, decode_queue_attempt, decode_queue_deduplication,
				decode_queue_item_state,
			},
		},
		store::SingleVersionRange,
	},
	key::{
		EncodableKey, queue_attempt::QueueAttemptKey, queue_deduplication::QueueDeduplicationKey,
		queue_schedule::QueueItemStateKey, row::RowKey,
	},
	lifecycle::{
		class::{Floor, FloorTerm, RetentionClass},
		progress::Progress,
		task::LifecycleTask,
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{multi::RangeScope, queue::scheduling::remove_item_states, transaction::Transaction};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId, row_number::RowNumber},
};
use tracing::{Span, debug, field::Empty, instrument, warn};

use crate::plane::RetentionPlane;

struct ItemCursor {
	queue: QueueId,
	partition: u16,
	last: EncodedKey,
}

struct Doomed {
	row: RowNumber,
	state_only: bool,
}

pub struct QueueRetentionTask {
	engine: StandardEngine,
	plane: RetentionPlane,
	clock: Clock,
	config: Arc<dyn GetConfig>,
	item_cursor: Option<ItemCursor>,
	dedup_cursor: HashMap<QueueId, EncodedKey>,
}

impl QueueRetentionTask {
	pub fn new(engine: StandardEngine, plane: RetentionPlane, clock: Clock, config: Arc<dyn GetConfig>) -> Self {
		Self {
			engine,
			plane,
			clock,
			config,
			item_cursor: None,
			dedup_cursor: HashMap::new(),
		}
	}

	fn queues(&self) -> Result<Vec<Queue>> {
		let mut query_txn = self.engine.begin_query(IdentityId::system())?;
		let mut txn = Transaction::Query(&mut query_txn);
		let mut queues = self.engine.catalog().list_queues(&mut txn)?;
		queues.sort_unstable_by_key(|queue| queue.id.0);
		Ok(queues)
	}

	fn scan_states(
		&self,
		queue: QueueId,
		partition: u16,
		after: Option<&EncodedKey>,
		limit: usize,
	) -> Result<Vec<(EncodedKey, RowNumber, QueueItemStatus, u32)>> {
		let mut range = QueueItemStateKey::partition_scan(queue, partition);
		if let Some(after) = after {
			range.start = Bound::Excluded(after.clone());
		}

		let store = self.engine.single_owned().read_store();
		let batch = SingleVersionRange::range_batch(&store, range, limit as u64)?;

		Ok(batch.items
			.iter()
			.filter_map(|item| {
				let key = QueueItemStateKey::decode(&item.key)?;
				let state = decode_queue_item_state(EncodedPodRow::view(&item.bytes))?;
				Some((item.key.clone(), key.row, state.status, state.attempt))
			})
			.collect())
	}

	fn classify(&self, queue: QueueId, row: RowNumber, attempt: u32, cutoff: DateTime) -> Result<Option<Doomed>> {
		let mut query_txn = self.engine.begin_query(IdentityId::system())?;
		let record = query_txn
			.get(&QueueAttemptKey::encoded(queue, row, attempt))?
			.and_then(|stored| decode_queue_attempt(EncodedQueueAttemptRow::view(&stored.bytes)));

		if let Some(record) = record {
			if record.finished_at > cutoff {
				return Ok(None);
			}
			return Ok(Some(Doomed {
				row,
				state_only: false,
			}));
		}

		if query_txn.get(&RowKey::encoded(queue, row))?.is_some() {
			warn!(
				queue = queue.0,
				item = row.0,
				attempt,
				"a terminal queue item has no attempt record; retention will not delete it without a finish time"
			);
			return Ok(None);
		}

		Ok(Some(Doomed {
			row,
			state_only: true,
		}))
	}

	fn delete_items(&self, queue: QueueId, doomed: &[Doomed]) -> Result<u64> {
		let purge: Vec<&Doomed> = doomed.iter().filter(|item| !item.state_only).collect();
		if purge.is_empty() {
			return Ok(0);
		}

		let mut txn = self.engine.begin_command(IdentityId::system())?;
		let mut attempt_keys = Vec::new();
		for item in &purge {
			let mut stream =
				txn.range(QueueAttemptKey::item_scan(queue, item.row), RangeScope::All, 1024)?;
			while let Some(entry) = stream.next() {
				attempt_keys.push(entry?.key.clone());
			}
		}

		for key in &attempt_keys {
			txn.remove(key)?;
		}
		for item in &purge {
			txn.remove(&RowKey::encoded(queue, item.row))?;
		}
		txn.commit()?;

		Ok(purge.len() as u64)
	}

	fn sweep_deduplication(&mut self, queue: QueueId, now: DateTime, limit: usize) -> Result<(u64, bool)> {
		let mut range = QueueDeduplicationKey::full_scan(queue);
		if let Some(after) = self.dedup_cursor.get(&queue) {
			range.start = Bound::Excluded(after.clone());
		}

		let expired = self.expired_deduplication_keys(range, now, limit)?;
		let drained = expired.scanned < limit;

		if drained {
			self.dedup_cursor.remove(&queue);
		} else if let Some(last) = expired.last {
			self.dedup_cursor.insert(queue, last);
		}

		if expired.keys.is_empty() {
			return Ok((0, drained));
		}

		let mut txn = self.engine.begin_command(IdentityId::system())?;
		for key in &expired.keys {
			txn.remove(key)?;
		}
		txn.commit()?;

		Ok((expired.keys.len() as u64, drained))
	}

	fn expired_deduplication_keys(
		&self,
		range: EncodedKeyRange,
		now: DateTime,
		limit: usize,
	) -> Result<ExpiredDeduplication> {
		let query_txn = self.engine.begin_query(IdentityId::system())?;
		let mut stream = query_txn.range(range, RangeScope::All, limit);

		let mut out = ExpiredDeduplication::default();
		while let Some(entry) = stream.next() {
			let entry = entry?;
			if out.scanned >= limit {
				break;
			}
			out.scanned += 1;
			out.last = Some(entry.key.clone());

			if let Some((_, expires_at)) =
				decode_queue_deduplication(EncodedQueueDeduplicationRow::view(&entry.bytes))
				&& expires_at <= now
			{
				out.keys.push(entry.key.clone());
			}
		}

		Ok(out)
	}

	fn cutoff_of(&self, queue: &Queue, now: DateTime) -> Option<(DateTime, (Floor, FloorTerm))> {
		let ttl = queue.retention.done?;
		let binding = self.plane.cutoff_with_binding(RetentionClass::QueueRetention, now, Some(ttl))?;
		match binding.0 {
			Floor::Instant(cutoff) => Some((cutoff, binding)),
			Floor::Version(_) => None,
		}
	}
}

#[derive(Default)]
struct ExpiredDeduplication {
	keys: Vec<EncodedKey>,
	last: Option<EncodedKey>,
	scanned: usize,
}

impl LifecycleTask for QueueRetentionTask {
	fn name(&self) -> &'static str {
		"queue-retention"
	}

	fn interval(&self) -> Duration {
		self.config.get_config_duration(ConfigKey::QueueRetentionInterval)
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::QueueRetention]
	}

	#[instrument(name = "queue::retention::slice", level = "debug", skip_all, fields(scanned = Empty, deleted = Empty))]
	fn run_slice(&mut self) -> Progress {
		let now = self.clock.now();
		let budget = (self.config.get_config_uint8(ConfigKey::QueueRetentionBatchSize) as usize).max(1);

		let queues = match self.queues() {
			Ok(queues) => queues,
			Err(e) => {
				warn!(error = %e, "queue retention failed to list queues");
				self.plane.record_reclamation(RetentionClass::QueueRetention, None, 0, 0);
				return Progress::Exhausted;
			}
		};

		let resume = self.item_cursor.take();
		let start = match &resume {
			Some(cursor) => queues.iter().position(|queue| queue.id == cursor.queue).unwrap_or(0),
			None => 0,
		};

		let mut scanned = 0usize;
		let mut deleted = 0u64;
		let mut floor = None;
		let mut parked: Option<ItemCursor> = None;
		let mut backlog = 0u64;

		'queues: for queue in &queues[start.min(queues.len())..] {
			let Some((cutoff, binding)) = self.cutoff_of(queue, now) else {
				continue;
			};
			floor = Some(binding);

			let resume_here = resume.as_ref().filter(|cursor| cursor.queue == queue.id);
			let first_partition = resume_here.map(|cursor| cursor.partition).unwrap_or(0);

			for partition in first_partition..queue.partitions() {
				let mut after = resume_here
					.filter(|cursor| cursor.partition == partition)
					.map(|cursor| cursor.last.clone());

				loop {
					let remaining = budget.saturating_sub(scanned);
					if remaining == 0 {
						if let Some(last) = after {
							parked = Some(ItemCursor {
								queue: queue.id,
								partition,
								last,
							});
						}
						break 'queues;
					}

					let states = match self.scan_states(
						queue.id,
						partition,
						after.as_ref(),
						remaining,
					) {
						Ok(states) => states,
						Err(e) => {
							warn!(queue = queue.id.0, partition, error = %e, "queue retention failed to scan item state");
							break;
						}
					};
					if states.is_empty() {
						break;
					}

					scanned += states.len();
					after = states.last().map(|(key, ..)| key.clone());
					let drained = states.len() < remaining;

					let mut doomed = Vec::new();
					for (_, row, status, attempt) in &states {
						if *status != QueueItemStatus::Done && *status != QueueItemStatus::Dead
						{
							continue;
						}
						match self.classify(queue.id, *row, *attempt, cutoff) {
							Ok(Some(item)) => doomed.push(item),
							Ok(None) => {}
							Err(e) => {
								warn!(queue = queue.id.0, item = row.0, error = %e, "queue retention failed to classify a terminal item")
							}
						}
					}

					if !doomed.is_empty() {
						match self.delete_items(queue.id, &doomed) {
							Ok(count) => deleted += count,
							Err(e) => {
								warn!(queue = queue.id.0, error = %e, "queue retention failed to delete items");
								doomed.clear();
							}
						}

						let rows: Vec<RowNumber> = doomed.iter().map(|item| item.row).collect();
						if let Err(e) = remove_item_states(
							&self.engine.single_owned(),
							queue.id,
							partition,
							&rows,
						) {
							warn!(queue = queue.id.0, partition, error = %e, "queue retention failed to remove item state");
						}
					}

					if drained {
						break;
					}
				}
			}

			if queue.deduplicate.is_some() {
				let remaining = budget.saturating_sub(scanned).max(1);
				match self.sweep_deduplication(queue.id, now, remaining) {
					Ok((removed, drained)) => {
						deleted += removed;
						if !drained {
							backlog += 1;
						}
					}
					Err(e) => {
						warn!(queue = queue.id.0, error = %e, "queue retention failed to sweep deduplication records")
					}
				}
			}
		}

		backlog += u64::from(parked.is_some());
		self.item_cursor = parked;
		let span = Span::current();
		span.record("scanned", scanned);
		span.record("deleted", deleted);

		self.plane.record_reclamation(RetentionClass::QueueRetention, floor, deleted, backlog);

		if backlog > 0 {
			debug!(scanned, deleted, "queue retention yielded with a backlog");
			self.plane.record_budget_exhausted(RetentionClass::QueueRetention);
			Progress::Yielded
		} else {
			Progress::Exhausted
		}
	}
}
