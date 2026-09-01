// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{pod::EncodedPodRow, queue_attempt::EncodedQueueAttemptRow},
};
use reifydb_core::{
	interface::{
		catalog::{
			config::{ConfigKey, GetConfig},
			id::QueueId,
			queue::{
				AttemptOutcome, Queue, QueueAttemptRecord, QueueFailure, QueueItemState,
				QueueItemStatus, decode_queue_attempt, decode_queue_item_state, encode_queue_attempt,
				on_failure,
			},
		},
		store::SingleVersionRange,
	},
	key::{Key, queue_attempt::QueueAttemptKey, queue_schedule::QueueItemStateKey},
	lifecycle::{class::RetentionClass, progress::Progress, task::LifecycleTask},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	change::QueueAckTransition,
	queue::scheduling::{ExpiredLease, apply_reap_transition},
	transaction::Transaction,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration, identity::IdentityId, row_number::RowNumber},
};
use tracing::{Span, debug, field::Empty, instrument, warn};

use crate::plane::RetentionPlane;

fn releases_work(queue: &Queue, transition: &QueueAckTransition, now: DateTime) -> bool {
	match transition {
		QueueAckTransition::Retry {
			backoff_until,
		} => *backoff_until <= now,
		QueueAckTransition::Done | QueueAckTransition::Dead => queue.ordered_by().is_some(),
	}
}

struct ReapCursor {
	queue: QueueId,
	partition: u16,
	last: EncodedKey,
}

struct Candidate {
	key: EncodedKey,
	row: RowNumber,
	state: QueueItemState,
}

pub struct QueueLeaseReapTask {
	engine: StandardEngine,
	plane: RetentionPlane,
	clock: Clock,
	config: Arc<dyn GetConfig>,
	cursor: Option<ReapCursor>,
}

impl QueueLeaseReapTask {
	pub fn new(engine: StandardEngine, plane: RetentionPlane, clock: Clock, config: Arc<dyn GetConfig>) -> Self {
		Self {
			engine,
			plane,
			clock,
			config,
			cursor: None,
		}
	}

	fn queues(&self) -> Result<Vec<Queue>> {
		let mut query_txn = self.engine.begin_query(IdentityId::system())?;
		let mut txn = Transaction::Query(&mut query_txn);
		let mut queues = self.engine.catalog().list_queues(&mut txn)?;
		queues.sort_unstable_by_key(|queue| queue.id.0);
		Ok(queues)
	}

	fn scan(
		&self,
		queue: QueueId,
		partition: u16,
		after: Option<&EncodedKey>,
		limit: usize,
	) -> Result<Vec<Candidate>> {
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
				Some(Candidate {
					key: item.key.clone(),
					row: key.row,
					state,
				})
			})
			.collect())
	}

	fn attempt_record(&self, queue: QueueId, row: RowNumber, attempt: u32) -> Result<Option<QueueAttemptRecord>> {
		let mut query_txn = self.engine.begin_query(IdentityId::system())?;
		Ok(query_txn
			.get(&QueueAttemptKey::encoded(queue, row, attempt))?
			.and_then(|stored| decode_queue_attempt(EncodedQueueAttemptRow::view(&stored.bytes))))
	}

	fn write_lost_attempt(&self, queue: QueueId, row: RowNumber, attempt: u32, now: DateTime) -> Result<()> {
		let mut txn = self.engine.begin_command(IdentityId::system())?;
		let key = QueueAttemptKey::encoded(queue, row, attempt);
		if txn.get(&key)?.is_some() {
			return Ok(());
		}

		txn.set(
			&key,
			encode_queue_attempt(&QueueAttemptRecord {
				worker: String::new(),
				outcome: AttemptOutcome::Err,
				response: None,
				finished_at: now,
				lost: true,
				anomaly: None,
			}),
		)?;
		txn.commit()?;

		Ok(())
	}

	fn failure_transition(&self, queue: &Queue, state: &QueueItemState, now: DateTime) -> QueueAckTransition {
		match on_failure(&queue.retry, state, now) {
			QueueFailure::Dead => QueueAckTransition::Dead,
			QueueFailure::Retry {
				backoff_until,
			} => QueueAckTransition::Retry {
				backoff_until,
			},
		}
	}

	fn decide(&self, queue: &Queue, candidate: &Candidate, now: DateTime) -> Result<QueueAckTransition> {
		let attempt = candidate.state.attempt;
		let Some(record) = self.attempt_record(queue.id, candidate.row, attempt)? else {
			self.write_lost_attempt(queue.id, candidate.row, attempt, now)?;
			return Ok(self.failure_transition(queue, &candidate.state, now));
		};

		if record.lost {
			return Ok(self.failure_transition(queue, &candidate.state, now));
		}

		Ok(match record.outcome {
			AttemptOutcome::Ok => QueueAckTransition::Done,
			AttemptOutcome::Err => self.failure_transition(queue, &candidate.state, now),
			AttemptOutcome::Dead => QueueAckTransition::Dead,
		})
	}

	#[instrument(
		name = "queue::reap::item",
		level = "trace",
		skip_all,
		fields(queue = queue.id.0, partition = partition, item = candidate.row.0)
	)]
	fn reap(&self, queue: &Queue, partition: u16, candidate: &Candidate, now: DateTime) -> Result<bool> {
		let Some(lease_deadline) = candidate.state.lease_deadline else {
			return Ok(false);
		};

		let transition = self.decide(queue, candidate, now)?;
		let lease = ExpiredLease {
			row: candidate.row,
			attempt: candidate.state.attempt,
			key_hash: queue.ordered_by().is_some().then_some(candidate.state.key_hash),
			lease_deadline,
		};

		let applied = apply_reap_transition(
			&self.engine.single_owned(),
			queue.id,
			partition,
			&lease,
			&transition,
			now,
		)?;
		if applied && releases_work(queue, &transition, now) {
			self.engine.queue_wake().nudge(queue.id, 1);
		}

		Ok(applied)
	}

	fn is_expired(state: &QueueItemState, now: DateTime) -> bool {
		state.status == QueueItemStatus::Leased && state.lease_deadline.is_some_and(|deadline| deadline <= now)
	}
}

impl LifecycleTask for QueueLeaseReapTask {
	fn name(&self) -> &'static str {
		"queue-lease-reap"
	}

	fn interval(&self) -> Duration {
		self.config.get_config_duration(ConfigKey::QueueLeaseReapInterval)
	}

	fn classes(&self) -> &'static [RetentionClass] {
		&[RetentionClass::QueueLeaseReap]
	}

	#[instrument(name = "queue::reap::slice", level = "debug", skip_all, fields(scanned = Empty, reaped = Empty))]
	fn run_slice(&mut self) -> Progress {
		let now = self.clock.now();
		let budget = (self.config.get_config_uint8(ConfigKey::QueueLeaseReapBatchSize) as usize).max(1);

		let queues = match self.queues() {
			Ok(queues) => queues,
			Err(e) => {
				warn!(error = %e, "queue reaper failed to list queues");
				self.plane.record_reclamation(RetentionClass::QueueLeaseReap, None, 0, 0);
				return Progress::Exhausted;
			}
		};

		let resume = self.cursor.take();
		let start = match &resume {
			Some(cursor) => queues.iter().position(|queue| queue.id == cursor.queue).unwrap_or(0),
			None => 0,
		};

		let mut scanned = 0usize;
		let mut reaped = 0u64;
		let mut parked: Option<ReapCursor> = None;

		'queues: for queue in &queues[start.min(queues.len())..] {
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
							parked = Some(ReapCursor {
								queue: queue.id,
								partition,
								last,
							});
						}
						break 'queues;
					}

					let candidates = match self.scan(queue.id, partition, after.as_ref(), remaining)
					{
						Ok(candidates) => candidates,
						Err(e) => {
							warn!(queue = queue.id.0, partition, error = %e, "queue reaper failed to scan item state");
							break;
						}
					};
					if candidates.is_empty() {
						break;
					}

					scanned += candidates.len();
					after = candidates.last().map(|candidate| candidate.key.clone());
					let drained = candidates.len() < remaining;

					for candidate in &candidates {
						if !Self::is_expired(&candidate.state, now) {
							continue;
						}
						match self.reap(queue, partition, candidate, now) {
							Ok(true) => reaped += 1,
							Ok(false) => {}
							Err(e) => {
								warn!(queue = queue.id.0, partition, item = candidate.row.0, error = %e, "queue reaper failed to transition an expired lease")
							}
						}
					}

					if drained {
						break;
					}
				}
			}
		}

		let span = Span::current();
		span.record("scanned", scanned);
		span.record("reaped", reaped);

		let backlog = u64::from(parked.is_some());
		self.cursor = parked;
		self.plane.record_reclamation(RetentionClass::QueueLeaseReap, None, reaped, backlog);

		if self.cursor.is_some() {
			debug!(scanned, reaped, "queue reaper yielded with a backlog");
			self.plane.record_budget_exhausted(RetentionClass::QueueLeaseReap);
			Progress::Yielded
		} else {
			Progress::Exhausted
		}
	}
}
