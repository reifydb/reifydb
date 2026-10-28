// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{common::CommitVersion, interface::catalog::id::QueueId};
use reifydb_runtime::context::clock::Clock;
use reifydb_transaction::{
	change::{QueueAckTransition, QueueRowAck, RowChange},
	interceptor::transaction::{PostCommitContext, PostCommitInterceptor},
	queue::scheduling::{QueueAdmission, admit_ready_items, apply_ack_transitions},
	single::SingleTransaction,
};
use reifydb_value::value::datetime::DateTime;
use tracing::instrument;

use crate::{Result, queue::wake::QueueWakeRegistry};

pub struct QueueSchedulingInterceptor {
	single: SingleTransaction,
	wake: QueueWakeRegistry,
	clock: Clock,
}

impl QueueSchedulingInterceptor {
	pub fn new(single: SingleTransaction, wake: QueueWakeRegistry, clock: Clock) -> Self {
		Self {
			single,
			wake,
			clock,
		}
	}

	#[instrument(
		name = "queue::interceptor::enqueue",
		level = "debug",
		skip_all,
		fields(queue = queue.0, partition = partition, items = items.len())
	)]
	fn admit(&self, queue: QueueId, partition: u16, items: &[QueueAdmission]) -> Result<()> {
		admit_ready_items(&self.single, queue, partition, items)?;
		let now = self.clock.now();
		self.wake.nudge(queue, items.iter().filter(|item| is_due(item.not_before, now)).count());
		Ok(())
	}

	#[instrument(
		name = "queue::interceptor::ack",
		level = "debug",
		skip_all,
		fields(queue = queue.0, partition = partition, items = items.len())
	)]
	fn ack(&self, queue: QueueId, partition: u16, items: &[QueueRowAck]) -> Result<()> {
		apply_ack_transitions(&self.single, queue, partition, items)?;
		let now = self.clock.now();
		self.wake.nudge(queue, items.iter().filter(|ack| releases_work(ack, now)).count());
		Ok(())
	}
}

fn is_due(not_before: Option<DateTime>, now: DateTime) -> bool {
	not_before.is_none_or(|instant| instant <= now)
}

fn releases_work(ack: &QueueRowAck, now: DateTime) -> bool {
	match &ack.transition {
		QueueAckTransition::Retry {
			backoff_until,
		} => *backoff_until <= now,
		QueueAckTransition::Done | QueueAckTransition::Dead => ack.key_hash.is_some(),
	}
}

impl PostCommitInterceptor for QueueSchedulingInterceptor {
	fn intercept(&self, ctx: &mut PostCommitContext) -> Result<()> {
		if ctx.version == CommitVersion(0) || ctx.row_changes.is_empty() {
			return Ok(());
		}

		let mut admissions: BTreeMap<(QueueId, u16), Vec<QueueAdmission>> = BTreeMap::new();
		let mut acks: BTreeMap<(QueueId, u16), Vec<QueueRowAck>> = BTreeMap::new();
		for change in &ctx.row_changes {
			match change {
				RowChange::QueueInsert(insertion) => {
					admissions.entry((insertion.queue_id, insertion.partition)).or_default().push(
						QueueAdmission {
							row: insertion.row_number,
							key_hash: insertion.key_hash,
							not_before: insertion.not_before,
						},
					);
				}
				RowChange::QueueAck(ack) => {
					acks.entry((ack.queue_id, ack.partition)).or_default().push(ack.clone());
				}
				RowChange::TableInsert(_) => {}
			}
		}

		for ((queue, partition), items) in admissions {
			if let Err(err) = self.admit(queue, partition, &items) {
				panic!(
					"queue admit failed after commit: queue {} partition {} version {}: {}",
					queue.0, partition, ctx.version.0, err
				);
			}
		}

		for ((queue, partition), items) in acks {
			if let Err(err) = self.ack(queue, partition, &items) {
				panic!(
					"queue ack failed after commit: queue {} partition {} version {}: {}",
					queue.0, partition, ctx.version.0, err
				);
			}
		}

		Ok(())
	}
}
