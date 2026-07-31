// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_core::{common::CommitVersion, interface::catalog::id::QueueId};
use reifydb_transaction::{
	change::{QueueRowAck, RowChange},
	interceptor::transaction::{PostCommitContext, PostCommitInterceptor},
	single::SingleTransaction,
};
use tracing::error;

use crate::{
	Result,
	queue::scheduling::{QueueAdmission, admit_ready_items, apply_ack_transitions},
};

pub struct QueueSchedulingInterceptor {
	single: SingleTransaction,
}

impl QueueSchedulingInterceptor {
	pub fn new(single: SingleTransaction) -> Self {
		Self {
			single,
		}
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
			if let Err(err) = admit_ready_items(&self.single, queue, partition, &items) {
				error!(
					queue = queue.0,
					partition,
					version = ctx.version.0,
					items = items.len(),
					error = %err,
					"queue scheduling handoff failed; hydration will recover these items at next boot"
				);
			}
		}

		for ((queue, partition), items) in acks {
			if let Err(err) = apply_ack_transitions(&self.single, queue, partition, &items) {
				error!(
					queue = queue.0,
					partition,
					version = ctx.version.0,
					items = items.len(),
					error = %err,
					"queue ack transition failed; the lease will expire and the item is redelivered"
				);
			}
		}

		Ok(())
	}
}
