// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::{EncodedBytes, RowBuilder};
use reifydb_core::{
	interface::catalog::{
		id::QueueId,
		queue::{
			QueueItemState, QueueItemStatus, decode_queue_item_state, decode_queue_partition_counters,
			encode_queue_item_state, encode_queue_partition_counters,
		},
	},
	key::queue_schedule::{QueueDueKey, QueueItemStateKey, QueuePartitionKey},
};
use reifydb_transaction::{
	change::{QueueAckTransition, QueueRowAck},
	single::SingleTransaction,
};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{datetime::DateTime, row_number::RowNumber},
};
use tracing::debug;

use crate::Result;

pub struct QueueAdmission {
	pub row: RowNumber,
	pub not_before: Option<DateTime>,
}

pub fn admit_ready_items(
	single: &SingleTransaction,
	queue: QueueId,
	partition: u16,
	items: &[QueueAdmission],
) -> Result<u64> {
	if items.is_empty() {
		return Ok(0);
	}

	let lock_key = QueuePartitionKey::encoded(queue, partition);
	let mut tx = single.begin_command_ranged(
		[&lock_key],
		vec![
			QueueItemStateKey::partition_scan(queue, partition),
			QueueDueKey::partition_scan(queue, partition),
		],
	)?;

	let mut admitted = 0u64;
	for item in items {
		let state_key = QueueItemStateKey::encoded(queue, partition, item.row);
		if tx.contains_key(&state_key)? {
			continue;
		}

		let state = QueueItemState::ready(item.not_before);
		tx.set(&state_key, encode_queue_item_state(&state).freeze_bytes())?;
		tx.set(
			&QueueDueKey::encoded(queue, partition, state.due(), item.row),
			EncodedBytes(CowVec::new(vec![])),
		)?;
		admitted += 1;
	}

	if admitted > 0 {
		let mut counters = tx
			.get(&lock_key)?
			.map(|stored| decode_queue_partition_counters(&stored.bytes))
			.unwrap_or_default();
		counters.depth += admitted;
		tx.set(&lock_key, encode_queue_partition_counters(&counters).freeze_bytes())?;
	}

	tx.commit()?;

	Ok(admitted)
}

pub fn apply_ack_transitions(
	single: &SingleTransaction,
	queue: QueueId,
	partition: u16,
	acks: &[QueueRowAck],
) -> Result<u64> {
	if acks.is_empty() {
		return Ok(0);
	}

	let lock_key = QueuePartitionKey::encoded(queue, partition);
	let mut tx = single.begin_command_ranged(
		[&lock_key],
		vec![
			QueueItemStateKey::partition_scan(queue, partition),
			QueueDueKey::partition_scan(queue, partition),
		],
	)?;

	let mut applied = 0u64;
	let mut requeued = 0u64;
	for ack in acks {
		let state_key = QueueItemStateKey::encoded(queue, partition, ack.row_number);
		let Some(stored) = tx.get(&state_key)? else {
			debug!(queue = queue.0, partition, item = ack.row_number.0, "ack has no item state");
			continue;
		};
		let Some(mut state) = decode_queue_item_state(&stored.bytes) else {
			continue;
		};

		if state.status != QueueItemStatus::Leased || state.attempt != ack.attempt {
			debug!(
				queue = queue.0,
				partition,
				item = ack.row_number.0,
				attempt = ack.attempt,
				"ack no longer matches the lease it was issued for"
			);
			continue;
		}

		state.lease_deadline = None;
		match &ack.transition {
			QueueAckTransition::Done => state.status = QueueItemStatus::Done,
			QueueAckTransition::Dead => state.status = QueueItemStatus::Dead,
			QueueAckTransition::Retry {
				not_before,
			} => {
				state.status = QueueItemStatus::Ready;
				state.not_before = Some(*not_before);
				state.backoff_until = Some(*not_before);
				tx.set(
					&QueueDueKey::encoded(queue, partition, *not_before, ack.row_number),
					EncodedBytes(CowVec::new(vec![])),
				)?;
				requeued += 1;
			}
		}

		tx.set(&state_key, encode_queue_item_state(&state).freeze_bytes())?;
		applied += 1;
	}

	if applied > 0 {
		let mut counters = tx
			.get(&lock_key)?
			.map(|stored| decode_queue_partition_counters(&stored.bytes))
			.unwrap_or_default();
		counters.in_flight = counters.in_flight.saturating_sub(applied);
		counters.depth += requeued;
		tx.set(&lock_key, encode_queue_partition_counters(&counters).freeze_bytes())?;
	}

	tx.commit()?;

	Ok(applied)
}
