// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::bytes::{EncodedBytes, RowBuilder},
};
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
	single::{SingleTransaction, write::SingleWriteTransaction},
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

		if apply_state_transition(&mut tx, queue, partition, ack.row_number, &mut state, &ack.transition)? {
			requeued += 1;
		}
		applied += 1;
	}

	if applied > 0 {
		adjust_counters(&mut tx, &lock_key, applied, requeued)?;
	}

	tx.commit()?;

	Ok(applied)
}

pub struct ExpiredLease {
	pub row: RowNumber,
	pub attempt: u32,
	pub lease_deadline: DateTime,
}

pub fn apply_reap_transition(
	single: &SingleTransaction,
	queue: QueueId,
	partition: u16,
	lease: &ExpiredLease,
	transition: &QueueAckTransition,
	now: DateTime,
) -> Result<bool> {
	let lock_key = QueuePartitionKey::encoded(queue, partition);
	let mut tx = single.begin_command_ranged(
		[&lock_key],
		vec![
			QueueItemStateKey::partition_scan(queue, partition),
			QueueDueKey::partition_scan(queue, partition),
		],
	)?;

	let state_key = QueueItemStateKey::encoded(queue, partition, lease.row);
	let Some(stored) = tx.get(&state_key)? else {
		return Ok(false);
	};
	let Some(mut state) = decode_queue_item_state(&stored.bytes) else {
		return Ok(false);
	};

	if state.status != QueueItemStatus::Leased
		|| state.attempt != lease.attempt
		|| state.lease_deadline != Some(lease.lease_deadline)
		|| lease.lease_deadline > now
	{
		debug!(
			queue = queue.0,
			partition,
			item = lease.row.0,
			attempt = lease.attempt,
			"the lease moved between the reaper's scan and its compare-and-set"
		);
		return Ok(false);
	}

	let requeued = apply_state_transition(&mut tx, queue, partition, lease.row, &mut state, transition)?;
	adjust_counters(&mut tx, &lock_key, 1, u64::from(requeued))?;
	tx.commit()?;

	Ok(true)
}

pub fn remove_item_states(
	single: &SingleTransaction,
	queue: QueueId,
	partition: u16,
	rows: &[RowNumber],
) -> Result<u64> {
	if rows.is_empty() {
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

	let mut removed = 0u64;
	for row in rows {
		let state_key = QueueItemStateKey::encoded(queue, partition, *row);
		let Some(stored) = tx.get(&state_key)? else {
			continue;
		};
		let Some(state) = decode_queue_item_state(&stored.bytes) else {
			tx.remove(&state_key)?;
			removed += 1;
			continue;
		};

		if state.status != QueueItemStatus::Done && state.status != QueueItemStatus::Dead {
			debug!(
				queue = queue.0,
				partition,
				item = row.0,
				"retention skipped an item that stopped being terminal under it"
			);
			continue;
		}

		tx.remove(&state_key)?;
		removed += 1;
	}

	tx.commit()?;

	Ok(removed)
}

fn apply_state_transition(
	tx: &mut SingleWriteTransaction<'_>,
	queue: QueueId,
	partition: u16,
	row: RowNumber,
	state: &mut QueueItemState,
	transition: &QueueAckTransition,
) -> Result<bool> {
	state.lease_deadline = None;

	let requeued = match transition {
		QueueAckTransition::Done => {
			state.status = QueueItemStatus::Done;
			false
		}
		QueueAckTransition::Dead => {
			state.status = QueueItemStatus::Dead;
			false
		}
		QueueAckTransition::Retry {
			backoff_until,
		} => {
			state.status = QueueItemStatus::Ready;
			state.backoff_until = Some(*backoff_until);
			tx.set(
				&QueueDueKey::encoded(queue, partition, state.due(), row),
				EncodedBytes(CowVec::new(vec![])),
			)?;
			true
		}
	};

	tx.set(&QueueItemStateKey::encoded(queue, partition, row), encode_queue_item_state(state).freeze_bytes())?;

	Ok(requeued)
}

fn adjust_counters(
	tx: &mut SingleWriteTransaction<'_>,
	lock_key: &EncodedKey,
	applied: u64,
	requeued: u64,
) -> Result<()> {
	let mut counters =
		tx.get(lock_key)?.map(|stored| decode_queue_partition_counters(&stored.bytes)).unwrap_or_default();
	counters.in_flight = counters.in_flight.saturating_sub(applied);
	counters.depth += requeued;
	tx.set(lock_key, encode_queue_partition_counters(&counters).freeze_bytes())?;

	Ok(())
}
