// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::{
		id::QueueId,
		queue::{
			QueueItemState, QueueItemStatus, QueuePartitionCounters, decode_queue_item_state,
			decode_queue_partition_counters, encode_queue_item_state, encode_queue_partition_counters,
		},
	},
	key::queue_schedule::{QueueDueKey, QueueItemStateKey, QueueKeyActiveKey, QueuePartitionKey},
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, row_number::RowNumber},
};
use tracing::debug;

use crate::{
	change::{QueueAckTransition, QueueRowAck},
	queue::chain::{ChainHead, ChainOverlay, chain_add, chain_peek, chain_remove},
	single::{SingleTransaction, write::SingleWriteTransaction},
};

pub struct QueueAdmission {
	pub row: RowNumber,
	pub key_hash: Option<u64>,
	pub not_before: Option<DateTime>,
}

struct TransitionEffect {
	requeued: bool,
	blocked_delta: i64,
}

fn partition_ranges(queue: QueueId, partition: u16) -> Vec<EncodedKeyRange> {
	vec![
		QueueItemStateKey::partition_scan(queue, partition),
		QueueDueKey::partition_scan(queue, partition),
		QueueKeyActiveKey::partition_scan(queue, partition),
	]
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
	let mut tx = single.begin_command_ranged([&lock_key], partition_ranges(queue, partition))?;

	let mut overlay = ChainOverlay::default();
	let mut admitted = 0u64;
	let mut blocked_delta = 0i64;
	for item in items {
		let state_key = QueueItemStateKey::encoded(queue, partition, item.row);
		if tx.contains_key(&state_key)? {
			continue;
		}

		let mut state = QueueItemState::ready(item.not_before);
		state.key_hash = item.key_hash.unwrap_or_default();

		if let Some(key_hash) = item.key_hash {
			match chain_peek(single, &overlay, queue, partition, key_hash)? {
				ChainHead::Empty => {}
				ChainHead::Single(_) => {
					state.status = QueueItemStatus::Parked;
					blocked_delta += 1;
				}
				ChainHead::Multiple(_) => state.status = QueueItemStatus::Parked,
			}
			chain_add(&mut tx, &mut overlay, queue, partition, key_hash, item.row)?;
		}

		tx.set(&state_key, encode_queue_item_state(&state))?;
		if state.status == QueueItemStatus::Ready {
			expose_due(&mut tx, queue, partition, item.row, &state)?;
		}
		admitted += 1;
	}

	if admitted > 0 {
		let mut counters = read_counters(&mut tx, &lock_key)?;
		counters.depth += admitted;
		counters.blocked_keys = counters.blocked_keys.saturating_add_signed(blocked_delta);
		tx.set(&lock_key, encode_queue_partition_counters(&counters))?;
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
	let mut tx = single.begin_command_ranged([&lock_key], partition_ranges(queue, partition))?;

	let mut overlay = ChainOverlay::default();
	let mut applied = 0u64;
	let mut requeued = 0u64;
	let mut blocked_delta = 0i64;
	for ack in acks {
		let state_key = QueueItemStateKey::encoded(queue, partition, ack.row_number);
		let Some(stored) = tx.get(&state_key)? else {
			debug!(queue = queue.0, partition, item = ack.row_number.0, "ack has no item state");
			continue;
		};
		let Some(mut state) = decode_queue_item_state(EncodedPodRow::view(&stored.bytes)) else {
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

		let effect = apply_state_transition(
			single,
			&mut tx,
			&mut overlay,
			TransitionTarget::for_ack(queue, partition, ack),
			&mut state,
			&ack.transition,
		)?;
		if effect.requeued {
			requeued += 1;
		}
		blocked_delta += effect.blocked_delta;
		applied += 1;
	}

	if applied > 0 {
		adjust_counters(&mut tx, &lock_key, applied, requeued, blocked_delta)?;
	}

	tx.commit()?;

	Ok(applied)
}

pub struct ExpiredLease {
	pub row: RowNumber,
	pub attempt: u32,
	pub key_hash: Option<u64>,
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
	let mut tx = single.begin_command_ranged([&lock_key], partition_ranges(queue, partition))?;

	let state_key = QueueItemStateKey::encoded(queue, partition, lease.row);
	let Some(stored) = tx.get(&state_key)? else {
		return Ok(false);
	};
	let Some(mut state) = decode_queue_item_state(EncodedPodRow::view(&stored.bytes)) else {
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

	let mut overlay = ChainOverlay::default();
	let effect = apply_state_transition(
		single,
		&mut tx,
		&mut overlay,
		TransitionTarget::for_lease(queue, partition, lease),
		&mut state,
		transition,
	)?;
	adjust_counters(&mut tx, &lock_key, 1, u64::from(effect.requeued), effect.blocked_delta)?;
	tx.commit()?;

	Ok(true)
}

pub enum ReplayOutcome {
	Ready,
	Parked,
	Unknown,
	Unreadable,
	NotDead(QueueItemStatus),
}

pub fn apply_replay_transition(
	single: &SingleTransaction,
	queue: QueueId,
	partition: u16,
	row: RowNumber,
	key_hash: Option<u64>,
) -> Result<ReplayOutcome> {
	let lock_key = QueuePartitionKey::encoded(queue, partition);
	let mut tx = single.begin_command_ranged([&lock_key], partition_ranges(queue, partition))?;

	let state_key = QueueItemStateKey::encoded(queue, partition, row);
	let Some(stored) = tx.get(&state_key)? else {
		return Ok(ReplayOutcome::Unknown);
	};
	let Some(mut state) = decode_queue_item_state(EncodedPodRow::view(&stored.bytes)) else {
		return Ok(ReplayOutcome::Unreadable);
	};

	if state.status != QueueItemStatus::Dead {
		return Ok(ReplayOutcome::NotDead(state.status));
	}

	state.status = QueueItemStatus::Ready;
	state.budget_base = state.attempt;
	state.backoff_until = None;
	state.lease_deadline = None;

	let mut overlay = ChainOverlay::default();
	let mut blocked_delta = 0i64;
	if let Some(key_hash) = key_hash {
		match chain_peek(single, &overlay, queue, partition, key_hash)? {
			ChainHead::Empty => {}
			ChainHead::Single(_) => {
				state.status = QueueItemStatus::Parked;
				blocked_delta += 1;
			}
			ChainHead::Multiple(_) => state.status = QueueItemStatus::Parked,
		}
		chain_add(&mut tx, &mut overlay, queue, partition, key_hash, row)?;
	}

	tx.set(&state_key, encode_queue_item_state(&state))?;
	if state.status == QueueItemStatus::Ready {
		expose_due(&mut tx, queue, partition, row, &state)?;
	}

	let mut counters = read_counters(&mut tx, &lock_key)?;
	counters.depth += 1;
	counters.blocked_keys = counters.blocked_keys.saturating_add_signed(blocked_delta);
	tx.set(&lock_key, encode_queue_partition_counters(&counters))?;

	tx.commit()?;

	Ok(if state.status == QueueItemStatus::Parked {
		ReplayOutcome::Parked
	} else {
		ReplayOutcome::Ready
	})
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
	let mut tx = single.begin_command_ranged([&lock_key], partition_ranges(queue, partition))?;

	let mut removed = 0u64;
	for row in rows {
		let state_key = QueueItemStateKey::encoded(queue, partition, *row);
		let Some(stored) = tx.get(&state_key)? else {
			continue;
		};
		let Some(state) = decode_queue_item_state(EncodedPodRow::view(&stored.bytes)) else {
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

struct TransitionTarget {
	queue: QueueId,
	partition: u16,
	row: RowNumber,
	key_hash: Option<u64>,
}

impl TransitionTarget {
	fn for_ack(queue: QueueId, partition: u16, ack: &QueueRowAck) -> Self {
		Self {
			queue,
			partition,
			row: ack.row_number,
			key_hash: ack.key_hash,
		}
	}

	fn for_lease(queue: QueueId, partition: u16, lease: &ExpiredLease) -> Self {
		Self {
			queue,
			partition,
			row: lease.row,
			key_hash: lease.key_hash,
		}
	}
}

fn apply_state_transition(
	single: &SingleTransaction,
	tx: &mut SingleWriteTransaction<'_>,
	overlay: &mut ChainOverlay,
	target: TransitionTarget,
	state: &mut QueueItemState,
	transition: &QueueAckTransition,
) -> Result<TransitionEffect> {
	let TransitionTarget {
		queue,
		partition,
		row,
		key_hash,
	} = target;

	state.lease_deadline = None;

	let terminal = match transition {
		QueueAckTransition::Done => {
			state.status = QueueItemStatus::Done;
			true
		}
		QueueAckTransition::Dead => {
			state.status = QueueItemStatus::Dead;
			true
		}
		QueueAckTransition::Retry {
			backoff_until,
		} => {
			state.status = QueueItemStatus::Ready;
			state.backoff_until = Some(*backoff_until);
			expose_due(tx, queue, partition, row, state)?;
			false
		}
	};

	tx.set(&QueueItemStateKey::encoded(queue, partition, row), encode_queue_item_state(state))?;

	let blocked_delta = match (terminal, key_hash) {
		(true, Some(key_hash)) => {
			chain_remove(tx, overlay, queue, partition, key_hash, row)?;
			promote_next(single, tx, overlay, queue, partition, key_hash)?
		}
		_ => 0,
	};

	Ok(TransitionEffect {
		requeued: !terminal,
		blocked_delta,
	})
}

fn promote_next(
	single: &SingleTransaction,
	tx: &mut SingleWriteTransaction<'_>,
	overlay: &ChainOverlay,
	queue: QueueId,
	partition: u16,
	key_hash: u64,
) -> Result<i64> {
	let (successor, blocked_delta) = match chain_peek(single, overlay, queue, partition, key_hash)? {
		ChainHead::Empty => return Ok(0),
		ChainHead::Single(row) => (row, -1),
		ChainHead::Multiple(row) => (row, 0),
	};

	let state_key = QueueItemStateKey::encoded(queue, partition, successor);
	let Some(stored) = tx.get(&state_key)? else {
		debug!(queue = queue.0, partition, item = successor.0, "the successor of a key has no item state");
		return Ok(blocked_delta);
	};
	let Some(mut state) = decode_queue_item_state(EncodedPodRow::view(&stored.bytes)) else {
		return Ok(blocked_delta);
	};

	if state.status != QueueItemStatus::Parked {
		debug!(
			queue = queue.0,
			partition,
			item = successor.0,
			"the successor of a key was not parked when its predecessor finished"
		);
		return Ok(blocked_delta);
	}

	state.status = QueueItemStatus::Ready;
	tx.set(&state_key, encode_queue_item_state(&state))?;
	expose_due(tx, queue, partition, successor, &state)?;

	Ok(blocked_delta)
}

fn expose_due(
	tx: &mut SingleWriteTransaction<'_>,
	queue: QueueId,
	partition: u16,
	row: RowNumber,
	state: &QueueItemState,
) -> Result<()> {
	tx.set(&QueueDueKey::encoded(queue, partition, state.due(), row), EncodedPodRow::new(&[]).into_bytes())
}

fn read_counters(tx: &mut SingleWriteTransaction<'_>, lock_key: &EncodedKey) -> Result<QueuePartitionCounters> {
	Ok(tx.get(lock_key)?
		.map(|stored| decode_queue_partition_counters(EncodedPodRow::view(&stored.bytes)))
		.unwrap_or_default())
}

fn adjust_counters(
	tx: &mut SingleWriteTransaction<'_>,
	lock_key: &EncodedKey,
	applied: u64,
	requeued: u64,
	blocked_delta: i64,
) -> Result<()> {
	let mut counters = read_counters(tx, lock_key)?;
	counters.in_flight = counters.in_flight.saturating_sub(applied);
	counters.depth += requeued;
	counters.blocked_keys = counters.blocked_keys.saturating_add_signed(blocked_delta);
	tx.set(lock_key, encode_queue_partition_counters(&counters))?;

	Ok(())
}
