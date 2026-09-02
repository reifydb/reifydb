// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, ops::Bound};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator::{
			keyspace::timer::{
				TimerIndex as TimerIndexSpace, TimerIndexKey, TimerWheel as TimerWheelSpace,
				TimerWheelKey, timer_id,
			},
			state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey, keyspace_inner_range},
		},
		typed::direction::Asc,
	},
	state::{
		timer::TimerKind,
		typed::{SuffixBytes, typed_key},
	},
};
use reifydb_store_operator::store::OperatorStore;
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};

use crate::{
	timer::{Timer, TimerDue},
	transaction::{
		FlowTransaction,
		scope::scoped_key,
		state::{StateExtension, StateRange, decode_payload, encode_payload},
	},
};

const MAX_TIMERS_PER_SCAN: usize = 64;

pub struct TimerWheel;

impl TimerWheel {
	pub fn arm(operator: OperatorId, txn: &mut impl FlowTransaction, timer: &Timer) -> Result<()> {
		if !timer.kind.is_maintenance() {
			txn.state_set(
				operator,
				&timer_key(timer.due, timer.kind, &timer.key),
				encode_payload(&timer.key.to_vec())?,
			)?;
			return Ok(());
		}
		let index = index_key(timer.kind, &timer.key);
		let previous = armed_at(operator, txn, &index)?;
		if previous == Some(timer.due) {
			return Ok(());
		}
		if let Some(previous) = previous {
			let stale = timer_key(previous, timer.kind, &timer.key);
			reifydb_assertions! {
				assert!(
					txn.state_get(operator, &stale)?.is_some(),
					"the timer index names an instant the wheel does not hold, so the two have \
					 diverged and the stale wheel row can never be cancelled again; every arm would \
					 then leave a permanent entry and the due probe degrades with it \
					 (operator={}, previous={:?}, requested={:?})",
					operator.0,
					previous,
					timer.due
				);
			}
			txn.state_remove(operator, &stale)?;
		}
		txn.state_set(
			operator,
			&timer_key(timer.due, timer.kind, &timer.key),
			encode_payload(&timer.key.to_vec())?,
		)?;
		txn.state_set(operator, &index, encode_payload(&timer.due)?)?;
		Ok(())
	}

	pub fn disarm(operator: OperatorId, txn: &mut impl FlowTransaction, timer: &Timer) -> Result<()> {
		if timer.kind.is_maintenance() {
			let index = index_key(timer.kind, &timer.key);
			if armed_at(operator, txn, &index)? == Some(timer.due) {
				txn.state_remove(operator, &index)?;
			}
		}
		txn.state_remove(operator, &timer_key(timer.due, timer.kind, &timer.key))?;
		Ok(())
	}

	pub fn disarm_by_key(
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		kind: TimerKind,
		key: &EncodedKey,
	) -> Result<()> {
		reifydb_assertions! {
			assert!(
				kind.is_maintenance(),
				"a non-maintenance kind holds no timer index, so a disarm addressed by key alone cannot name \
				 which of its instants to cancel and would leave every one of them armed \
				 (operator={}, kind={})",
				operator.0,
				kind as u8
			);
		}
		let index = index_key(kind, key);
		let Some(due) = armed_at(operator, txn, &index)? else {
			return Ok(());
		};
		txn.state_remove(operator, &timer_key(due, kind, key))?;
		txn.state_remove(operator, &index)?;
		Ok(())
	}

	pub fn next_due_stored(operator: OperatorId, store: &OperatorStore) -> Option<TimerDue> {
		let mut wheel = keyspace_inner_range(GroupId::ROOT, KeyspaceId::TIMER_WHEEL);
		loop {
			let batch = store.range_batch(operator, wheel.clone(), 1);
			if let Some((key, _)) = batch.items.first() {
				let (_, _, suffix) = OperatorStateKey::decode_inner(key.as_slice())?;
				return Some(TimerDue {
					operator_id: operator,
					due: TimerWheelKey::from_suffix_bytes(suffix)?.due.0,
				});
			}
			wheel = EncodedKeyRange::new(Bound::Excluded(batch.resume?), wheel.end);
		}
	}

	pub fn take_due(
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		watermark: DateTime,
		limit: usize,
	) -> Result<(Vec<Timer>, Option<DateTime>)> {
		if limit == 0 {
			return Ok((Vec::new(), None));
		}
		let take = limit.min(MAX_TIMERS_PER_SCAN);
		let wheel = keyspace_inner_range(GroupId::ROOT, KeyspaceId::TIMER_WHEEL);
		let batch = txn.state_range(
			operator,
			StateRange::forward(wheel, "timer::take_due").limit(take.saturating_add(1)),
		)?;
		let mut due = Vec::new();
		let mut next = None;
		for item in &batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let suffix = TimerWheelKey::from_suffix_bytes(&decoded.suffix)
				.expect("state_range must return timer wheel suffixes");
			let timer = Timer {
				due: suffix.due.0,
				kind: suffix.kind.0,
				key: armed_key(&EncodedPodRow::from(item.bytes.clone()))?,
			};
			if timer.due > watermark || due.len() == take {
				next = Some(timer.due);
				break;
			}
			due.push(timer);
		}

		let maintenance_indices: Vec<GroupStateKey> = due
			.iter()
			.filter(|timer| timer.kind.is_maintenance())
			.map(|timer| index_key(timer.kind, &timer.key))
			.collect();
		let mut armed: HashMap<EncodedKey, DateTime> = HashMap::with_capacity(maintenance_indices.len());
		if !maintenance_indices.is_empty() {
			for row in txn.state_get_many(operator, &maintenance_indices)?.items {
				let payload = decode_payload::<DateTime>(&EncodedPodRow::from(row.bytes))?;
				armed.insert(row.key, payload);
			}
		}

		for timer in &due {
			txn.state_remove(operator, &timer_key(timer.due, timer.kind, &timer.key))?;
			if timer.kind.is_maintenance() {
				let index = index_key(timer.kind, &timer.key);
				if armed.get(&scoped_key(operator, &index)) == Some(&timer.due) {
					txn.state_remove(operator, &index)?;
				}
			}
		}

		Ok((due, next))
	}
}

fn wheel_suffix(due: DateTime, kind: TimerKind, key: &EncodedKey) -> TimerWheelKey {
	TimerWheelKey {
		due: Asc(due),
		kind: Asc(kind),
		id: Asc(timer_id(key.as_slice())),
	}
}

fn timer_key(due: DateTime, kind: TimerKind, key: &EncodedKey) -> GroupStateKey {
	typed_key::<TimerWheelSpace>(GroupId::ROOT, &wheel_suffix(due, kind, key))
}

fn index_key(kind: TimerKind, key: &EncodedKey) -> GroupStateKey {
	typed_key::<TimerIndexSpace>(
		GroupId::ROOT,
		&TimerIndexKey {
			kind: Asc(kind),
			id: Asc(timer_id(key.as_slice())),
		},
	)
}

fn armed_key(row: &EncodedPodRow) -> Result<EncodedKey> {
	Ok(EncodedKey::new(&decode_payload::<Vec<u8>>(row)?))
}

fn armed_at(operator: OperatorId, txn: &mut impl FlowTransaction, index: &GroupStateKey) -> Result<Option<DateTime>> {
	match txn.state_get(operator, index)? {
		Some(row) => Ok(Some(decode_payload::<DateTime>(&row)?)),
		None => Ok(None),
	}
}
