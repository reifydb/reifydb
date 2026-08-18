// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::{
	key::{decode_datetime_asc, encode_datetime_asc, encoded::EncodedKey},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey, keyspace_inner_range},
	},
	state::store::TimerKind,
};
use reifydb_store_operator::store::OperatorStore;
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};

use crate::{
	timer::{Timer, TimerDue},
	transaction::{
		FlowTransaction,
		group::{decode_payload, encode_payload},
		scope::scoped_key,
		state::StateExtension,
	},
};

#[derive(Clone, Default)]
pub struct TimerWheel;

impl TimerWheel {
	pub fn arm(&self, operator: OperatorId, txn: &mut impl FlowTransaction, timer: &Timer) -> Result<()> {
		if !timer.kind.is_unique() {
			txn.state_set(operator, &timer_key(timer.due, timer.kind, &timer.key), encode_payload(&1u64)?)?;
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
		txn.state_set(operator, &timer_key(timer.due, timer.kind, &timer.key), encode_payload(&1u64)?)?;
		txn.state_set(operator, &index, encode_payload(&timer.due)?)?;
		Ok(())
	}

	pub fn disarm(&self, operator: OperatorId, txn: &mut impl FlowTransaction, timer: &Timer) -> Result<()> {
		if timer.kind.is_unique() {
			let index = index_key(timer.kind, &timer.key);
			if armed_at(operator, txn, &index)? == Some(timer.due) {
				txn.state_remove(operator, &index)?;
			}
		}
		txn.state_remove(operator, &timer_key(timer.due, timer.kind, &timer.key))?;
		Ok(())
	}

	pub fn disarm_by_key(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		kind: TimerKind,
		key: &EncodedKey,
	) -> Result<()> {
		reifydb_assertions! {
			assert!(
				kind.is_unique(),
				"a non-unique kind holds no timer index, so a disarm addressed by key alone cannot name \
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

	pub fn next_due_stored(&self, operator: OperatorId, store: &OperatorStore) -> Option<TimerDue> {
		let wheel = keyspace_inner_range(GroupId::ROOT, Keyspace::TIMER_WHEEL);
		let batch = store.range_batch(operator, wheel, 1);
		let (key, _) = batch.items.first()?;
		let (_, _, suffix) = OperatorStateKey::decode_inner(key.as_slice())?;
		Some(TimerDue {
			operator_id: operator,
			due: decode_timer(&suffix).due,
		})
	}

	pub fn take_due(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		watermark: DateTime,
		limit: usize,
	) -> Result<(Vec<Timer>, Option<DateTime>)> {
		if limit == 0 {
			return Ok((Vec::new(), None));
		}
		let wheel = keyspace_inner_range(GroupId::ROOT, Keyspace::TIMER_WHEEL);
		let batch = txn.state_range(operator, wheel, Some(limit.saturating_add(1)), "timer::take_due")?;
		let mut due = Vec::new();
		let mut next = None;
		for item in &batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let timer = decode_timer(&decoded.suffix);
			if timer.due > watermark || due.len() == limit {
				next = Some(timer.due);
				break;
			}
			due.push(timer);
		}

		let unique_indices: Vec<GroupStateKey> = due
			.iter()
			.filter(|timer| timer.kind.is_unique())
			.map(|timer| index_key(timer.kind, &timer.key))
			.collect();
		let mut armed: HashMap<EncodedKey, DateTime> = HashMap::with_capacity(unique_indices.len());
		if !unique_indices.is_empty() {
			for row in txn.state_get_many(operator, &unique_indices)?.items {
				let payload = decode_payload::<DateTime>(&EncodedPodRow::from(row.bytes))?;
				armed.insert(row.key, payload);
			}
		}

		for timer in &due {
			txn.state_remove(operator, &timer_key(timer.due, timer.kind, &timer.key))?;
			if timer.kind.is_unique() {
				let index = index_key(timer.kind, &timer.key);
				if armed.get(&scoped_key(operator, &index)) == Some(&timer.due) {
					txn.state_remove(operator, &index)?;
				}
			}
		}

		Ok((due, next))
	}
}

fn timer_suffix(due: DateTime, kind: TimerKind, key: &EncodedKey) -> Vec<u8> {
	let mut suffix = Vec::with_capacity(9 + key.len());
	suffix.extend_from_slice(&encode_datetime_asc(due));
	suffix.push(kind as u8);
	suffix.extend_from_slice(key.as_ref());
	suffix
}

fn timer_key(due: DateTime, kind: TimerKind, key: &EncodedKey) -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::TIMER_WHEEL, timer_suffix(due, kind, key))
}

fn index_key(kind: TimerKind, key: &EncodedKey) -> GroupStateKey {
	let mut suffix = Vec::with_capacity(1 + key.len());
	suffix.push(kind as u8);
	suffix.extend_from_slice(key.as_ref());
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::TIMER_INDEX, suffix)
}

fn armed_at(operator: OperatorId, txn: &mut impl FlowTransaction, index: &GroupStateKey) -> Result<Option<DateTime>> {
	match txn.state_get(operator, index)? {
		Some(row) => Ok(Some(decode_payload::<DateTime>(&row)?)),
		None => Ok(None),
	}
}

fn decode_timer(suffix: &[u8]) -> Timer {
	assert!(suffix.len() >= 9, "a timer wheel suffix must carry at least the instant and the kind");
	let due = decode_datetime_asc(suffix[..8].try_into().expect("eight instant bytes"));
	let kind = TimerKind::from_u8(suffix[8]).expect("a timer wheel suffix must carry a known kind");
	Timer {
		due,
		kind,
		key: EncodedKey::new(&suffix[9..]),
	}
}
