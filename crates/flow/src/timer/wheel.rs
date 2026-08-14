//  SPDX-License-Identifier: AGPL-3.0-or-later
//  Copyright (c) 2025 ReifyDB

use std::collections::Bound;

use reifydb_codec::key::{
	decode_u64_asc, encode_u64_asc,
	encoded::{EncodedKey, EncodedKeyRange},
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
use reifydb_value::{reifydb_assertions, value::datetime::DateTime};

use crate::{
	timer::{Timer, TimerDue},
	transaction::{
		FlowTransaction,
		group::{decode_payload, encode_payload},
		state::StateExtension,
	},
};

#[derive(Clone, Default)]
pub struct TimerWheel;

impl TimerWheel {
	pub fn arm(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		timer: &Timer,
	) -> reifydb_value::Result<()> {
		let now = txn.written_at();
		let at = timer.due.to_millis();
		if !timer.kind.is_unique() {
			txn.state_set(
				operator,
				&timer_key(timer.due, timer.kind, &timer.key),
				encode_payload(&1u64, now)?,
			)?;
			return Ok(());
		}
		let index = index_key(timer.kind, &timer.key);
		let previous = armed_at(operator, txn, &index)?;
		if previous == Some(at) {
			return Ok(());
		}
		if let Some(previous) = previous {
			let stale = timer_key(DateTime::from_millis(previous), timer.kind, &timer.key);
			reifydb_assertions! {
				assert!(
					txn.state_get(operator, &stale)?.is_some(),
					"the timer index names an instant the wheel does not hold, so the two have \
					 diverged and the stale wheel row can never be cancelled again; every arm would \
					 then leave a permanent entry and the due probe degrades with it \
					 (operator={}, previous={}, requested={})",
					operator.0,
					previous,
					at
				);
			}
			txn.state_remove(operator, &stale)?;
		}
		txn.state_set(operator, &timer_key(timer.due, timer.kind, &timer.key), encode_payload(&1u64, now)?)?;
		txn.state_set(operator, &index, encode_payload(&at, now)?)?;
		Ok(())
	}

	pub fn disarm(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		timer: &Timer,
	) -> reifydb_value::Result<()> {
		let at = timer.due.to_millis();
		if timer.kind.is_unique() {
			let index = index_key(timer.kind, &timer.key);
			if armed_at(operator, txn, &index)? == Some(at) {
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
	) -> reifydb_value::Result<()> {
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
		let Some(at) = armed_at(operator, txn, &index)? else {
			return Ok(());
		};
		txn.state_remove(operator, &timer_key(DateTime::from_millis(at), kind, key))?;
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

	pub fn next_due(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
	) -> reifydb_value::Result<Option<TimerDue>> {
		let wheel = keyspace_inner_range(GroupId::ROOT, Keyspace::TIMER_WHEEL);
		let batch = txn.state_range(operator, wheel, Some(1), "timer::next_due")?;
		let Some(item) = batch.items.first() else {
			return Ok(None);
		};
		let decoded = OperatorStateKey::decode(&item.key).expect("state_range must return OperatorState keys");
		Ok(Some(TimerDue {
			operator_id: operator,
			due: decode_timer(&decoded.suffix).due,
		}))
	}

	pub fn take_due(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		watermark: DateTime,
		limit: usize,
	) -> reifydb_value::Result<Vec<Timer>> {
		if limit == 0 {
			return Ok(Vec::new());
		}
		let ceiling = watermark.to_millis();
		let batch = txn.state_range(operator, due_range(ceiling), Some(limit), "timer::take_due")?;
		let mut due = Vec::new();
		for item in &batch.items {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let timer = decode_timer(&decoded.suffix);
			if timer.due.to_millis() > ceiling {
				break;
			}
			due.push(timer);
		}

		for timer in &due {
			txn.state_remove(operator, &timer_key(timer.due, timer.kind, &timer.key))?;
			if timer.kind.is_unique() {
				let index = index_key(timer.kind, &timer.key);
				if armed_at(operator, txn, &index)? == Some(timer.due.to_millis()) {
					txn.state_remove(operator, &index)?;
				}
			}
		}

		Ok(due)
	}
}

fn timer_suffix(at: DateTime, kind: TimerKind, key: &EncodedKey) -> Vec<u8> {
	let mut suffix = Vec::with_capacity(9 + key.len());
	suffix.extend_from_slice(&encode_u64_asc(at.to_millis()));
	suffix.push(kind as u8);
	suffix.extend_from_slice(key.as_ref());
	suffix
}

fn timer_key(at: DateTime, kind: TimerKind, key: &EncodedKey) -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::TIMER_WHEEL, timer_suffix(at, kind, key))
}

fn due_range(ceiling: u64) -> EncodedKeyRange {
	let wheel = keyspace_inner_range(GroupId::ROOT, Keyspace::TIMER_WHEEL);
	let Some(exclusive) = ceiling.checked_add(1) else {
		return wheel;
	};
	let end = OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::TIMER_WHEEL, encode_u64_asc(exclusive));
	EncodedKeyRange::new(wheel.start, Bound::Excluded(end.into_encoded()))
}

fn index_key(kind: TimerKind, key: &EncodedKey) -> GroupStateKey {
	let mut suffix = Vec::with_capacity(1 + key.len());
	suffix.push(kind as u8);
	suffix.extend_from_slice(key.as_ref());
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::TIMER_INDEX, suffix)
}

fn armed_at(
	operator: OperatorId,
	txn: &mut impl FlowTransaction,
	index: &GroupStateKey,
) -> reifydb_value::Result<Option<u64>> {
	match txn.state_get(operator, index)? {
		Some(row) => Ok(Some(decode_payload::<u64>(&row)?)),
		None => Ok(None),
	}
}

fn decode_timer(suffix: &[u8]) -> Timer {
	assert!(suffix.len() >= 9, "a timer wheel suffix must carry at least the instant and the kind");
	let at = decode_u64_asc(suffix[..8].try_into().expect("eight instant bytes"));
	let kind = TimerKind::from_u8(suffix[8]).expect("a timer wheel suffix must carry a known kind");
	Timer {
		due: DateTime::from_millis(at),
		kind,
		key: EncodedKey::new(&suffix[9..]),
	}
}
