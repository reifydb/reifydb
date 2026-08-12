// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use dashmap::DashMap;
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
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};

use crate::{
	timer::Timer,
	transaction::{
		FlowTransaction,
		group::{decode_payload, encode_payload},
	},
};

const TAKE_CHUNK: usize = 1_024;

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

fn index_key(kind: TimerKind, key: &EncodedKey) -> GroupStateKey {
	let mut suffix = Vec::with_capacity(1 + key.len());
	suffix.push(kind as u8);
	suffix.extend_from_slice(key.as_ref());
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::TIMER_INDEX, suffix)
}

fn armed_at(operator: OperatorId, txn: &mut impl FlowTransaction, index: &GroupStateKey) -> Result<Option<u64>> {
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
		at: DateTime::from_millis(at),
		kind,
		key: EncodedKey::new(&suffix[9..]),
	}
}

#[derive(Default)]
pub struct WheelState {
	pub hydrated: bool,
	pub earliest: Option<u64>,
}

impl WheelState {
	fn invalidate_if_earliest(&mut self, at: u64) {
		if self.earliest.is_some_and(|earliest| at <= earliest) {
			self.hydrated = false;
			self.earliest = None;
		}
	}
}

#[derive(Clone, Default)]
pub struct TimerWheel {
	pub inner: Arc<DashMap<OperatorId, WheelState>>,
}

impl TimerWheel {
	pub fn arm(&self, operator: OperatorId, txn: &mut impl FlowTransaction, timer: &Timer) -> Result<()> {
		let now = txn.written_at();
		let at = timer.at.to_millis();
		if !timer.kind.is_unique() {
			let mut state = self.inner.entry(operator).or_default();
			if state.hydrated {
				state.earliest = Some(state.earliest.map_or(at, |earliest| earliest.min(at)));
			}
			drop(state);
			txn.state_set(
				operator,
				&timer_key(timer.at, timer.kind, &timer.key),
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
		{
			let mut state = self.inner.entry(operator).or_default();
			if let Some(previous) = previous {
				state.invalidate_if_earliest(previous);
			}
			if state.hydrated {
				state.earliest = Some(state.earliest.map_or(at, |earliest| earliest.min(at)));
			}
		}
		txn.state_set(operator, &timer_key(timer.at, timer.kind, &timer.key), encode_payload(&1u64, now)?)?;
		txn.state_set(operator, &index, encode_payload(&at, now)?)?;
		Ok(())
	}

	pub fn disarm(&self, operator: OperatorId, txn: &mut impl FlowTransaction, timer: &Timer) -> Result<()> {
		let at = timer.at.to_millis();
		if timer.kind.is_unique() {
			let index = index_key(timer.kind, &timer.key);
			if armed_at(operator, txn, &index)? == Some(at) {
				txn.state_remove(operator, &index)?;
			}
		}
		self.inner.entry(operator).or_default().invalidate_if_earliest(at);
		txn.state_remove(operator, &timer_key(timer.at, timer.kind, &timer.key))?;
		Ok(())
	}

	pub fn take_due(
		&self,
		operator: OperatorId,
		txn: &mut impl FlowTransaction,
		watermark: DateTime,
		limit: usize,
	) -> Result<Vec<Timer>> {
		if limit == 0 {
			return Ok(Vec::new());
		}
		{
			let mut state = self.inner.entry(operator).or_default();
			Self::hydrate_once(&mut state, operator, txn)?;
			match state.earliest {
				None => return Ok(Vec::new()),
				Some(earliest) if earliest > watermark.to_millis() => return Ok(Vec::new()),
				Some(_) => {}
			}
			state.hydrated = false;
		}

		let base = keyspace_inner_range(GroupId::ROOT, Keyspace::TIMER_WHEEL);
		let ceiling = watermark.to_millis();
		let mut due = Vec::new();
		let mut next_earliest = None;
		let mut start = base.start.clone();
		'scan: loop {
			let want = (limit - due.len()).min(TAKE_CHUNK);
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.state_range(operator, range, Some(want + 1), "timer::take_due")?;
			let mut last_inner: Option<EncodedKey> = None;
			for item in &batch.items {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_range must return OperatorState keys");
				let timer = decode_timer(&decoded.suffix);
				if timer.at.to_millis() > ceiling || due.len() == limit {
					next_earliest = Some(timer.at.to_millis());
					break 'scan;
				}
				due.push(timer);
				last_inner = Some(decoded.inner());
			}
			if !batch.has_more {
				break;
			}
			let Some(last) = last_inner else {
				break;
			};
			start = Bound::Excluded(last);
		}

		for timer in &due {
			txn.state_remove(operator, &timer_key(timer.at, timer.kind, &timer.key))?;
			if timer.kind.is_unique() {
				let index = index_key(timer.kind, &timer.key);
				if armed_at(operator, txn, &index)? == Some(timer.at.to_millis()) {
					txn.state_remove(operator, &index)?;
				}
			}
		}

		let mut state = self.inner.entry(operator).or_default();
		state.hydrated = true;
		state.earliest = next_earliest;
		Ok(due)
	}

	fn hydrate_once(state: &mut WheelState, operator: OperatorId, txn: &mut impl FlowTransaction) -> Result<()> {
		if state.hydrated {
			return Ok(());
		}
		state.hydrated = true;
		let range = keyspace_inner_range(GroupId::ROOT, Keyspace::TIMER_WHEEL);
		let batch = txn.state_range(operator, range, Some(1), "timer::hydrate_probe")?;
		state.earliest = batch.items.first().map(|item| {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			decode_timer(&decoded.suffix).at.to_millis()
		});
		Ok(())
	}
}

