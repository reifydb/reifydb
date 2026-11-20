// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::slice::from_ref;

use reifydb_codec::row::operator::state::{OperatorState, decode};
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId, OperatorStateKey},
	metrics::heap::HeapSize,
	state::timer::StateStore,
};
use reifydb_macro::operator_state;
use reifydb_value::{error::ValueError, value::datetime::DateTime};

use crate::{operator::state::seal::rule::SealedThrough, timer::Timer};

#[operator_state]
#[derive(Clone, Default)]
pub struct SealLedgerState {
	pub sealed_through: u64,
	pub forget_pass_armed: Option<u64>,
}

impl HeapSize for SealLedgerState {
	fn heap_size(&self) -> usize {
		0
	}
}

pub fn seal_ledger_key() -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::SEAL_LEDGER, vec![])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FiredAt(DateTime);

impl FiredAt {
	pub fn of(timer: &Timer) -> Self {
		Self(timer.due)
	}

	pub fn at(self) -> DateTime {
		self.0
	}
}

pub struct SealLedger;

impl SealLedger {
	pub fn advance(store: &mut dyn StateStore, fired: FiredAt) -> Result<SealedThrough, ValueError> {
		Self::advance_state(store, fired).map(|state| SealedThrough::from_order(state.sealed_through))
	}

	pub fn advance_state(store: &mut dyn StateStore, fired: FiredAt) -> Result<SealLedgerState, ValueError> {
		let fired_order = fired.at().to_order();
		let mut state = Self::read(store)?.unwrap_or_default();
		if fired_order <= state.sealed_through {
			return Ok(state);
		}
		state.sealed_through = fired_order;
		Self::write(store, &state)?;
		Ok(state)
	}

	pub fn observe(store: &mut dyn StateStore, order: u64) -> Result<u64, ValueError> {
		let mut state = Self::read(store)?.unwrap_or_default();
		if order <= state.sealed_through {
			return Ok(state.sealed_through);
		}
		state.sealed_through = order;
		Self::write(store, &state)?;
		Ok(order)
	}

	pub fn read_order(store: &mut dyn StateStore) -> Result<Option<u64>, ValueError> {
		Ok(Self::read(store)?.map(|state| state.sealed_through))
	}

	pub fn record_forget_pass(
		store: &mut dyn StateStore,
		mut state: SealLedgerState,
		due: DateTime,
	) -> Result<(), ValueError> {
		state.forget_pass_armed = Some(due.to_order());
		Self::write(store, &state)
	}

	pub fn set_forget_pass_armed(store: &mut dyn StateStore, due: Option<DateTime>) -> Result<(), ValueError> {
		let mut state = Self::read(store)?.unwrap_or_default();
		let armed = due.map(|due| due.to_order());
		if state.forget_pass_armed == armed {
			return Ok(());
		}
		state.forget_pass_armed = armed;
		Self::write(store, &state)
	}

	fn read(store: &mut dyn StateStore) -> Result<Option<SealLedgerState>, ValueError> {
		let Some(bytes) = store.state_get(from_ref(&seal_ledger_key()))?.pop().flatten() else {
			return Ok(None);
		};
		Ok(Some(decode(&bytes)?))
	}

	fn write(store: &mut dyn StateStore, state: &SealLedgerState) -> Result<(), ValueError> {
		store.state_set(vec![(seal_ledger_key(), state.encode_state()?)])
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::state::timer::TimerKind;

	use super::*;
	use crate::operator::state::mock::MockStore;

	fn timer(millis: i64) -> Timer {
		Timer {
			due: DateTime::from_millis(millis),
			kind: TimerKind::Seal,
			key: EncodedKey::new(b"bucket".as_slice()),
		}
	}

	#[test]
	fn the_timer_seal_path_never_opens_to_arriving_data() {
		// FiredAt's only constructor takes a &Timer, so this path never seals on arriving data; observe may.
		let mut store = MockStore::default();

		let sealed = SealLedger::advance(&mut store, FiredAt::of(&timer(5_000))).unwrap();

		assert_eq!(sealed, SealedThrough::from_order(DateTime::from_millis(5_000).to_order()));
	}

	#[test]
	fn two_timers_at_one_instant_seal_identically_whatever_key_they_carry() {
		// Seal entry points take a FiredAt, never a &Timer, so the timer's key cannot reach the
		// sealing decision. Both chaos and rolling arm seals with an empty key, so a sweep that
		// consulted timer.key would seal nothing for either.
		let keyed = Timer {
			due: DateTime::from_millis(5_000),
			kind: TimerKind::Seal,
			key: EncodedKey::new(b"some-window".as_slice()),
		};
		let keyless = Timer {
			due: DateTime::from_millis(5_000),
			kind: TimerKind::Seal,
			key: EncodedKey::new(Vec::new()),
		};

		assert_eq!(FiredAt::of(&keyed), FiredAt::of(&keyless));

		let mut keyed_store = MockStore::default();
		let mut keyless_store = MockStore::default();
		assert_eq!(
			SealLedger::advance(&mut keyed_store, FiredAt::of(&keyed)).unwrap(),
			SealLedger::advance(&mut keyless_store, FiredAt::of(&keyless)).unwrap()
		);
	}

	#[test]
	fn an_empty_ledger_reads_as_none_rather_than_the_epoch() {
		// none and "sealed through 1970" are different answers to the forget pass: none means the operator has
		// no seal clamp, the epoch would mean everything is sealed and let the forget pass erase live state.
		let mut store = MockStore::default();

		assert!(SealLedger::read_order(&mut store).unwrap().is_none());
	}

	#[test]
	fn the_ledger_only_moves_forward() {
		// Timers fire in (at, kind, key) order within a round, but a restart re-reads a cold wheel
		// and a late round can present an earlier instant. Rewinding the ledger would unclamp
		// the forget pass and expose already-sealed windows to a second seal.
		let mut store = MockStore::default();

		SealLedger::advance(&mut store, FiredAt::of(&timer(9_000))).unwrap();
		let after_earlier = SealLedger::advance(&mut store, FiredAt::of(&timer(3_000))).unwrap();

		assert_eq!(after_earlier, SealedThrough::from_order(DateTime::from_millis(9_000).to_order()));
		assert_eq!(SealLedger::read_order(&mut store).unwrap(), Some(DateTime::from_millis(9_000).to_order()));
	}

	#[test]
	fn the_ledger_lives_in_the_root_group_and_carries_no_suffix() {
		// the key must be derivable from the operator alone, since the forget pass reads it without knowing
		// which group wrote it
		let key = seal_ledger_key();
		let (group, keyspace, suffix) =
			OperatorStateKey::decode_inner(key.as_encoded().as_bytes()).expect("structured key");

		assert_eq!(group, GroupId::ROOT);
		assert_eq!(keyspace, KeyspaceId::SEAL_LEDGER);
		assert!(suffix.is_empty());
	}
}
