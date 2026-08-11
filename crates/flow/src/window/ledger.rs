// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::operator::{OperatorState, decode};
#[cfg(feature = "runtime")]
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_core::{
	key::operator_state::{GroupId, GroupStateKey, Keyspace, OperatorStateKey},
	metrics::heap::HeapSize,
	state::store::StateStore,
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, value::datetime::DateTime};

#[cfg(feature = "runtime")]
use crate::transaction::FlowTransaction;
use crate::{
	timer::Timer,
	window::{policy::SealedThrough, span::WindowCoord},
};

#[operator_state]
#[derive(Clone, Default)]
pub struct SealLedgerState {
	pub sealed_through: u64,
}

impl HeapSize for SealLedgerState {
	fn heap_size(&self) -> usize {
		0
	}
}

pub fn seal_ledger_key() -> GroupStateKey {
	OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::SEAL_LEDGER, vec![])
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FiredAt(DateTime);

impl FiredAt {
	pub fn of(timer: &Timer) -> Self {
		Self(timer.at)
	}

	pub fn at(self) -> DateTime {
		self.0
	}
}

pub struct SealLedger;

impl SealLedger {
	pub fn read(store: &mut impl StateStore) -> Result<Option<SealedThrough>> {
		Ok(Self::read_order(store)?.map(SealedThrough::from_order))
	}

	pub fn advance(store: &mut impl StateStore, fired: FiredAt) -> Result<SealedThrough> {
		let fired_order = fired.at().to_order();
		let current = Self::read_order(store)?.unwrap_or(0);
		if fired_order <= current {
			return Ok(SealedThrough::from_order(current));
		}
		let state = SealLedgerState {
			sealed_through: fired_order,
		};
		let now = store.written_at();
		store.state_set(&seal_ledger_key(), state.encode_state(now)?)?;
		Ok(SealedThrough::from_order(fired_order))
	}

	pub fn read_order(store: &mut impl StateStore) -> Result<Option<u64>> {
		let Some(bytes) = store.state_get(&seal_ledger_key())? else {
			return Ok(None);
		};
		let state: SealLedgerState = decode(&bytes)?;
		Ok(Some(state.sealed_through))
	}
}

#[cfg(feature = "runtime")]
pub fn read_sealed_through<T: FlowTransaction>(txn: &mut T, operator: OperatorId) -> Result<Option<SealedThrough>> {
	let Some(row) = txn.state_get(operator, &seal_ledger_key())? else {
		return Ok(None);
	};
	let state: SealLedgerState = decode(&row)?;
	Ok(Some(SealedThrough::from_order(state.sealed_through)))
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::state::store::TimerKind;

	use super::*;
	use crate::window::engine::test_support::MockStore;

	fn timer(millis: u64) -> Timer {
		Timer {
			at: DateTime::from_millis(millis),
			kind: TimerKind::Seal,
			key: EncodedKey::new(b"bucket".as_slice()),
		}
	}

	#[test]
	fn a_ledger_can_only_be_advanced_from_a_fired_timer() {
		// `advance` takes a FiredAt, whose only constructor takes a &Timer, so sealing on arriving
		// data is unrepresentable rather than merely avoided.
		let mut store = MockStore::default();

		let sealed = SealLedger::advance(&mut store, FiredAt::of(&timer(5_000))).unwrap();

		assert_eq!(sealed.at(), DateTime::from_millis(5_000));
	}

	#[test]
	fn two_timers_at_one_instant_seal_identically_whatever_key_they_carry() {
		// Seal entry points take a FiredAt, never a &Timer, so the timer's key cannot reach the
		// sealing decision. Both chaos and rolling arm seals with an empty key, so a sweep that
		// consulted timer.key would seal nothing for either.
		let keyed = Timer {
			at: DateTime::from_millis(5_000),
			kind: TimerKind::Seal,
			key: EncodedKey::new(b"some-window".as_slice()),
		};
		let keyless = Timer {
			at: DateTime::from_millis(5_000),
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
		// none and "sealed through 1970" are different answers to reclaim: none means the operator has
		// no seal clamp, the epoch would mean everything is sealed and let reclaim erase live state.
		let mut store = MockStore::default();

		assert!(SealLedger::read(&mut store).unwrap().is_none());
	}

	#[test]
	fn the_ledger_only_moves_forward() {
		// Timers fire in (at, kind, key) order within a round, but a restart re-reads a cold wheel
		// and a late round can present an earlier instant. Rewinding the ledger would unclamp
		// reclaim and expose already-sealed windows to a second seal.
		let mut store = MockStore::default();

		SealLedger::advance(&mut store, FiredAt::of(&timer(9_000))).unwrap();
		let after_earlier = SealLedger::advance(&mut store, FiredAt::of(&timer(3_000))).unwrap();

		assert_eq!(after_earlier.at(), DateTime::from_millis(9_000));
		assert_eq!(SealLedger::read(&mut store).unwrap().unwrap().at(), DateTime::from_millis(9_000));
	}

	#[test]
	fn the_ledger_lives_in_the_root_group_and_carries_no_suffix() {
		// the key must be derivable from the operator alone, since reclaim reads it without knowing which group
		// wrote it
		let key = seal_ledger_key();
		let (group, keyspace, suffix) =
			OperatorStateKey::decode_inner(key.as_encoded().as_bytes()).expect("structured key");

		assert_eq!(group, GroupId::ROOT);
		assert_eq!(keyspace, Keyspace::SEAL_LEDGER);
		assert!(suffix.is_empty());
	}
}
