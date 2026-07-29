// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::state::{OperatorState, decode_state};
use reifydb_core::{
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, StateKey},
	metrics::heap::HeapSize,
	state::store::StateStore,
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, value::datetime::DateTime};

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

pub fn seal_ledger_key() -> StateKey {
	OperatorStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::SEAL_LEDGER, vec![])
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
		let now = store.clock_now();
		store.state_set(&seal_ledger_key(), state.encode_state(now)?)?;
		Ok(SealedThrough::from_order(fired_order))
	}

	fn read_order(store: &mut impl StateStore) -> Result<Option<u64>> {
		let Some(bytes) = store.state_get(&seal_ledger_key())? else {
			return Ok(None);
		};
		let state: SealLedgerState = decode_state(&bytes)?;
		Ok(Some(state.sealed_through))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_abi::operator::timer::TimerKind;
	use reifydb_codec::key::encoded::EncodedKey;

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
		// Intent: THE structural fix for D-a and D-b. `advance` takes a FiredAt, and
		// FiredAt::of is the only constructor and takes a &Timer. Arriving data carries no
		// Timer, so there is no expression a guest operator can write that seals on
		// arrival - the guest's advance_seal_watermark becomes unrepresentable rather than
		// merely deleted.
		// Mutation: add `FiredAt::at_instant(DateTime)` and the guarantee is gone; the
		// absence of that constructor is the thing under test.
		let mut store = MockStore::default();

		let sealed = SealLedger::advance(&mut store, FiredAt::of(&timer(5_000))).unwrap();

		assert_eq!(sealed.at(), DateTime::from_millis(5_000));
	}

	#[test]
	fn an_empty_ledger_reads_as_none_rather_than_the_epoch() {
		// Intent: none and "sealed through 1970" are different answers and reclaim treats
		// them differently - none means "this node has no seal clamp", the epoch would
		// mean "everything is sealed", which would let reclaim erase live state.
		// Mutation: return Some(SealedThrough::from_order(0)) on a missing key and a
		// never-fired window's state becomes reclaimable on the first tick.
		let mut store = MockStore::default();

		assert!(SealLedger::read(&mut store).unwrap().is_none());
	}

	#[test]
	fn the_ledger_only_moves_forward() {
		// Intent: timers fire in (at, kind, key) order within one round, but a restart
		// re-reads a cold wheel and a late round can present an EARLIER instant. Letting
		// that rewind the ledger would unclamp reclaim and expose already-sealed windows
		// to a second seal.
		// Mutation: drop the `fired_order <= current` guard and the second advance rewinds
		// the ledger to 3_000.
		let mut store = MockStore::default();

		SealLedger::advance(&mut store, FiredAt::of(&timer(9_000))).unwrap();
		let after_earlier = SealLedger::advance(&mut store, FiredAt::of(&timer(3_000))).unwrap();

		assert_eq!(after_earlier.at(), DateTime::from_millis(9_000));
		assert_eq!(SealLedger::read(&mut store).unwrap().unwrap().at(), DateTime::from_millis(9_000));
	}

	#[test]
	fn the_ledger_is_node_scoped_and_carries_no_group_or_suffix() {
		// Intent: reclaim reads this key without knowing which operator wrote it, so the
		// key must be derivable from the node alone. A group-scoped or suffixed key would
		// make the read impossible without asking the operator, which is exactly the
		// vtable dependency P8 deletes.
		let key = seal_ledger_key();
		let (group, keyspace, suffix) =
			OperatorStateKey::decode_inner(key.as_encoded().as_bytes()).expect("structured key");

		assert_eq!(group, GroupId::NODE_SCOPE);
		assert_eq!(keyspace, Keyspace::SEAL_LEDGER);
		assert!(suffix.is_empty());
	}
}
