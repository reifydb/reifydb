// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	metrics::heap::{StateCompleteness, StateMemory},
	state::membership::MembershipIndex,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{count::Count, util::hash::Hash128};

pub(crate) const MEMBERSHIP_BYTE_CAP: u64 = 16 * 1024 * 1024;

pub(crate) fn fold_hash128(hash: &Hash128) -> u64 {
	(hash.0 as u64) ^ ((hash.0 >> 64) as u64)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MembershipAnswer {
	Untracked,
	DefinitelyAbsent,
	MaybePresent,
}

struct MembershipState {
	index: Option<MembershipIndex>,
	hydrated: bool,
	absences_served: u64,
	false_positives: u64,
	discards: u64,
}

pub(crate) struct KeyspaceMembership {
	byte_cap: u64,
	state: Mutex<MembershipState>,
}

impl KeyspaceMembership {
	pub(crate) fn new(byte_cap: u64) -> Self {
		Self {
			byte_cap,
			state: Mutex::new(MembershipState {
				index: None,
				hydrated: false,
				absences_served: 0,
				false_positives: 0,
				discards: 0,
			}),
		}
	}

	pub(crate) fn is_hydrated(&self) -> bool {
		self.state.lock().hydrated
	}

	pub(crate) fn install(&self, hashes: &[u64]) {
		let mut state = self.state.lock();
		let mut index = MembershipIndex::with_capacity(hashes.len(), self.byte_cap);
		let tracked = hashes.iter().all(|hash| index.insert(*hash));
		state.index = tracked.then_some(index);
		state.hydrated = true;
	}

	pub(crate) fn probe(&self, hash: u64) -> MembershipAnswer {
		let mut state = self.state.lock();
		match &state.index {
			None => MembershipAnswer::Untracked,
			Some(index) => {
				if index.contains(hash) {
					MembershipAnswer::MaybePresent
				} else {
					state.absences_served += 1;
					MembershipAnswer::DefinitelyAbsent
				}
			}
		}
	}

	pub(crate) fn insert(&self, hash: u64) {
		let mut state = self.state.lock();
		if let Some(index) = state.index.as_mut()
			&& !index.insert(hash)
		{
			state.index = None;
			state.discards += 1;
		}
	}

	pub(crate) fn remove(&self, hash: u64) {
		let mut state = self.state.lock();
		if let Some(index) = state.index.as_mut() {
			index.remove(hash);
		}
	}

	pub(crate) fn record_store_miss(&self) {
		let mut state = self.state.lock();
		if state.index.is_some() {
			state.false_positives += 1;
		}
	}

	pub(crate) fn memory(&self) -> StateMemory {
		let state = self.state.lock();
		state.index.as_ref().map_or(StateMemory::ZERO, MembershipIndex::approximate_memory)
	}

	pub(crate) fn completeness(&self) -> StateCompleteness {
		let state = self.state.lock();
		StateCompleteness {
			values_complete: true,
			membership_complete: state.index.is_some(),
			absences_served: Count::new(state.absences_served),
			false_positives: Count::new(state.false_positives),
			revocations: Count::new(state.discards),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn probes_are_untracked_until_hydration_installs_an_index() {
		// A pre-hydration filter must never answer: an Untracked probe forces the
		// caller to read the store, which is the only safe default before the
		// keyspace has been scanned. DefinitelyAbsent here would be silent state
		// loss for every key written before this process started.
		let membership = KeyspaceMembership::new(MEMBERSHIP_BYTE_CAP);
		assert_eq!(membership.probe(42), MembershipAnswer::Untracked);
		assert!(!membership.is_hydrated());

		membership.install(&[1, 2, 3]);
		assert!(membership.is_hydrated());
		assert_eq!(membership.probe(2), MembershipAnswer::MaybePresent);
		assert_eq!(membership.probe(42), MembershipAnswer::DefinitelyAbsent);
		assert_eq!(membership.completeness().absences_served.as_u64(), 1);
	}

	#[test]
	fn multiset_semantics_keep_presence_until_the_last_instance_is_removed() {
		// Join sides store many rows per key hash: one filter instance per row.
		// Removing one row of two must NOT flip the key to absent - that would
		// make contains_key deny a key that still has matches (wrong join output).
		let membership = KeyspaceMembership::new(MEMBERSHIP_BYTE_CAP);
		membership.install(&[]);
		membership.insert(7);
		membership.insert(7);

		membership.remove(7);
		assert_eq!(membership.probe(7), MembershipAnswer::MaybePresent);

		membership.remove(7);
		assert_eq!(membership.probe(7), MembershipAnswer::DefinitelyAbsent);
	}

	#[test]
	fn safe_overcount_degrades_to_a_false_positive_never_a_false_absence() {
		// Blind inserts (put_row with unknown prior presence) may over-count. The
		// stale instance must surface as a counted false positive on the verify
		// read, never as a false absence: the caller sees MaybePresent, reads the
		// store, finds nothing, and records the miss.
		let membership = KeyspaceMembership::new(MEMBERSHIP_BYTE_CAP);
		membership.install(&[]);
		membership.insert(9);
		membership.insert(9);
		membership.remove(9);

		assert_eq!(membership.probe(9), MembershipAnswer::MaybePresent);
		membership.record_store_miss();
		let completeness = membership.completeness();
		assert_eq!(completeness.false_positives.as_u64(), 1);
		assert!(completeness.membership_complete);
	}

	#[test]
	fn a_hot_key_no_longer_costs_the_keyspace_its_index() {
		// Regression pin for the production discard: every hash-join node in the
		// 2026-07-21 profile showed revocations=1 because one hot join key (one
		// filter instance per stored row) chained the cuckoo filter to its byte
		// cap, and the next insert discarded the whole side into permanent
		// read-through. Hundreds of instances of one hash must leave the index
		// alive, other keys' absence proofs intact, and count no revocation -
		// and the drained hot key must still flip back to an exact RAM absence.
		let membership = KeyspaceMembership::new(MEMBERSHIP_BYTE_CAP);
		membership.install(&[]);
		for _ in 0..500 {
			membership.insert(7);
		}

		assert_eq!(
			membership.probe(42),
			MembershipAnswer::DefinitelyAbsent,
			"an unrelated key must still be provably absent while the hot key is live"
		);
		let completeness = membership.completeness();
		assert!(completeness.membership_complete, "the index must survive the hot key");
		assert_eq!(completeness.revocations.as_u64(), 0);

		for _ in 0..500 {
			membership.remove(7);
		}
		assert_eq!(
			membership.probe(7),
			MembershipAnswer::DefinitelyAbsent,
			"a fully drained hot key must read as an exact RAM absence, not a lingering FP"
		);
	}

	#[test]
	fn exceeding_the_byte_cap_discards_the_index_and_counts_a_revocation() {
		// A filter that cannot grow must degrade to read-through (Untracked), not
		// drop instances: a partial filter would produce false absences. The
		// discard surfaces through the revocations counter so the [memory] log
		// shows why a node lost membership_complete mid-run.
		let membership = KeyspaceMembership::new(64);
		membership.install(&[]);
		for hash in 0..100_000u64 {
			membership.insert(hash);
		}
		assert_eq!(membership.probe(1), MembershipAnswer::Untracked);
		let completeness = membership.completeness();
		assert!(!completeness.membership_complete);
		assert_eq!(completeness.revocations.as_u64(), 1);
	}

	#[test]
	fn install_over_cap_leaves_the_keyspace_untracked() {
		// Hydrating a keyspace larger than the cap must land in the same safe
		// Untracked mode as a mid-run overflow; a truncated install would deny
		// every key the scan could not fit.
		let membership = KeyspaceMembership::new(64);
		let hashes: Vec<u64> = (0..100_000).collect();
		membership.install(&hashes);
		assert!(membership.is_hydrated(), "a failed install must not trigger endless rescans");
		assert_eq!(membership.probe(5), MembershipAnswer::Untracked);
	}

	#[test]
	fn record_store_miss_without_an_index_counts_nothing() {
		// Untracked mode reads through by design; those misses are not filter
		// false positives and counting them would drown the real FP signal.
		let membership = KeyspaceMembership::new(MEMBERSHIP_BYTE_CAP);
		membership.record_store_miss();
		assert_eq!(membership.completeness().false_positives.as_u64(), 0);
	}

	#[test]
	fn fold_hash128_mixes_both_halves() {
		let low = Hash128(0xAAAA_BBBB_CCCC_DDDDu128);
		let high = Hash128(0xAAAA_BBBB_CCCC_DDDDu128 << 64);
		assert_eq!(fold_hash128(&low), fold_hash128(&high), "xor fold must be symmetric in the halves");
		assert_ne!(fold_hash128(&Hash128(1)), fold_hash128(&Hash128(2)));
	}
}
