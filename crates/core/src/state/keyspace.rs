// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{count::Count, util::hash::Hash128};

use super::membership::{MembershipAnswer, MembershipTracker};
use crate::metrics::heap::{StateCompleteness, StateMemory};

pub fn fold_hash128(hash: &Hash128) -> u64 {
	(hash.0 as u64) ^ ((hash.0 >> 64) as u64)
}

struct MembershipState {
	tracker: MembershipTracker,
	hydrated: bool,
	discards: u64,
}

pub struct KeyspaceMembership {
	state: Mutex<MembershipState>,
}

impl KeyspaceMembership {
	pub fn new(byte_cap: u64) -> Self {
		Self {
			state: Mutex::new(MembershipState {
				tracker: MembershipTracker::new(byte_cap),
				hydrated: false,
				discards: 0,
			}),
		}
	}

	pub fn is_hydrated(&self) -> bool {
		self.state.lock().hydrated
	}

	pub fn install(&self, hashes: &[u64]) {
		let mut state = self.state.lock();
		state.tracker.install(hashes);
		state.hydrated = true;
	}

	pub fn invalidate(&self) {
		self.state.lock().hydrated = false;
	}

	pub fn probe(&self, hash: u64) -> MembershipAnswer {
		self.state.lock().tracker.probe(hash)
	}

	pub fn insert(&self, hash: u64) {
		let mut state = self.state.lock();
		if state.tracker.insert(hash) {
			state.discards += 1;
		}
	}

	pub fn remove(&self, hash: u64) {
		self.state.lock().tracker.remove(hash);
	}

	pub fn record_store_miss(&self) {
		self.state.lock().tracker.record_store_miss();
	}

	pub fn memory(&self) -> StateMemory {
		self.state.lock().tracker.memory()
	}

	pub fn completeness(&self) -> StateCompleteness {
		let state = self.state.lock();
		StateCompleteness {
			values_complete: true,
			membership_complete: state.tracker.is_tracked(),
			absences_served: Count::new(state.tracker.absences_served()),
			false_positives: Count::new(state.tracker.false_positives()),
			revocations: Count::new(state.discards),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::state::membership::MEMBERSHIP_BYTE_CAP;

	#[test]
	fn probes_are_untracked_until_hydration_installs_an_index() {
		// Before the keyspace is scanned the only safe answer is Untracked, which forces a store
		// read; DefinitelyAbsent would be silent state loss for every key written before this
		// process started.
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
		// Join sides store one filter instance per row, so removing one row of two must not flip the
		// key to absent - contains_key would then deny a key that still has matches.
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
	fn invalidation_forces_a_rebuild_that_drops_instances_the_substrate_deleted() {
		// Group reclamation deletes every row of a key and reports only the group id, with no row
		// count. remove() decrements one instance, so a key that held N rows would strand N-1 and
		// read maybe-present forever; only a rebuild from the store is an exact correction.
		let membership = KeyspaceMembership::new(MEMBERSHIP_BYTE_CAP);
		membership.install(&[]);
		for _ in 0..3 {
			membership.insert(7);
		}
		assert_eq!(membership.probe(7), MembershipAnswer::MaybePresent);

		membership.invalidate();
		assert!(!membership.is_hydrated(), "an invalidated keyspace must re-scan before it answers again");

		membership.install(&[]);
		assert_eq!(
			membership.probe(7),
			MembershipAnswer::DefinitelyAbsent,
			"the rebuild must drop every instance of a key the store no longer holds"
		);
	}

	#[test]
	fn safe_overcount_degrades_to_a_false_positive_never_a_false_absence() {
		// A blind insert with unknown prior presence may over-count; the stale instance must surface
		// as a counted false positive on the verify read, never as a false absence.
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
		// One hot join key holds one filter instance per stored row; chaining those to the byte cap
		// used to discard the whole side into permanent read-through. Hundreds of instances of one
		// hash must leave the index alive, other keys' absence proofs intact, and count no revocation.
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
		// A filter that cannot grow must degrade to Untracked rather than drop instances, since a
		// partial filter produces false absences; the revocations counter records why.
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
		// Hydrating a keyspace larger than the cap must land in the same Untracked mode as a mid-run
		// overflow; a truncated install would deny every key the scan could not fit.
		let membership = KeyspaceMembership::new(64);
		let hashes: Vec<u64> = (0..100_000).collect();
		membership.install(&hashes);
		assert!(membership.is_hydrated(), "a failed install must not trigger endless rescans");
		assert_eq!(membership.probe(5), MembershipAnswer::Untracked);
	}

	#[test]
	fn record_store_miss_without_an_index_counts_nothing() {
		// Untracked mode reads through by design, so those misses are not filter false positives and
		// counting them would drown the real signal.
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
