// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry<V> {
	Row(V),
	Deleted,
	Absent,
}

impl<V> Entry<V> {
	pub fn row(value: V) -> Self {
		Self::Row(value)
	}

	pub fn deleted() -> Self {
		Self::Deleted
	}

	pub fn absent() -> Self {
		Self::Absent
	}

	pub fn value(&self) -> Option<&V> {
		match self {
			Entry::Row(value) => Some(value),
			Entry::Deleted | Entry::Absent => None,
		}
	}

	pub fn resolves(&self) -> bool {
		match self {
			Entry::Row(_) | Entry::Deleted | Entry::Absent => true,
		}
	}

	pub fn evictable(&self, removals: bool) -> bool {
		match self {
			Entry::Row(_) | Entry::Absent => true,
			Entry::Deleted => removals,
		}
	}

	fn removal(&self) -> bool {
		matches!(self, Entry::Deleted)
	}
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PinnedCount {
	removals: usize,
	total: usize,
}

impl PinnedCount {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn insert<V>(&mut self, entry: &Entry<V>) {
		self.total += 1;
		if entry.removal() {
			self.removals += 1;
		}
	}

	pub fn remove<V>(&mut self, entry: &Entry<V>) {
		self.total -= 1;
		if entry.removal() {
			self.removals -= 1;
		}
	}

	pub fn replace<V>(&mut self, before: &Entry<V>, after: &Entry<V>) {
		match (before.removal(), after.removal()) {
			(false, true) => self.removals += 1,
			(true, false) => self.removals -= 1,
			_ => {}
		}
	}

	pub fn removals(&self) -> usize {
		self.removals
	}

	pub fn total(&self) -> usize {
		self.total
	}

	pub fn has_victim(&self, removals: bool) -> bool {
		if removals {
			return self.total > 0;
		}
		self.total > self.removals
	}
}

#[cfg(test)]
mod tests {
	use super::{Entry, PinnedCount};

	fn row(value: u32) -> Entry<u32> {
		Entry::row(value)
	}

	fn deleted() -> Entry<u32> {
		Entry::deleted()
	}

	fn absent() -> Entry<u32> {
		Entry::absent()
	}

	#[test]
	fn deleted_is_not_evictable_where_nothing_beneath_records_the_removal() {
		// Dropping a removal the persistent tier has not seen resurrects the row it still holds.
		assert!(!deleted().evictable(false));
	}

	#[test]
	fn deleted_is_evictable_where_the_layer_beneath_records_the_removal() {
		// There the drop reads as unknown and the layer below answers, so pinning only wastes the budget.
		assert!(deleted().evictable(true));
	}

	#[test]
	fn a_row_and_a_proven_absence_are_evictable() {
		// Neither contradicts the persistent tier, so eviction only shrinks coverage.
		assert!(row(7).evictable(false));
		assert!(absent().evictable(false));
	}

	#[test]
	fn only_a_row_carries_a_value() {
		// A removal or an absence must never be read back as a row.
		assert_eq!(row(7).value(), Some(&7));
		assert_eq!(deleted().value(), None);
		assert_eq!(absent().value(), None);
	}

	#[test]
	fn every_state_resolves_a_read_outright() {
		// A stored absence exists precisely so a read stops here instead of hitting sqlite.
		assert!(row(7).resolves());
		assert!(deleted().resolves());
		assert!(absent().resolves());
	}

	#[test]
	fn insert_charges_a_removal_against_the_pinned_floor() {
		// A budget that cannot see the floor would keep asking for victims that do not exist.
		let mut count = PinnedCount::new();
		count.insert(&deleted());
		assert_eq!(count.removals(), 1);
		assert_eq!(count.total(), 1);
	}

	#[test]
	fn insert_charges_an_evictable_entry_to_total_only() {
		// Counting a row as pinned would understate the victims eviction can actually take.
		let mut count = PinnedCount::new();
		count.insert(&row(7));
		count.insert(&absent());
		assert_eq!(count.removals(), 0);
		assert_eq!(count.total(), 2);
	}

	#[test]
	fn replace_moves_an_entry_across_the_pin_without_changing_total() {
		// A transition is one entry before and after; charging total again double counts it.
		let mut count = PinnedCount::new();
		count.insert(&row(7));
		assert_eq!((count.removals(), count.total()), (0, 1));

		count.replace(&row(7), &deleted());
		assert_eq!((count.removals(), count.total()), (1, 1));

		count.replace(&deleted(), &absent());
		assert_eq!((count.removals(), count.total()), (0, 1));
	}

	#[test]
	fn replace_between_two_evictable_states_changes_nothing() {
		// An overwrite of a live row must not disturb the pinned floor.
		let mut count = PinnedCount::new();
		count.insert(&row(7));
		count.replace(&row(7), &row(9));
		assert_eq!((count.removals(), count.total()), (0, 1));
	}

	#[test]
	fn pinned_count_returns_to_zero_across_a_full_lifecycle() {
		// Any leak in the arithmetic leaves a phantom floor eviction can never clear.
		let mut count = PinnedCount::new();
		count.insert(&row(7));
		count.insert(&absent());
		count.insert(&deleted());
		assert_eq!((count.removals(), count.total()), (1, 3));

		count.replace(&deleted(), &absent());
		assert_eq!((count.removals(), count.total()), (0, 3));

		count.remove(&row(7));
		count.remove(&absent());
		count.remove(&absent());
		assert_eq!(count, PinnedCount::new());
	}

	#[test]
	fn remove_releases_the_pin_of_a_removal_it_drops() {
		// A pin outliving its entry wedges the shard on a floor with nothing behind it.
		let mut count = PinnedCount::new();
		count.insert(&deleted());
		count.remove(&deleted());
		assert_eq!((count.removals(), count.total()), (0, 0));
	}

	#[test]
	fn has_victim_is_false_when_every_entry_is_pinned() {
		// The eviction loop must stop cleanly here rather than spin on an all-pinned sample.
		let mut count = PinnedCount::new();
		count.insert(&deleted());
		count.insert(&deleted());
		assert!(!count.has_victim(false));
	}

	#[test]
	fn has_victim_offers_every_entry_where_removals_are_released() {
		// An all-removal partition must still yield a victim, or the tier fills with entries it cannot free.
		let mut count = PinnedCount::new();
		count.insert(&deleted());
		count.insert(&deleted());
		assert!(count.has_victim(true));
	}

	#[test]
	fn has_victim_is_true_when_one_unpinned_entry_remains() {
		// Pressure must fall through to the single victim, not give up because most are pinned.
		let mut count = PinnedCount::new();
		count.insert(&deleted());
		count.insert(&deleted());
		count.insert(&row(7));
		assert!(count.has_victim(false));
	}

	#[test]
	fn has_victim_is_false_when_there_are_no_entries() {
		// An empty shard has nothing to offer and must not report a victim.
		assert!(!PinnedCount::new().has_victim(false));
	}

	#[test]
	fn has_victim_follows_a_flush_that_releases_the_last_pin() {
		// Flush lag, not tombstone density, is what bounds the pinned population.
		let mut count = PinnedCount::new();
		count.insert(&deleted());
		assert!(!count.has_victim(false));

		count.replace(&deleted(), &absent());
		assert!(count.has_victim(false));
	}
}
