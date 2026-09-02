// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, hash::Hash};

use reifydb_core::key::typed::{Edge, TypedKey};

use crate::coverage::interval::CoverageSet;

pub struct CoverageIndex<D, K> {
	sets: HashMap<D, CoverageSet<K>>,
	heads: HashMap<D, Edge<K>>,
}

impl<D, K> Default for CoverageIndex<D, K> {
	fn default() -> Self {
		Self {
			sets: HashMap::new(),
			heads: HashMap::new(),
		}
	}
}

impl<D: Hash + Eq + Copy, K: TypedKey> CoverageIndex<D, K> {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn set(&self, dimension: D) -> Option<&CoverageSet<K>> {
		self.sets.get(&dimension)
	}

	pub fn contains(&self, dimension: D, key: &K) -> bool {
		self.sets.get(&dimension).is_some_and(|set| set.contains(key))
	}

	pub fn extend(&mut self, dimension: D, start: K, end: Edge<K>) {
		self.sets.entry(dimension).or_default().extend(start, end);
	}

	pub fn shrink_key(&mut self, dimension: D, key: &K) {
		self.shrink(dimension, |set| set.shrink_key(key));
	}

	pub fn shrink_range(&mut self, dimension: D, start: &K, end: &Edge<K>) {
		self.shrink(dimension, |set| set.shrink_range(start, end));
	}

	pub fn head(&self, dimension: D) -> Option<&Edge<K>> {
		self.heads.get(&dimension)
	}

	pub fn set_head(&mut self, dimension: D, key: Edge<K>) {
		self.heads.insert(dimension, key);
	}

	pub fn remove(&mut self, dimension: D) {
		self.sets.remove(&dimension);
		self.heads.remove(&dimension);
	}

	pub fn retain(&mut self, keep: impl Fn(&D) -> bool) {
		self.sets.retain(|dimension, _| keep(dimension));
		self.heads.retain(|dimension, _| keep(dimension));
	}

	pub fn clear(&mut self) {
		self.sets.clear();
		self.heads.clear();
	}

	pub fn intervals(&self) -> usize {
		self.sets.values().map(|set| set.len()).sum()
	}

	pub fn iter(&self) -> impl Iterator<Item = (D, &CoverageSet<K>)> + '_ {
		self.sets.iter().map(|(dimension, set)| (*dimension, set))
	}

	fn shrink(&mut self, dimension: D, apply: impl FnOnce(&mut CoverageSet<K>)) {
		let Some(set) = self.sets.get_mut(&dimension) else {
			return;
		};
		apply(set);
		if set.is_empty() {
			self.sets.remove(&dimension);
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::key::typed::{Edge, MultiKey};

	use super::CoverageIndex;
	use crate::coverage::interval::Interval;

	fn k(bytes: &str) -> EncodedKey {
		EncodedKey::new(bytes)
	}

	fn index() -> CoverageIndex<u8, MultiKey> {
		CoverageIndex::new()
	}

	fn intervals(index: &CoverageIndex<u8, MultiKey>, dimension: u8) -> Vec<Interval<MultiKey>> {
		index.set(dimension).map(|set| set.iter().collect()).unwrap_or_default()
	}

	#[test]
	fn a_fresh_index_claims_nothing_for_any_dimension() {
		// An unscanned dimension must read absent, or a point read answers an absence persistent still holds.
		let index = index();
		assert!(index.set(1).is_none());
		assert!(!index.contains(1, &k("c")));
		assert_eq!(index.intervals(), 0);
	}

	#[test]
	fn a_claim_is_confined_to_the_dimension_that_made_it() {
		// A claim must never leak sideways, or one dimension's key is answered out of another's proof.
		let mut index = index();
		index.extend(1, k("c"), Edge::of("f"));

		assert!(index.contains(1, &k("d")));
		assert!(!index.contains(2, &k("d")));
		assert!(index.set(2).is_none());
	}

	#[test]
	fn extending_the_same_dimension_coalesces_into_one_interval() {
		// Adjacent claims must merge, otherwise a plan splits a covered span into gaps it re-reads.
		let mut index = index();
		index.extend(1, k("c"), Edge::of("f"));
		index.extend(1, k("f"), Edge::of("j"));

		assert_eq!(intervals(&index, 1), vec![Interval::new(k("c"), Edge::of("j"))]);
		assert_eq!(index.intervals(), 1);
	}

	#[test]
	fn shrinking_one_key_out_of_a_wider_claim_keeps_the_rest() {
		// A withdrawal must cost exactly the key withdrawn, never the whole dimension.
		let mut index = index();
		index.extend(1, k("c"), Edge::of("f"));

		index.shrink_key(1, &k("d"));

		assert!(index.contains(1, &k("c")));
		assert!(!index.contains(1, &k("d")));
		assert!(index.contains(1, &k("e")));
	}

	#[test]
	fn a_dimension_shrunk_to_nothing_leaves_the_map() {
		// An emptied set reads exactly like an absent one, so keeping it is retention without a reader.
		let mut index = index();
		index.extend(1, k("c"), Edge::just_past(&k("c")));

		index.shrink_key(1, &k("c"));

		assert!(index.set(1).is_none());
		assert_eq!(index.intervals(), 0);
	}

	#[test]
	fn a_range_shrink_that_empties_a_dimension_leaves_the_map() {
		// Span retraction must prune on the same terms as a single key, or the two paths leave different
		// residue.
		let mut index = index();
		index.extend(1, k("c"), Edge::of("f"));

		index.shrink_range(1, &k("a"), &Edge::of("z"));

		assert!(index.set(1).is_none());
	}

	#[test]
	fn a_range_shrink_that_leaves_a_claim_standing_keeps_the_dimension() {
		// Pruning on any shrink rather than on emptiness would drop claims a reader still needs.
		let mut index = index();
		index.extend(1, k("c"), Edge::of("f"));

		index.shrink_range(1, &k("e"), &Edge::of("z"));

		assert_eq!(intervals(&index, 1), vec![Interval::new(k("c"), Edge::of("e"))]);
	}

	#[test]
	fn shrinking_a_dimension_that_never_claimed_is_inert() {
		// A withdrawal on an unknown dimension must not seat an empty set the prune then has to undo.
		let mut index = index();

		index.shrink_key(1, &k("c"));
		index.shrink_range(2, &k("a"), &Edge::Top);

		assert!(index.set(1).is_none());
		assert!(index.set(2).is_none());
	}

	#[test]
	fn removing_a_dimension_drops_every_claim_it_held() {
		// A surviving interval answers keys whose rows the wholesale invalidate has already dropped.
		let mut index = index();
		index.extend(1, k("c"), Edge::of("f"));
		index.extend(1, k("m"), Edge::Top);
		index.extend(2, k("c"), Edge::of("f"));

		index.remove(1);

		assert!(index.set(1).is_none());
		assert!(index.contains(2, &k("d")));
	}

	#[test]
	fn clear_drops_every_dimension() {
		// A clear that spares one dimension leaves a claim over rows the tier is about to drop.
		let mut index = index();
		index.extend(1, k("c"), Edge::of("f"));
		index.extend(2, k("c"), Edge::of("f"));

		index.clear();

		assert_eq!(index.intervals(), 0);
		assert_eq!(index.iter().count(), 0);
	}

	#[test]
	fn intervals_totals_every_dimension() {
		// The gauge is tier-wide; counting one dimension understates the coverage a plan can draw on.
		let mut index = index();
		index.extend(1, k("c"), Edge::of("f"));
		index.extend(1, k("m"), Edge::of("p"));
		index.extend(2, k("c"), Edge::of("f"));

		assert_eq!(index.intervals(), 3);
	}

	#[test]
	fn iter_yields_each_dimension_with_its_own_set() {
		// The pairing is what lets a caller attribute an interval back to the dimension that proved it.
		let mut index = index();
		index.extend(1, k("c"), Edge::of("f"));
		index.extend(2, k("m"), Edge::of("p"));

		let mut seen: Vec<(u8, Vec<Interval<MultiKey>>)> =
			index.iter().map(|(dimension, set)| (dimension, set.iter().collect())).collect();
		seen.sort_by_key(|(dimension, _)| *dimension);

		assert_eq!(
			seen,
			vec![
				(1, vec![Interval::new(k("c"), Edge::of("f"))]),
				(2, vec![Interval::new(k("m"), Edge::of("p"))]),
			]
		);
	}
}
