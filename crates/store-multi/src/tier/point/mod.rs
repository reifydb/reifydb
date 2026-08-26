// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Point tier of the multi-version store: the shared point cache, instantiated over multi's keys and a
//! two-deep row.
//!
//! A dimension is one entry kind, matching the range tier, so an invalidation scoped to a storage reaches
//! every key it cached. There is one counter slot, because multi has no keyspace byte to attribute a read
//! to and every point read here is the same kind of work.
//!
//! The version scope filter lives above this module, not in it: the tier stores whatever row the domain
//! names and knows nothing about which versions a reader may see.

use std::borrow::Cow;

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
use reifydb_store::tier::{
	point::{PointConfig, PointDomain, PointTier},
	range::RowBytes,
};
use reifydb_value::util::cowvec::CowVec;

pub type MultiPointConfig = PointConfig;
pub type MultiPointTier = PointTier<MultiPointDomain>;

/// Two cached versions of a key, since a reader below the newest version must still find what it displaced.
#[derive(Clone, Debug)]
pub struct MultiPointRow {
	pub version: CommitVersion,
	pub value: Option<CowVec<u8>>,
	pub previous: Option<(CommitVersion, Option<CowVec<u8>>)>,
}

impl MultiPointRow {
	pub fn new(version: CommitVersion, value: Option<CowVec<u8>>) -> Self {
		Self {
			version,
			value,
			previous: None,
		}
	}

	pub fn at(&self, read: CommitVersion) -> Option<(CommitVersion, &Option<CowVec<u8>>)> {
		if self.version <= read {
			return Some((self.version, &self.value));
		}
		match &self.previous {
			Some((version, value)) if *version <= read => Some((*version, value)),
			_ => None,
		}
	}

	pub fn served_previous(&self, read: CommitVersion) -> bool {
		self.version > read && self.previous.as_ref().is_some_and(|(version, _)| *version <= read)
	}
}

impl RowBytes for MultiPointRow {
	fn row_bytes(&self) -> usize {
		let current = self.value.as_ref().map_or(0, |value| value.len());
		let previous = self.previous.as_ref().map_or(0, |(_, value)| value.as_ref().map_or(0, CowVec::len));
		current + previous
	}
}

#[derive(Clone, Copy, Debug)]
pub struct MultiPointDomain;

impl PointDomain for MultiPointDomain {
	type Dimension = EntryKind;
	type Slot = ();
	type Row = MultiPointRow;

	const SLOTS: usize = 1;

	const SCOPE: &'static str = "multi_point";

	fn slot(_key: &EncodedKey) -> Option<usize> {
		Some(0)
	}

	fn caches_points(_slot: usize) -> bool {
		true
	}

	fn supersede(resident: &mut Self::Row, incoming: Self::Row) -> bool {
		if resident.version > incoming.version {
			return false;
		}
		resident.previous = if resident.version < incoming.version {
			Some((resident.version, resident.value.take()))
		} else {
			None
		};
		resident.version = incoming.version;
		resident.value = incoming.value;
		true
	}

	fn slot_at(_index: usize) -> Self::Slot {}

	fn slot_name(_slot: Self::Slot) -> Cow<'static, str> {
		Cow::Borrowed("row")
	}
}

#[cfg(test)]
mod tests {
	use super::{CommitVersion, CowVec, MultiPointDomain, MultiPointRow, PointDomain, RowBytes};

	fn value(body: &str) -> Option<CowVec<u8>> {
		Some(CowVec::new(body.as_bytes().to_vec()))
	}

	fn row(version: u64, body: &str) -> MultiPointRow {
		MultiPointRow::new(CommitVersion(version), value(body))
	}

	fn body(slot: &Option<CowVec<u8>>) -> &str {
		std::str::from_utf8(slot.as_ref().expect("the slot must carry a value")).expect("test bodies are utf8")
	}

	#[test]
	fn an_older_write_is_refused_rather_than_seated() {
		// Seating it would move the current slot backwards and strand the newer value in previous.
		let mut resident = row(5, "new");

		assert!(!MultiPointDomain::supersede(&mut resident, row(3, "old")), "an older write must be refused");
		assert_eq!(resident.version, CommitVersion(5), "the refusal moved the version backwards");
		assert_eq!(body(&resident.value), "new", "the refusal took the older value");
		assert!(resident.previous.is_none(), "the refusal invented a previous slot");
	}

	#[test]
	fn a_write_at_the_same_version_replaces_without_inventing_a_previous() {
		// One version can only hold one value, so keeping the displaced one would let a reader see a version twice.
		let mut resident = row(5, "first");

		assert!(MultiPointDomain::supersede(&mut resident, row(5, "second")), "a same-version write must land");
		assert_eq!(body(&resident.value), "second", "the newer value at the same version never landed");
		assert!(resident.previous.is_none(), "a same-version replace fabricated a version that never existed");
	}

	#[test]
	fn a_newer_write_pushes_the_displaced_value_into_previous() {
		// Dropping it instead makes every reader below the new version fall through to persistent.
		let mut resident = row(5, "old");

		assert!(MultiPointDomain::supersede(&mut resident, row(9, "new")), "a newer write must land");
		assert_eq!(resident.version, CommitVersion(9));
		assert_eq!(body(&resident.value), "new");
		let (version, displaced) = resident.previous.as_ref().expect("the displaced value must be kept");
		assert_eq!(*version, CommitVersion(5), "previous must carry the version it was written at");
		assert_eq!(body(displaced), "old");
	}

	#[test]
	fn a_displaced_tombstone_is_kept_like_any_other_value() {
		// Dropping it would let a reader below the new version see the value the tombstone deleted.
		let mut resident = MultiPointRow::new(CommitVersion(5), None);

		assert!(MultiPointDomain::supersede(&mut resident, row(9, "resurrected")), "a newer write must land");
		let (version, displaced) = resident.previous.as_ref().expect("a displaced tombstone must be kept");
		assert_eq!(*version, CommitVersion(5));
		assert!(displaced.is_none(), "the tombstone was rewritten as a value");
	}

	#[test]
	fn a_third_write_forgets_the_oldest_of_the_three() {
		// The chain is two deep by design, so the oldest must fall off rather than grow the row without bound.
		let mut resident = row(1, "first");
		MultiPointDomain::supersede(&mut resident, row(2, "second"));
		MultiPointDomain::supersede(&mut resident, row(3, "third"));

		assert_eq!(body(&resident.value), "third");
		let (version, displaced) = resident.previous.as_ref().expect("the chain must still hold one displaced value");
		assert_eq!(*version, CommitVersion(2), "the chain kept the wrong version");
		assert_eq!(body(displaced), "second", "a two-deep chain must forget the oldest, not the newest");
	}

	#[test]
	fn a_reader_below_the_current_version_is_served_from_previous() {
		// Answering from the current slot would show a reader a version committed after its snapshot.
		let mut resident = row(5, "old");
		MultiPointDomain::supersede(&mut resident, row(9, "new"));

		let (version, served) = resident.at(CommitVersion(7)).expect("previous must answer a reader below the current version");
		assert_eq!(version, CommitVersion(5));
		assert_eq!(body(served), "old");
		assert!(resident.served_previous(CommitVersion(7)), "the read came from previous and must be counted as such");
	}

	#[test]
	fn a_reader_below_both_versions_is_not_served_at_all() {
		// Serving the oldest slot anyway would answer with a value written after the reader's snapshot.
		let mut resident = row(5, "old");
		MultiPointDomain::supersede(&mut resident, row(9, "new"));

		assert!(resident.at(CommitVersion(4)).is_none(), "a reader below every cached version must fall through");
		assert!(!resident.served_previous(CommitVersion(4)), "a fall-through must not be counted as a previous hit");
	}

	#[test]
	fn a_reader_at_or_above_the_current_version_is_served_from_the_current_slot() {
		// Reading previous here would hand back a value the newer write already replaced.
		let mut resident = row(5, "old");
		MultiPointDomain::supersede(&mut resident, row(9, "new"));

		let (version, served) = resident.at(CommitVersion(9)).expect("the current slot must answer its own version");
		assert_eq!(version, CommitVersion(9));
		assert_eq!(body(served), "new");
		assert!(!resident.served_previous(CommitVersion(9)), "a current-slot read must not be counted as a previous hit");
	}

	#[test]
	fn the_footprint_counts_both_slots() {
		// Counting only the current slot lets the chain grow the row while the budget reports it unchanged.
		let mut resident = row(5, "aaaa");
		let current_only = resident.row_bytes();
		MultiPointDomain::supersede(&mut resident, row(9, "bbbbbbbb"));

		assert_eq!(current_only, 4);
		assert_eq!(resident.row_bytes(), 12, "the displaced value is resident and must be charged for");
	}
}
