// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Partial range coverage: the shared claim that RAM is authoritative over a span of the key
//! space, so a cache holding a subset of keys can still answer a range by serving what it can
//! prove and filling the gaps from the persistent tier.
//!
//! The unit of proof is the interval between two adjacent observed keys, not the whole
//! partition. A scan that returns `c, f, g` starting at `a` has proven that nothing exists in
//! `[a, g]` beyond those three keys, and that proof survives a later write of `k` into the span
//! because the writer is the one placing `k` there. Coverage therefore only shrinks on eviction.
//!
//! Coverage is stored apart from the rows it describes. It is small, read-mostly and ordered
//! across every partition, while rows are bulk, write-heavy and sharded. The two are sequenced,
//! never held together:
//!
//! ```text
//! on write or invalidate    shrink coverage first, then mutate rows
//! on fill                   insert rows first, then extend coverage
//! ```
//!
//! Coverage may understate what RAM holds and must never overstate it. A partition lock and the
//! coverage lock are never held at the same time; the orderings above are sequences of separately
//! locked steps, which is what keeps the understatement safe rather than atomic.

pub mod chunk;
pub mod entry;
pub mod interval;
pub mod plan;

use std::{cmp::Ordering, hash::Hash};

use reifydb_codec::key::encoded::EncodedKey;

/// The exclusive upper end of a coverage interval.
///
/// Byte-lexicographic order gives the empty key as a natural bottom, so a lower bound is always a
/// concrete [`EncodedKey`]. There is no such largest key, so an unbounded upper end needs the
/// [`Edge::Top`] sentinel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Edge {
	Key(EncodedKey),
	Top,
}

impl Edge {
	pub fn of(key: impl AsRef<[u8]>) -> Self {
		Edge::Key(EncodedKey::new(key))
	}

	pub fn is_top(&self) -> bool {
		matches!(self, Edge::Top)
	}

	pub fn key(&self) -> Option<&EncodedKey> {
		match self {
			Edge::Key(key) => Some(key),
			Edge::Top => None,
		}
	}

	pub fn cmp_key(&self, key: &EncodedKey) -> Ordering {
		match self {
			Edge::Key(edge) => edge.as_slice().cmp(key.as_slice()),
			Edge::Top => Ordering::Greater,
		}
	}

	pub fn covers(&self, key: &EncodedKey) -> bool {
		self.cmp_key(key) == Ordering::Greater
	}

	pub fn min(self, other: Edge) -> Edge {
		match (&self, &other) {
			(Edge::Top, _) => other,
			(_, Edge::Top) => self,
			(Edge::Key(left), Edge::Key(right)) => {
				if left.as_slice() <= right.as_slice() {
					self
				} else {
					other
				}
			}
		}
	}

	pub fn max(self, other: Edge) -> Edge {
		match (&self, &other) {
			(Edge::Top, _) | (_, Edge::Top) => Edge::Top,
			(Edge::Key(left), Edge::Key(right)) => {
				if left.as_slice() >= right.as_slice() {
					self
				} else {
					other
				}
			}
		}
	}
}

impl PartialOrd for Edge {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Edge {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(Edge::Top, Edge::Top) => Ordering::Equal,
			(Edge::Top, Edge::Key(_)) => Ordering::Greater,
			(Edge::Key(_), Edge::Top) => Ordering::Less,
			(Edge::Key(left), Edge::Key(right)) => left.as_slice().cmp(right.as_slice()),
		}
	}
}

/// The immediate successor of `key` in byte-lexicographic order over all byte strings.
///
/// Appending a zero byte is exact: no string sorts strictly between `k` and `k || 0x00`. This is
/// what turns an exclusive lower bound into the inclusive one a coverage interval stores.
pub fn successor(key: &EncodedKey) -> EncodedKey {
	let mut bytes = Vec::with_capacity(key.len() + 1);
	bytes.extend_from_slice(key.as_slice());
	bytes.push(0);
	EncodedKey::new(bytes)
}

/// A key space a cache is layered over: how a key maps to a storage partition, and whether that
/// partition's span can be derived without a scan.
///
/// `seed_span` returning `Some` is a cheap special case of observed coverage: `store-multi` can
/// reconstruct a page's key range arithmetically, so residency alone seeds a covered interval.
/// `store-operator` returns `None`, because operator key suffixes are opaque and it can only ever
/// know what a scan observed.
pub trait CacheDomain {
	type Partition: Copy + Eq + Hash;

	type Value: Clone;

	fn partition_of(key: &EncodedKey) -> Option<Self::Partition>;

	fn seed_span(partition: Self::Partition) -> Option<(EncodedKey, Edge)>;
}
