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
//!
//! How an interval is proven differs per cache. Where a partition's key range can be reconstructed
//! arithmetically, residency alone seeds a covered interval; where the key suffixes are opaque, the
//! cache can only ever know what a scan observed.

pub mod cursor;
pub mod entry;
pub mod index;
pub mod interval;
pub mod plan;
pub mod retraction;

#[cfg(test)]
mod protocol;

use std::cmp::Ordering;

use reifydb_codec::key::encoded::EncodedKey;

/// The exclusive upper end of a coverage interval.
///
/// Byte-lexicographic order gives the empty key as a natural bottom, so a lower bound is always a
/// concrete [`EncodedKey`]. There is no such largest key, so an unbounded upper end needs the
/// [`ExclusiveUpperEnd::Top`] sentinel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExclusiveUpperEnd {
	Key(EncodedKey),
	Top,
}

impl ExclusiveUpperEnd {
	pub fn of(key: impl AsRef<[u8]>) -> Self {
		ExclusiveUpperEnd::Key(EncodedKey::new(key))
	}

	pub fn is_top(&self) -> bool {
		matches!(self, ExclusiveUpperEnd::Top)
	}

	pub fn key(&self) -> Option<&EncodedKey> {
		match self {
			ExclusiveUpperEnd::Key(key) => Some(key),
			ExclusiveUpperEnd::Top => None,
		}
	}

	pub fn cmp_key(&self, key: &EncodedKey) -> Ordering {
		match self {
			ExclusiveUpperEnd::Key(edge) => edge.as_slice().cmp(key.as_slice()),
			ExclusiveUpperEnd::Top => Ordering::Greater,
		}
	}

	pub fn covers(&self, key: &EncodedKey) -> bool {
		self.cmp_key(key) == Ordering::Greater
	}

	pub fn min(self, other: ExclusiveUpperEnd) -> ExclusiveUpperEnd {
		match (&self, &other) {
			(ExclusiveUpperEnd::Top, _) => other,
			(_, ExclusiveUpperEnd::Top) => self,
			(ExclusiveUpperEnd::Key(left), ExclusiveUpperEnd::Key(right)) => {
				if left.as_slice() <= right.as_slice() {
					self
				} else {
					other
				}
			}
		}
	}

	pub fn max(self, other: ExclusiveUpperEnd) -> ExclusiveUpperEnd {
		match (&self, &other) {
			(ExclusiveUpperEnd::Top, _) | (_, ExclusiveUpperEnd::Top) => ExclusiveUpperEnd::Top,
			(ExclusiveUpperEnd::Key(left), ExclusiveUpperEnd::Key(right)) => {
				if left.as_slice() >= right.as_slice() {
					self
				} else {
					other
				}
			}
		}
	}
}

impl PartialOrd for ExclusiveUpperEnd {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for ExclusiveUpperEnd {
	fn cmp(&self, other: &Self) -> Ordering {
		match (self, other) {
			(ExclusiveUpperEnd::Top, ExclusiveUpperEnd::Top) => Ordering::Equal,
			(ExclusiveUpperEnd::Top, ExclusiveUpperEnd::Key(_)) => Ordering::Greater,
			(ExclusiveUpperEnd::Key(_), ExclusiveUpperEnd::Top) => Ordering::Less,
			(ExclusiveUpperEnd::Key(left), ExclusiveUpperEnd::Key(right)) => {
				left.as_slice().cmp(right.as_slice())
			}
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
