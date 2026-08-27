// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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

pub fn successor(key: &EncodedKey) -> EncodedKey {
	let mut bytes = Vec::with_capacity(key.len() + 1);
	bytes.extend_from_slice(key.as_slice());
	bytes.push(0);
	EncodedKey::new(bytes)
}
