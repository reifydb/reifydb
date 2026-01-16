// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{cmp, cmp::Reverse};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{encoded::EncodedValues, key::EncodedKey},
	interface::store::MultiVersionValues,
};
use reifydb_type::util::cowvec::CowVec;

pub enum TransactionValue {
	PendingIter {
		version: CommitVersion,
		key: EncodedKey,
		values: EncodedValues,
	},
	Pending(Pending),
	Committed(Committed),
}

impl From<MultiVersionValues> for TransactionValue {
	fn from(value: MultiVersionValues) -> Self {
		Self::Committed(Committed {
			key: value.key,
			values: value.values,
			version: value.version,
		})
	}
}

impl core::fmt::Debug for TransactionValue {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		f.debug_struct("TransactionValue")
			.field("key", self.key())
			.field("version", &self.version())
			.field("value", &self.values())
			.finish()
	}
}

impl Clone for TransactionValue {
	fn clone(&self) -> Self {
		match self {
			Self::Committed(item) => Self::Committed(item.clone()),
			Self::Pending(delta) => Self::Pending(delta.clone()),
			Self::PendingIter {
				version,
				key,
				values: value,
			} => Self::PendingIter {
				version: *version,
				key: key.clone(),
				values: value.clone(),
			},
		}
	}
}

impl TransactionValue {
	pub fn key(&self) -> &EncodedKey {
		match self {
			Self::PendingIter {
				key,
				..
			} => key,
			Self::Pending(item) => item.key(),
			Self::Committed(item) => item.key(),
		}
	}

	pub fn version(&self) -> CommitVersion {
		match self {
			Self::PendingIter {
				version,
				..
			} => *version,
			Self::Pending(item) => item.version(),
			Self::Committed(item) => item.version(),
		}
	}

	pub fn values(&self) -> &EncodedValues {
		match self {
			Self::PendingIter {
				values,
				..
			} => values,
			Self::Pending(item) => item.values().expect("encoded of pending cannot be `None`"),
			Self::Committed(item) => &item.values,
		}
	}

	pub fn is_committed(&self) -> bool {
		matches!(self, Self::Committed(_))
	}

	pub fn into_multi_version_values(self) -> MultiVersionValues {
		match self {
			Self::PendingIter {
				version,
				key,
				values,
			} => MultiVersionValues {
				key,
				values,
				version,
			},
			Self::Pending(item) => match item.delta {
				Delta::Set {
					key,
					values,
				} => MultiVersionValues {
					key,
					values,
					version: item.version,
				},
				Delta::Unset {
					key,
					..
				}
				| Delta::Remove {
					key,
				}
				| Delta::Drop {
					key,
					..
				} => MultiVersionValues {
					key,
					values: EncodedValues(CowVec::default()),
					version: item.version,
				},
			},
			Self::Committed(item) => MultiVersionValues {
				key: item.key,
				values: item.values,
				version: item.version,
			},
		}
	}
}

impl From<(CommitVersion, EncodedKey, EncodedValues)> for TransactionValue {
	fn from((version, k, b): (CommitVersion, EncodedKey, EncodedValues)) -> Self {
		Self::PendingIter {
			version,
			key: k,
			values: b,
		}
	}
}

impl From<(CommitVersion, &EncodedKey, &EncodedValues)> for TransactionValue {
	fn from((version, k, b): (CommitVersion, &EncodedKey, &EncodedValues)) -> Self {
		Self::PendingIter {
			version,
			key: k.clone(),
			values: b.clone(),
		}
	}
}

impl From<Pending> for TransactionValue {
	fn from(pending: Pending) -> Self {
		Self::Pending(pending)
	}
}

impl From<Committed> for TransactionValue {
	fn from(item: Committed) -> Self {
		Self::Committed(item)
	}
}

#[derive(Clone, Debug)]
pub struct Committed {
	pub(crate) key: EncodedKey,
	pub(crate) values: EncodedValues,
	pub(crate) version: CommitVersion,
}

impl From<MultiVersionValues> for Committed {
	fn from(value: MultiVersionValues) -> Self {
		Self {
			key: value.key,
			values: value.values,
			version: value.version,
		}
	}
}

impl Committed {
	pub fn key(&self) -> &EncodedKey {
		&self.key
	}

	pub fn values(&self) -> &EncodedValues {
		&self.values
	}

	pub fn version(&self) -> CommitVersion {
		self.version
	}
}

#[derive(Debug, PartialEq, Eq)]
pub struct Pending {
	pub delta: Delta,
	pub version: CommitVersion,
}

impl PartialOrd for Pending {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Pending {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		self.delta.key().cmp(other.delta.key()).then_with(|| Reverse(self.version).cmp(&Reverse(other.version)))
	}
}

impl Clone for Pending {
	fn clone(&self) -> Self {
		Self {
			version: self.version,
			delta: self.delta.clone(),
		}
	}
}

impl Pending {
	pub fn delta(&self) -> &Delta {
		&self.delta
	}

	pub fn version(&self) -> CommitVersion {
		self.version
	}

	pub fn into_components(self) -> (CommitVersion, Delta) {
		(self.version, self.delta)
	}

	pub fn key(&self) -> &EncodedKey {
		self.delta.key()
	}

	pub fn values(&self) -> Option<&EncodedValues> {
		self.delta.values()
	}

	pub fn was_removed(&self) -> bool {
		matches!(self.delta, Delta::Unset { .. } | Delta::Remove { .. })
	}
}
