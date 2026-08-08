// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp, cmp::Reverse};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{common::CommitVersion, delta::Delta, interface::store::MultiVersionRow};
use reifydb_value::util::cowvec::CowVec;

pub enum TransactionValue {
	PendingIter {
		version: CommitVersion,
		key: EncodedKey,
		bytes: EncodedBytes,
	},
	Pending(DeltaEntry),
	Committed(Committed),
}

impl From<MultiVersionRow> for TransactionValue {
	fn from(value: MultiVersionRow) -> Self {
		Self::Committed(Committed {
			key: value.key,
			bytes: value.bytes,
			version: value.version,
		})
	}
}

impl core::fmt::Debug for TransactionValue {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		f.debug_struct("TransactionValue")
			.field("key", self.key())
			.field("version", &self.version())
			.field("value", &self.bytes())
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
				bytes: value,
			} => Self::PendingIter {
				version: *version,
				key: key.clone(),
				bytes: value.clone(),
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

	pub fn bytes(&self) -> &EncodedBytes {
		match self {
			Self::PendingIter {
				bytes,
				..
			} => bytes,
			Self::Pending(item) => item.bytes().expect("encoded of pending cannot be `None`"),
			Self::Committed(item) => &item.bytes,
		}
	}

	pub fn is_committed(&self) -> bool {
		matches!(self, Self::Committed(_))
	}

	pub fn into_multi_version_row(self) -> MultiVersionRow {
		match self {
			Self::PendingIter {
				version,
				key,
				bytes,
			} => MultiVersionRow {
				key,
				bytes,
				version,
			},
			Self::Pending(item) => match item.delta {
				Delta::Set {
					key,
					bytes,
				} => MultiVersionRow {
					key,
					bytes,
					version: item.version,
				},
				Delta::Remove {
					key,
					..
				} => MultiVersionRow {
					key,
					bytes: EncodedBytes(CowVec::default()),
					version: item.version,
				},
			},
			Self::Committed(item) => MultiVersionRow {
				key: item.key,
				bytes: item.bytes,
				version: item.version,
			},
		}
	}
}

impl From<(CommitVersion, EncodedKey, EncodedBytes)> for TransactionValue {
	fn from((version, k, b): (CommitVersion, EncodedKey, EncodedBytes)) -> Self {
		Self::PendingIter {
			version,
			key: k,
			bytes: b,
		}
	}
}

impl From<(CommitVersion, &EncodedKey, &EncodedBytes)> for TransactionValue {
	fn from((version, k, b): (CommitVersion, &EncodedKey, &EncodedBytes)) -> Self {
		Self::PendingIter {
			version,
			key: k.clone(),
			bytes: b.clone(),
		}
	}
}

impl From<DeltaEntry> for TransactionValue {
	fn from(pending: DeltaEntry) -> Self {
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
	pub(crate) bytes: EncodedBytes,
	pub(crate) version: CommitVersion,
}

impl From<MultiVersionRow> for Committed {
	fn from(value: MultiVersionRow) -> Self {
		Self {
			key: value.key,
			bytes: value.bytes,
			version: value.version,
		}
	}
}

impl Committed {
	pub fn key(&self) -> &EncodedKey {
		&self.key
	}

	pub fn bytes(&self) -> &EncodedBytes {
		&self.bytes
	}

	pub fn version(&self) -> CommitVersion {
		self.version
	}
}

#[derive(Debug, PartialEq, Eq)]
pub struct DeltaEntry {
	pub delta: Delta,
	pub version: CommitVersion,
}

impl PartialOrd for DeltaEntry {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for DeltaEntry {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		self.delta.key().cmp(other.delta.key()).then_with(|| Reverse(self.version).cmp(&Reverse(other.version)))
	}
}

impl Clone for DeltaEntry {
	fn clone(&self) -> Self {
		Self {
			version: self.version,
			delta: self.delta.clone(),
		}
	}
}

impl DeltaEntry {
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

	pub fn bytes(&self) -> Option<&EncodedBytes> {
		self.delta.bytes()
	}

	pub fn was_removed(&self) -> bool {
		matches!(self.delta, Delta::Remove { .. })
	}
}
