// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cmp, cmp::Reverse};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{common::CommitVersion, delta::Delta, interface::store::MultiVersionRow, key::any::AnyKey};
use reifydb_value::util::cowvec::CowVec;

pub enum TransactionValue {
	Pending(DeltaEntry),
	Committed(Committed),
}

impl From<MultiVersionRow<AnyKey>> for TransactionValue {
	fn from(value: MultiVersionRow<AnyKey>) -> Self {
		Self::Committed(Committed {
			key: value.key,
			bytes: value.bytes,
			version: value.version,
		})
	}
}

impl core::fmt::Debug for TransactionValue {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		let mut out = f.debug_struct("TransactionValue");
		match self {
			Self::Pending(item) => out.field("key", item.key()),
			Self::Committed(item) => out.field("key", item.key()),
		};
		out.field("version", &self.version()).field("value", &self.bytes()).finish()
	}
}

impl Clone for TransactionValue {
	fn clone(&self) -> Self {
		match self {
			Self::Committed(item) => Self::Committed(item.clone()),
			Self::Pending(delta) => Self::Pending(delta.clone()),
		}
	}
}

impl TransactionValue {
	pub fn version(&self) -> CommitVersion {
		match self {
			Self::Pending(item) => item.version(),
			Self::Committed(item) => item.version(),
		}
	}

	pub fn bytes(&self) -> &EncodedBytes {
		match self {
			Self::Pending(item) => item.bytes().expect("encoded of pending cannot be `None`"),
			Self::Committed(item) => &item.bytes,
		}
	}

	pub fn is_committed(&self) -> bool {
		matches!(self, Self::Committed(_))
	}

	pub fn into_multi_version_row(self) -> MultiVersionRow<AnyKey> {
		match self {
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
	pub(crate) key: AnyKey,
	pub(crate) bytes: EncodedBytes,
	pub(crate) version: CommitVersion,
}

impl From<MultiVersionRow<AnyKey>> for Committed {
	fn from(value: MultiVersionRow<AnyKey>) -> Self {
		Self {
			key: value.key,
			bytes: value.bytes,
			version: value.version,
		}
	}
}

impl Committed {
	pub fn key(&self) -> &AnyKey {
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

	pub fn key(&self) -> &AnyKey {
		self.delta.key()
	}

	pub fn bytes(&self) -> Option<&EncodedBytes> {
		self.delta.bytes()
	}

	pub fn was_removed(&self) -> bool {
		matches!(self.delta, Delta::Remove { .. })
	}
}
