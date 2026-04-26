// SPDX-License-Identifier: Apache-2.0
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
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::store::MultiVersionRow,
};
use reifydb_type::util::cowvec::CowVec;

pub enum TransactionValue {
	PendingIter {
		version: CommitVersion,
		key: EncodedKey,
		row: EncodedRow,
	},
	Pending(DeltaEntry),
	Committed(Committed),
}

impl From<MultiVersionRow> for TransactionValue {
	fn from(value: MultiVersionRow) -> Self {
		Self::Committed(Committed {
			key: value.key,
			row: value.row,
			version: value.version,
		})
	}
}

impl core::fmt::Debug for TransactionValue {
	fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
		f.debug_struct("TransactionValue")
			.field("key", self.key())
			.field("version", &self.version())
			.field("value", &self.row())
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
				row: value,
			} => Self::PendingIter {
				version: *version,
				key: key.clone(),
				row: value.clone(),
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

	pub fn row(&self) -> &EncodedRow {
		match self {
			Self::PendingIter {
				row,
				..
			} => row,
			Self::Pending(item) => item.row().expect("encoded of pending cannot be `None`"),
			Self::Committed(item) => &item.row,
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
				row,
			} => MultiVersionRow {
				key,
				row,
				version,
			},
			Self::Pending(item) => match item.delta {
				Delta::Set {
					key,
					row,
				} => MultiVersionRow {
					key,
					row,
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
				} => MultiVersionRow {
					key,
					row: EncodedRow(CowVec::default()),
					version: item.version,
				},
			},
			Self::Committed(item) => MultiVersionRow {
				key: item.key,
				row: item.row,
				version: item.version,
			},
		}
	}
}

impl From<(CommitVersion, EncodedKey, EncodedRow)> for TransactionValue {
	fn from((version, k, b): (CommitVersion, EncodedKey, EncodedRow)) -> Self {
		Self::PendingIter {
			version,
			key: k,
			row: b,
		}
	}
}

impl From<(CommitVersion, &EncodedKey, &EncodedRow)> for TransactionValue {
	fn from((version, k, b): (CommitVersion, &EncodedKey, &EncodedRow)) -> Self {
		Self::PendingIter {
			version,
			key: k.clone(),
			row: b.clone(),
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
	pub(crate) row: EncodedRow,
	pub(crate) version: CommitVersion,
}

impl From<MultiVersionRow> for Committed {
	fn from(value: MultiVersionRow) -> Self {
		Self {
			key: value.key,
			row: value.row,
			version: value.version,
		}
	}
}

impl Committed {
	pub fn key(&self) -> &EncodedKey {
		&self.key
	}

	pub fn row(&self) -> &EncodedRow {
		&self.row
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

	pub fn row(&self) -> Option<&EncodedRow> {
		self.delta.row()
	}

	pub fn was_removed(&self) -> bool {
		matches!(self.delta, Delta::Unset { .. } | Delta::Remove { .. })
	}
}
