// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::cmp;

use crate::{
	CommitVersion,
	value::encoded::{EncodedKey, EncodedValues},
};

#[derive(Debug, PartialEq, Eq)]
pub enum Delta {
	Set {
		key: EncodedKey,
		values: EncodedValues,
	},
	/// Unset an entry, preserving the deleted values.
	/// Symmetric with Set - use when the deleted data matters (e.g., row data, CDC).
	Unset {
		key: EncodedKey,
		values: EncodedValues,
	},
	/// Remove an entry without preserving the deleted values.
	/// Use when only the key matters (e.g., index entries, catalog metadata).
	Remove {
		key: EncodedKey,
	},
	/// Drop operation - completely erases versioned entries from storage.
	/// Unlike Remove (which writes a tombstone and generates CDC), Drop:
	/// - Deletes existing entries without writing anything new
	/// - Never generates CDC events
	Drop {
		key: EncodedKey,
		/// If Some(v), drop all versions where version < v (keeps v and later).
		/// If None, this constraint is not applied.
		up_to_version: Option<CommitVersion>,
		/// If Some(n), keep the n most recent versions, drop older ones.
		/// If None, this constraint is not applied.
		/// Can be combined with up_to_version (both constraints apply).
		/// If both are None, drops ALL versions.
		keep_last_versions: Option<usize>,
	},
}

impl PartialOrd for Delta {
	fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Delta {
	fn cmp(&self, other: &Self) -> cmp::Ordering {
		self.key().cmp(other.key())
	}
}

impl Delta {
	/// Returns the key
	pub fn key(&self) -> &EncodedKey {
		match self {
			Self::Set {
				key,
				..
			} => key,
			Self::Unset {
				key,
				..
			} => key,
			Self::Remove {
				key,
			} => key,
			Self::Drop {
				key,
				..
			} => key,
		}
	}

	/// Returns the encoded values, if None, it means the entry is marked as remove or drop.
	pub fn values(&self) -> Option<&EncodedValues> {
		match self {
			Self::Set {
				values,
				..
			} => Some(values),
			Self::Unset {
				..
			} => None,
			Self::Remove {
				..
			} => None,
			Self::Drop {
				..
			} => None,
		}
	}
}

impl Clone for Delta {
	fn clone(&self) -> Self {
		match self {
			Self::Set {
				key,
				values,
			} => Self::Set {
				key: key.clone(),
				values: values.clone(),
			},
			Self::Unset {
				key,
				values,
			} => Self::Unset {
				key: key.clone(),
				values: values.clone(),
			},
			Self::Remove {
				key,
			} => Self::Remove {
				key: key.clone(),
			},
			Self::Drop {
				key,
				up_to_version,
				keep_last_versions,
			} => Self::Drop {
				key: key.clone(),
				up_to_version: *up_to_version,
				keep_last_versions: *keep_last_versions,
			},
		}
	}
}
