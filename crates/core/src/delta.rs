// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::cmp;

use crate::value::encoded::{EncodedKey, EncodedValues};

#[derive(Debug, PartialEq, Eq)]
pub enum Delta {
	Set {
		key: EncodedKey,
		values: EncodedValues,
	},
	Remove {
		key: EncodedKey,
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
			Self::Remove {
				key,
			} => key,
		}
	}

	/// Returns the encoded, if None, it means the entry is marked as remove.
	pub fn values(&self) -> Option<&EncodedValues> {
		match self {
			Self::Set {
				values: row,
				..
			} => Some(row),
			Self::Remove {
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
				values: value,
			} => Self::Set {
				key: key.clone(),
				values: value.clone(),
			},
			Self::Remove {
				key,
			} => Self::Remove {
				key: key.clone(),
			},
		}
	}
}
