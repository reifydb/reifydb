// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::cmp;

use reifydb_codec::{encoded::row::EncodedRow, key::encoded::EncodedKey};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Delta {
	Set {
		key: EncodedKey,
		row: EncodedRow,
	},

	Remove {
		key: EncodedKey,
		announce: RemoveAnnounce,
	},
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RemoveAnnounce {
	Silent,

	Announced {
		pre: EncodedRow,
	},
}

impl RemoveAnnounce {
	pub fn announces(&self) -> bool {
		matches!(self, Self::Announced { .. })
	}

	pub fn pre(&self) -> Option<&EncodedRow> {
		match self {
			Self::Silent => None,
			Self::Announced {
				pre,
			} => Some(pre),
		}
	}
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
	pub fn remove_silent(key: EncodedKey) -> Self {
		Self::Remove {
			key,
			announce: RemoveAnnounce::Silent,
		}
	}

	pub fn remove_announced(key: EncodedKey, pre: EncodedRow) -> Self {
		Self::Remove {
			key,
			announce: RemoveAnnounce::Announced {
				pre,
			},
		}
	}

	pub fn key(&self) -> &EncodedKey {
		match self {
			Self::Set {
				key,
				..
			} => key,
			Self::Remove {
				key,
				..
			} => key,
		}
	}

	pub fn row(&self) -> Option<&EncodedRow> {
		match self {
			Self::Set {
				row,
				..
			} => Some(row),
			Self::Remove {
				..
			} => None,
		}
	}

	pub fn announces(&self) -> bool {
		match self {
			Self::Set {
				..
			} => true,
			Self::Remove {
				announce,
				..
			} => announce.announces(),
		}
	}
}
