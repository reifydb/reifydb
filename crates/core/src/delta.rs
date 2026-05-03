// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::cmp;

use crate::encoded::{key::EncodedKey, row::EncodedRow};

#[derive(Debug, PartialEq, Eq)]
pub enum Delta {
	Set {
		key: EncodedKey,
		row: EncodedRow,
	},

	Unset {
		key: EncodedKey,
		row: EncodedRow,
	},

	Remove {
		key: EncodedKey,
	},

	Drop {
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

	pub fn row(&self) -> Option<&EncodedRow> {
		match self {
			Self::Set {
				row,
				..
			} => Some(row),
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
				row,
			} => Self::Set {
				key: key.clone(),
				row: row.clone(),
			},
			Self::Unset {
				key,
				row,
			} => Self::Unset {
				key: key.clone(),
				row: row.clone(),
			},
			Self::Remove {
				key,
			} => Self::Remove {
				key: key.clone(),
			},
			Self::Drop {
				key,
			} => Self::Drop {
				key: key.clone(),
			},
		}
	}
}
