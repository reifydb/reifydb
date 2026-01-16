// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::store::{MultiVersionValues, SingleVersionValues},
};

#[derive(Debug, Clone)]
pub enum MultiVersionGetResult {
	Value(MultiVersionValues),
	Tombstone {
		key: EncodedKey,
		version: CommitVersion,
	},
	NotFound,
}

impl MultiVersionGetResult {
	pub fn is_tombstone(&self) -> bool {
		matches!(self, Self::Tombstone { .. })
	}

	pub fn is_value(&self) -> bool {
		matches!(self, Self::Value(_))
	}

	pub fn into_option(self) -> Option<MultiVersionValues> {
		match self {
			Self::Value(v) => Some(v),
			_ => None,
		}
	}
}

impl Into<Option<MultiVersionValues>> for MultiVersionGetResult {
	fn into(self) -> Option<MultiVersionValues> {
		self.into_option()
	}
}

#[derive(Debug, Clone)]
pub enum SingleVersionGetResult {
	Value(SingleVersionValues),
	Tombstone {
		key: EncodedKey,
	},
	NotFound,
}

impl SingleVersionGetResult {
	pub fn is_tombstone(&self) -> bool {
		matches!(self, Self::Tombstone { .. })
	}

	pub fn is_value(&self) -> bool {
		matches!(self, Self::Value(_))
	}

	pub fn into_option(self) -> Option<SingleVersionValues> {
		match self {
			Self::Value(v) => Some(v),
			_ => None,
		}
	}
}

impl Into<Option<SingleVersionValues>> for SingleVersionGetResult {
	fn into(self) -> Option<SingleVersionValues> {
		self.into_option()
	}
}

#[derive(Debug, Clone)]
pub enum MultiVersionIterResult {
	Value(MultiVersionValues),
	Tombstone {
		key: EncodedKey,
		version: CommitVersion,
	},
}

#[derive(Debug, Clone)]
pub enum SingleVersionIterResult {
	Value(SingleVersionValues),
	Tombstone {
		key: EncodedKey,
	},
}

impl SingleVersionIterResult {
	pub fn into_option(self) -> Option<SingleVersionValues> {
		match self {
			Self::Value(v) => Some(v),
			Self::Tombstone {
				..
			} => None,
		}
	}

	pub fn key(&self) -> &EncodedKey {
		match self {
			Self::Value(v) => &v.key,
			Self::Tombstone {
				key,
			} => key,
		}
	}
}
