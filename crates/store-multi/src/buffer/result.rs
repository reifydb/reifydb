// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::key::EncodedKey,
	interface::store::{MultiVersionRow, SingleVersionRow},
};

#[derive(Debug, Clone)]
pub enum MultiVersionGetResult {
	Value(MultiVersionRow),
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

	pub fn into_option(self) -> Option<MultiVersionRow> {
		match self {
			Self::Value(v) => Some(v),
			_ => None,
		}
	}
}

impl From<MultiVersionGetResult> for Option<MultiVersionRow> {
	fn from(val: MultiVersionGetResult) -> Self {
		val.into_option()
	}
}

#[derive(Debug, Clone)]
pub enum SingleVersionGetResult {
	Value(SingleVersionRow),
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

	pub fn into_option(self) -> Option<SingleVersionRow> {
		match self {
			Self::Value(v) => Some(v),
			_ => None,
		}
	}
}

impl From<SingleVersionGetResult> for Option<SingleVersionRow> {
	fn from(val: SingleVersionGetResult) -> Self {
		val.into_option()
	}
}

#[derive(Debug, Clone)]
pub enum MultiVersionIterResult {
	Value(MultiVersionRow),
	Tombstone {
		key: EncodedKey,
		version: CommitVersion,
	},
}

#[derive(Debug, Clone)]
pub enum SingleVersionIterResult {
	Value(SingleVersionRow),
	Tombstone {
		key: EncodedKey,
	},
}

impl SingleVersionIterResult {
	pub fn into_option(self) -> Option<SingleVersionRow> {
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
