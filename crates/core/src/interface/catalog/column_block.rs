// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	error::Error,
	value::{Value, partition::Partition},
};
use serde::{Deserialize, Serialize, de};

use crate::{
	common::CommitVersion,
	interface::catalog::id::{ColumnBlockId, NamespaceId, SeriesId, TableId},
};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum ColumnBlockKind {
	Table = 0,
	Series = 1,
}

impl From<ColumnBlockKind> for u8 {
	fn from(kind: ColumnBlockKind) -> Self {
		kind as u8
	}
}

impl TryFrom<u8> for ColumnBlockKind {
	type Error = Error;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(Self::Table),
			1 => Ok(Self::Series),
			_ => Err(de::Error::custom(format!("invalid ColumnBlockKind value: {value}"))),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnBlockSource {
	Table {
		table_id: TableId,
		commit_version: CommitVersion,
	},
	Series {
		series_id: SeriesId,
		partition: Option<Partition>,
		start: u64,
		end: u64,
	},
}

impl ColumnBlockSource {
	pub fn kind(&self) -> ColumnBlockKind {
		match self {
			Self::Table {
				..
			} => ColumnBlockKind::Table,
			Self::Series {
				..
			} => ColumnBlockKind::Series,
		}
	}

	pub fn read_version(&self) -> CommitVersion {
		match self {
			Self::Table {
				commit_version,
				..
			} => *commit_version,
			Self::Series {
				..
			} => panic!("a series block has no read version, only table blocks are versioned copies"),
		}
	}

	pub fn series_range(&self) -> Option<(u64, u64)> {
		match self {
			Self::Series {
				start,
				end,
				..
			} => Some((*start, *end)),
			Self::Table {
				..
			} => None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnStats {
	pub column: String,
	pub min: Option<Value>,
	pub max: Option<Value>,
	pub none_count: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnBlock {
	pub id: ColumnBlockId,
	pub namespace: NamespaceId,
	pub source: ColumnBlockSource,
	pub row_count: u64,
	pub partition_values: Vec<Value>,
	pub stats: Vec<ColumnStats>,
}

impl ColumnBlock {
	pub fn kind(&self) -> ColumnBlockKind {
		self.source.kind()
	}

	pub fn read_version(&self) -> CommitVersion {
		self.source.read_version()
	}
}
