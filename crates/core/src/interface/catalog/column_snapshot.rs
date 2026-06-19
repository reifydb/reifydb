// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::error::Error;
use serde::{Deserialize, Serialize, de};

use crate::{
	common::CommitVersion,
	interface::catalog::id::{ColumnSnapshotId, NamespaceId, SeriesId, TableId},
};

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub enum ColumnSnapshotKind {
	Table = 0,
	SeriesBucket = 1,
}

impl From<ColumnSnapshotKind> for u8 {
	fn from(kind: ColumnSnapshotKind) -> Self {
		kind as u8
	}
}

impl TryFrom<u8> for ColumnSnapshotKind {
	type Error = Error;

	fn try_from(value: u8) -> Result<Self, Self::Error> {
		match value {
			0 => Ok(Self::Table),
			1 => Ok(Self::SeriesBucket),
			_ => Err(de::Error::custom(format!("invalid ColumnSnapshotKind value: {value}"))),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ColumnSnapshotSource {
	Table {
		table_id: TableId,
		commit_version: CommitVersion,
	},
	SeriesBucket {
		series_id: SeriesId,
		bucket_start: u64,
		bucket_width: u64,
		sequence_counter: u64,
		sealed_at_commit_version: CommitVersion,
	},
}

impl ColumnSnapshotSource {
	pub fn kind(&self) -> ColumnSnapshotKind {
		match self {
			Self::Table {
				..
			} => ColumnSnapshotKind::Table,
			Self::SeriesBucket {
				..
			} => ColumnSnapshotKind::SeriesBucket,
		}
	}

	pub fn read_version(&self) -> CommitVersion {
		match self {
			Self::Table {
				commit_version,
				..
			} => *commit_version,
			Self::SeriesBucket {
				sealed_at_commit_version,
				..
			} => *sealed_at_commit_version,
		}
	}

	pub fn series_bucket_range(&self) -> Option<(u64, u64)> {
		match self {
			Self::SeriesBucket {
				bucket_start,
				bucket_width,
				..
			} => Some((*bucket_start, *bucket_start + *bucket_width)),
			Self::Table {
				..
			} => None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnSnapshot {
	pub id: ColumnSnapshotId,
	pub namespace: NamespaceId,
	pub source: ColumnSnapshotSource,
	pub row_count: u64,
}

impl ColumnSnapshot {
	pub fn kind(&self) -> ColumnSnapshotKind {
		self.source.kind()
	}

	pub fn read_version(&self) -> CommitVersion {
		self.source.read_version()
	}
}
