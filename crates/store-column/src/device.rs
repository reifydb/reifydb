// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::column_snapshot::{ColumnSnapshot, ColumnSnapshotSource};
use reifydb_runtime::io::fs::{Len, Pread};
use reifydb_value::Result;

use crate::{persist::BlockHandle, snapshot::ColumnBlock};

pub trait Device: Send + Sync + 'static {}

pub trait WriteBlock: Device {
	fn write(&self, key: &BlockKey, block: &ColumnBlock) -> Result<()>;
}

pub trait OpenBlock: Device {
	type File: Pread + Len;

	fn open(&self, key: &BlockKey) -> Result<Option<BlockHandle<Self::File>>>;
}

pub trait RemoveBlock: Device {
	fn remove(&self, key: &BlockKey) -> Result<()>;
}

pub struct BlockKey {
	pub dir: u64,
	pub name: u64,
}

impl BlockKey {
	pub fn of(snapshot: &ColumnSnapshot) -> Self {
		match snapshot.source {
			ColumnSnapshotSource::Table {
				table_id,
				..
			} => Self {
				dir: table_id.0,
				name: snapshot.id.0,
			},
			ColumnSnapshotSource::SeriesBucket {
				series_id,
				..
			} => Self {
				dir: series_id.0,
				name: snapshot.id.0,
			},
		}
	}
}
