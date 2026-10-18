// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::column_block::{ColumnBlock, ColumnBlockSource};
use reifydb_runtime::io::fs::{Len, Pread};
use reifydb_value::Result;

use crate::{block::BlockData, persist::BlockHandle};

pub trait Device: Send + Sync + 'static {}

pub trait WriteBlock: Device {
	fn write(&self, key: &BlockFileKey, data: &BlockData) -> Result<()>;
}

pub trait OpenBlock: Device {
	type File: Pread + Len;

	fn open(&self, key: &BlockFileKey) -> Result<Option<BlockHandle<Self::File>>>;
}

pub trait RemoveBlock: Device {
	fn remove(&self, key: &BlockFileKey) -> Result<()>;
}

pub struct BlockFileKey {
	pub dir: u64,
	pub name: u64,
}

impl BlockFileKey {
	pub fn of(block: &ColumnBlock) -> Self {
		match block.source {
			ColumnBlockSource::Table {
				table_id,
				..
			} => Self {
				dir: table_id.0,
				name: block.id.0,
			},
			ColumnBlockSource::SeriesBucket {
				series_id,
				..
			} => Self {
				dir: series_id.0,
				name: block.id.0,
			},
		}
	}
}
