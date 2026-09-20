// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::log::{LogIndex, LogVersion, Position, record::Record};
use reifydb_value::byte_size::ByteSize;

use crate::error::Result;

pub trait Device: Send + Sync + 'static {}

pub trait Append: Device {
	fn append(&self, record: &Record) -> Result<Position>;

	fn head(&self) -> Result<Option<LogVersion>>;
}

pub trait Flush: Device {
	fn flush(&self) -> Result<()>;

	fn durable(&self) -> Result<Option<LogVersion>>;
}

pub trait RecordCursor {
	fn next_batch(&mut self, max: usize) -> Result<Vec<Record>>;
}

pub trait ReadFrom: Device {
	type Cursor<'a>: RecordCursor
	where
		Self: 'a;

	fn read_from(&self, from: LogVersion) -> Result<Self::Cursor<'_>>;
}

pub trait Reclaim: Device {
	fn start(&self) -> Result<Option<LogVersion>>;

	fn drop_below(&self, index: LogIndex) -> Result<()>;

	fn bytes(&self) -> Result<ByteSize>;
}

pub trait Mark: Device {
	fn register(&self, id: &str) -> Result<()>;

	fn record(&self, id: &str, version: LogVersion) -> Result<()>;

	fn readers(&self) -> Result<Vec<(String, LogVersion)>>;
}
