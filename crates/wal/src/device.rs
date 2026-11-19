// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::log::{LogIndex, LogOffset, LogVersion, record::Record};
use reifydb_value::byte_size::ByteSize;

use crate::error::WalError;

pub trait Device: Send + Sync + 'static {}

pub trait Append: Device {
	fn append(&self, record: &Record) -> Result<LogOffset, WalError>;

	fn head(&self) -> Result<Option<LogVersion>, WalError>;
}

pub trait Flush: Device {
	fn flush(&self) -> Result<(), WalError>;

	fn durable(&self) -> Result<Option<LogVersion>, WalError>;
}

pub trait RecordCursor {
	fn next_batch(&mut self, max: usize) -> Result<Vec<Record>, WalError>;
}

pub trait ReadFrom: Device {
	type Cursor<'a>: RecordCursor
	where
		Self: 'a;

	fn read_from(&self, from: LogVersion) -> Result<Self::Cursor<'_>, WalError>;
}

pub trait Reclaim: Device {
	fn start(&self) -> Result<Option<LogVersion>, WalError>;

	fn committed(&self) -> Result<LogIndex, WalError>;

	fn drop_below(&self, index: LogIndex) -> Result<(), WalError>;

	fn bytes(&self) -> Result<ByteSize, WalError>;
}

pub trait Mark: Device {
	fn register(&self, id: &str) -> Result<(), WalError>;

	fn record(&self, id: &str, version: LogVersion) -> Result<(), WalError>;

	fn readers(&self) -> Result<Vec<(String, LogVersion)>, WalError>;
}
