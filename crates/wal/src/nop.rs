// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::atomic::{AtomicU64, Ordering};

use reifydb_codec::log::{LogIndex, LogOffset, LogVersion, record::Record};
use reifydb_value::byte_size::ByteSize;

use crate::{
	device::{Append, Device, Flush, Mark, ReadFrom, Reclaim, RecordCursor},
	error::WalError,
};

#[derive(Default)]
pub struct NopDevice {
	last: AtomicU64,
}

impl NopDevice {
	pub fn new() -> Self {
		Self::default()
	}

	fn last(&self) -> Option<LogVersion> {
		match self.last.load(Ordering::Acquire) {
			0 => None,
			last => Some(LogVersion::new(last)),
		}
	}
}

pub struct NopCursor;

impl RecordCursor for NopCursor {
	fn next_batch(&mut self, _max: usize) -> Result<Vec<Record>, WalError> {
		Ok(Vec::new())
	}
}

impl Device for NopDevice {}

impl Append for NopDevice {
	fn append(&self, record: &Record) -> Result<LogOffset, WalError> {
		self.last.store(record.version.as_u64(), Ordering::Release);
		Ok(LogOffset::ZERO)
	}

	fn head(&self) -> Result<Option<LogVersion>, WalError> {
		Ok(self.last())
	}
}

impl Flush for NopDevice {
	fn flush(&self) -> Result<(), WalError> {
		Ok(())
	}

	fn durable(&self) -> Result<Option<LogVersion>, WalError> {
		Ok(self.last())
	}
}

impl ReadFrom for NopDevice {
	type Cursor<'a>
		= NopCursor
	where
		Self: 'a;

	fn read_from(&self, _from: LogVersion) -> Result<NopCursor, WalError> {
		Ok(NopCursor)
	}
}

impl Reclaim for NopDevice {
	fn start(&self) -> Result<Option<LogVersion>, WalError> {
		Ok(None)
	}

	fn committed(&self) -> Result<LogIndex, WalError> {
		Ok(LogIndex::ZERO)
	}

	fn drop_below(&self, _index: LogIndex) -> Result<(), WalError> {
		Ok(())
	}

	fn bytes(&self) -> Result<ByteSize, WalError> {
		Ok(ByteSize::ZERO)
	}
}

impl Mark for NopDevice {
	fn register(&self, _id: &str) -> Result<(), WalError> {
		Ok(())
	}

	fn record(&self, _id: &str, _version: LogVersion) -> Result<(), WalError> {
		Ok(())
	}

	fn readers(&self) -> Result<Vec<(String, LogVersion)>, WalError> {
		Ok(Vec::new())
	}
}
