// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::log::{LogIndex, LogVersion, Position, record::Record};
use reifydb_runtime::io::fs::{Create, Filesystem, Mkdir, Open, OpenMut, ReadDir, Rename, SyncDir, Unlink};
use reifydb_store_log::{cursor::Cursor, log::Log, partition::Partition};
use reifydb_value::{byte_size::ByteSize, clock::ClockNow};

use crate::{
	device::{Append, Device, Flush, Mark, ReadFrom, Reclaim, RecordCursor},
	error::Result,
	lsn::Lsn,
};

impl<F, C> Device for Log<F, C>
where
	F: Filesystem
		+ Create
		+ Mkdir
		+ Open
		+ OpenMut
		+ ReadDir
		+ Rename
		+ SyncDir
		+ Unlink
		+ Clone
		+ Send
		+ Sync
		+ 'static,
	C: ClockNow + Clone + Send + 'static,
	Partition<F, C>: Send,
{
}

impl<F, C> Append for Log<F, C>
where
	F: Filesystem
		+ Create
		+ Mkdir
		+ Open
		+ OpenMut
		+ ReadDir
		+ Rename
		+ SyncDir
		+ Unlink
		+ Clone
		+ Send
		+ Sync
		+ 'static,
	C: ClockNow + Clone + Send + 'static,
	Partition<F, C>: Send,
{
	fn append(&self, record: &Record) -> Result<Position> {
		Ok(self.append(0, record)?)
	}

	fn head(&self) -> Result<Option<LogVersion>> {
		Ok(self.with(0, |partition| partition.head())?)
	}
}

impl<F, C> Flush for Log<F, C>
where
	F: Filesystem
		+ Create
		+ Mkdir
		+ Open
		+ OpenMut
		+ ReadDir
		+ Rename
		+ SyncDir
		+ Unlink
		+ Clone
		+ Send
		+ Sync
		+ 'static,
	C: ClockNow + Clone + Send + 'static,
	Partition<F, C>: Send,
{
	fn flush(&self) -> Result<()> {
		Ok(self.flush(0)?)
	}

	fn durable(&self) -> Result<Option<LogVersion>> {
		Ok(self.durable(0)?)
	}
}

impl<F: Filesystem + Open + ReadDir> RecordCursor for Cursor<'_, F> {
	fn next_batch(&mut self, max: usize) -> Result<Vec<Record>> {
		Ok(Cursor::next_batch(self, max)?)
	}
}

impl<F, C> ReadFrom for Log<F, C>
where
	F: Filesystem
		+ Create
		+ Mkdir
		+ Open
		+ OpenMut
		+ ReadDir
		+ Rename
		+ SyncDir
		+ Unlink
		+ Clone
		+ Send
		+ Sync
		+ 'static,
	C: ClockNow + Clone + Send + 'static,
	Partition<F, C>: Send,
{
	type Cursor<'a>
		= Cursor<'a, F>
	where
		Self: 'a;

	fn read_from(&self, from: LogVersion) -> Result<Cursor<'_, F>> {
		Ok(self.read_from(0, from)?)
	}
}

impl<F, C> Reclaim for Log<F, C>
where
	F: Filesystem
		+ Create
		+ Mkdir
		+ Open
		+ OpenMut
		+ ReadDir
		+ Rename
		+ SyncDir
		+ Unlink
		+ Clone
		+ Send
		+ Sync
		+ 'static,
	C: ClockNow + Clone + Send + 'static,
	Partition<F, C>: Send,
{
	fn start(&self) -> Result<Option<LogVersion>> {
		let Some(base) = self.bases(0)?.first().copied() else {
			return Ok(None);
		};
		if base != LogVersion::ZERO {
			return Ok(Some(base));
		}
		let head = self.with(0, |partition| partition.head())?;
		Ok(head.map(|_| Lsn::FIRST.into()))
	}

	fn committed(&self) -> Result<LogIndex> {
		Ok(self.commit_index(0)?)
	}

	fn drop_below(&self, index: LogIndex) -> Result<()> {
		let _dropped = self.drop_below(0, index)?;
		Ok(())
	}

	fn bytes(&self) -> Result<ByteSize> {
		Ok(self.bytes(0)?)
	}
}

impl<F, C> Mark for Log<F, C>
where
	F: Filesystem
		+ Create
		+ Mkdir
		+ Open
		+ OpenMut
		+ ReadDir
		+ Rename
		+ SyncDir
		+ Unlink
		+ Clone
		+ Send
		+ Sync
		+ 'static,
	C: ClockNow + Clone + Send + 'static,
	Partition<F, C>: Send,
{
	fn register(&self, id: &str) -> Result<()> {
		Ok(self.register(0, id)?)
	}

	fn record(&self, id: &str, version: LogVersion) -> Result<()> {
		Ok(self.record(0, id, version)?)
	}

	fn readers(&self) -> Result<Vec<(String, LogVersion)>> {
		Ok(self.readers(0)?)
	}
}
