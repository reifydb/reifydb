// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc as StdArc,
	atomic::{AtomicU64, Ordering},
};

use reifydb_codec::log::{LogIndex, LogVersion, Position, record::Record};
use reifydb_runtime::{
	io::fs::{Create, Filesystem, Mkdir, Open, OpenMut, ReadDir, Rename, SyncDir, Unlink},
	sync::Arc,
};
use reifydb_store_log::{cursor::Cursor, log::Log, partition::Partition};
use reifydb_value::{byte_size::ByteSize, clock::ClockNow};

use crate::{
	device::{Append, Device, Flush, Mark, ReadFrom, Reclaim},
	error::{Result, WalError},
	lsn::Lsn,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Continue {
	Yes,
	Crash,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AppendOutcome {
	Land,
	Err(WalError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlushOutcome {
	Land,
	Err(WalError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GcStage {
	FloorsRead,
	Dropped,
}

pub trait WalHooks: Send + Sync {
	fn on_append(&self, _lsn: Lsn, _bytes: usize) -> AppendOutcome {
		AppendOutcome::Land
	}

	fn on_flush(&self, _target: Option<Lsn>) -> FlushOutcome {
		FlushOutcome::Land
	}

	fn during_flush(&self, _target: Option<Lsn>) {}

	fn during_gc(&self, _stage: GcStage) {}

	fn on_call(&self, _call: u64) -> Continue {
		Continue::Yes
	}
}

pub struct NoFaults;

impl WalHooks for NoFaults {}

struct Inner<F: Filesystem, C: ClockNow> {
	log: Log<F, C>,
	hooks: StdArc<dyn WalHooks>,
	calls: AtomicU64,
}

pub struct TestingLog<F: Filesystem, C: ClockNow>(Arc<Inner<F, C>>);

impl<F: Filesystem, C: ClockNow> Clone for TestingLog<F, C> {
	fn clone(&self) -> Self {
		Self(Arc::clone(&self.0))
	}
}

impl<F: Filesystem, C: ClockNow> TestingLog<F, C> {
	pub fn over(log: Log<F, C>, hooks: StdArc<dyn WalHooks>) -> Self {
		Self(Arc::new(Inner {
			log,
			hooks,
			calls: AtomicU64::new(0),
		}))
	}

	pub fn log(&self) -> &Log<F, C> {
		&self.0.log
	}

	pub fn calls(&self) -> u64 {
		self.0.calls.load(Ordering::SeqCst)
	}

	fn call(&self) {
		let call = self.0.calls.fetch_add(1, Ordering::SeqCst) + 1;
		if self.0.hooks.on_call(call) == Continue::Crash {
			panic!("simulated crash at wal call {call}");
		}
	}
}

impl<F, C> Device for TestingLog<F, C>
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

impl<F, C> Append for TestingLog<F, C>
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
		self.call();
		let lsn = Lsn::from_version(record.version).unwrap_or(Lsn::FIRST);
		match self.0.hooks.on_append(lsn, record.payload.len()) {
			AppendOutcome::Land => Append::append(&self.0.log, record),
			AppendOutcome::Err(error) => Err(error),
		}
	}

	fn head(&self) -> Result<Option<LogVersion>> {
		Append::head(&self.0.log)
	}
}

impl<F, C> Flush for TestingLog<F, C>
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
		self.call();
		let target = Append::head(&self.0.log)?.and_then(Lsn::from_version);
		if let FlushOutcome::Err(error) = self.0.hooks.on_flush(target) {
			return Err(error);
		}
		self.0.hooks.during_flush(target);
		Flush::flush(&self.0.log)
	}

	fn durable(&self) -> Result<Option<LogVersion>> {
		Flush::durable(&self.0.log)
	}
}

impl<F, C> ReadFrom for TestingLog<F, C>
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
		ReadFrom::read_from(&self.0.log, from)
	}
}

impl<F, C> Reclaim for TestingLog<F, C>
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
		Reclaim::start(&self.0.log)
	}

	fn committed(&self) -> Result<LogIndex> {
		Reclaim::committed(&self.0.log)
	}

	fn drop_below(&self, index: LogIndex) -> Result<()> {
		self.call();
		self.0.hooks.during_gc(GcStage::FloorsRead);
		let dropped = Reclaim::drop_below(&self.0.log, index);
		self.0.hooks.during_gc(GcStage::Dropped);
		dropped
	}

	fn bytes(&self) -> Result<ByteSize> {
		Reclaim::bytes(&self.0.log)
	}
}

impl<F, C> Mark for TestingLog<F, C>
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
		self.call();
		Mark::register(&self.0.log, id)
	}

	fn record(&self, id: &str, version: LogVersion) -> Result<()> {
		self.call();
		Mark::record(&self.0.log, id, version)
	}

	fn readers(&self) -> Result<Vec<(String, LogVersion)>> {
		Mark::readers(&self.0.log)
	}
}
