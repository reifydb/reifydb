// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	path::Path,
	sync::{Arc, Weak},
};

use reifydb_codec::log::record::HEADER_BYTES;
use reifydb_core::{common::CommitVersion, interface::store::WalCheckpoint, wal::CommitRecord};
use reifydb_runtime::{
	context::clock::Clock,
	io::fs::{Fs, ReadDir, memory::MemoryFs},
};
use reifydb_store_log::{error::LogError, log::Log, partition::Config};
use reifydb_value::{Result, byte_size::ByteSize};
use reifydb_wal::{error::Result as WalResult, floor::Floor, wal::Wal};

use crate::{
	error::TransactionError,
	multi::{
		oracle::CommitShared,
		transaction::version::{StandardVersionProvider, VersionProvider},
	},
};

pub type CommitWal = Wal<CommitRecord, Log<Fs, Clock>>;

pub(crate) const SEGMENT_BYTES: ByteSize = ByteSize::from_mib(64);

pub fn open_wal(fs: Fs, clock: Clock, dir: &Path) -> WalResult<CommitWal> {
	let parent = dir.parent().filter(|parent| !parent.as_os_str().is_empty()).unwrap_or(Path::new("."));
	if !fs.read_dir(parent).map_err(LogError::from)?.iter().any(|entry| entry == dir) {
		let config = Config {
			segment_bytes: SEGMENT_BYTES,
			..Config::default()
		};
		drop(Log::create_detached(fs.clone(), clock.clone(), dir, config, 1)?);
	}
	let (log, scans) = Log::open_detached(fs, clock, dir)?;
	let (wal, _) = Wal::open(log, scans)?;
	Ok(wal)
}

pub fn memory_wal(clock: Clock) -> WalResult<CommitWal> {
	open_wal(Fs::Memory(MemoryFs::new()), clock, Path::new("/wal"))
}

pub(crate) fn check_record_size(encoded: &[u8]) -> Result<()> {
	let bytes = (HEADER_BYTES + encoded.len()) as u64;
	let limit = SEGMENT_BYTES.as_bytes();
	if bytes > limit {
		return Err(TransactionError::CommitTooLarge {
			bytes,
			limit,
		}
		.into());
	}
	Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckpointedStore {
	Multi,
}

impl CheckpointedStore {
	pub(crate) fn floor_name(self) -> &'static str {
		match self {
			Self::Multi => "multi",
		}
	}
}

pub(crate) struct StoreCheckpoint {
	shared: Weak<CommitShared<StandardVersionProvider>>,
	floor: Floor<CommitRecord, Log<Fs, Clock>>,
}

impl StoreCheckpoint {
	pub(crate) fn new(
		shared: Weak<CommitShared<StandardVersionProvider>>,
		floor: Floor<CommitRecord, Log<Fs, Clock>>,
	) -> Self {
		Self {
			shared,
			floor,
		}
	}

	fn shared(&self) -> Arc<CommitShared<StandardVersionProvider>> {
		self.shared.upgrade().unwrap_or_else(|| {
			panic!("commit log checkpoint for {} outlived its transaction manager", self.floor.name())
		})
	}
}

impl WalCheckpoint for StoreCheckpoint {
	fn make_durable(&self) -> Result<CommitVersion> {
		let shared = self.shared();
		let version = shared.versions.current()?;
		shared.wal.sync().unwrap_or_else(|err| {
			panic!(
				"commit log failed to flush for the {} checkpoint at version {}: {err}",
				self.floor.name(),
				version.0
			)
		});
		Ok(version)
	}

	fn saved_through(&self, version: CommitVersion) -> Result<()> {
		let shared = self.shared();
		let Some(lsn) = shared.inner.write().lsn_through(version) else {
			return Ok(());
		};
		self.floor.advance(lsn).unwrap_or_else(|err| {
			panic!(
				"commit log floor {} failed to advance to lsn {} for version {}: {err}",
				self.floor.name(),
				lsn.as_u64(),
				version.0
			)
		});
		shared.wal.gc().unwrap_or_else(|err| {
			panic!(
				"commit log gc failed after floor {} reached lsn {} for version {}: {err}",
				self.floor.name(),
				lsn.as_u64(),
				version.0
			)
		});
		Ok(())
	}
}
