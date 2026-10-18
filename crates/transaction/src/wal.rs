// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::VecDeque,
	path::Path,
	sync::{Arc, Weak},
};

use reifydb_codec::log::record::HEADER_BYTES;
use reifydb_core::{
	common::CommitVersion,
	interface::store::{SingleWalCheckpoint, WalCheckpoint},
	wal::WalRecord,
};
use reifydb_runtime::{
	context::clock::Clock,
	io::fs::{Fs, ReadDir, memory::MemoryFs},
};
use reifydb_store_commit::store::CommitStore;
use reifydb_store_log::{error::LogError, log::Log, partition::Config};
use reifydb_store_single::SingleStore;
use reifydb_store_multi::store::multi::classify_deltas;
use reifydb_value::{Result, byte_size::ByteSize, reifydb_assertions, util::cowvec::CowVec};
use reifydb_wal::{
	error::{Result as WalResult, WalError},
	floor::Floor,
	lsn::Lsn,
	wal::Wal,
};

use crate::{
	error::TransactionError,
	multi::{
		oracle::{CommitShared, note_lsn},
		transaction::version::{StandardVersionProvider, VersionProvider},
	},
};

pub type CommitWal = Wal<WalRecord, Log<Fs, Clock>>;

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
	Single,
}

impl CheckpointedStore {
	pub(crate) fn floor_name(self) -> &'static str {
		match self {
			Self::Multi => "multi",
			Self::Single => "single",
		}
	}
}

pub(crate) struct StoreCheckpoint {
	shared: Weak<CommitShared<StandardVersionProvider>>,
	floor: Floor<WalRecord, Log<Fs, Clock>>,
}

impl StoreCheckpoint {
	pub(crate) fn new(
		shared: Weak<CommitShared<StandardVersionProvider>>,
		floor: Floor<WalRecord, Log<Fs, Clock>>,
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

pub struct Recovery {
	pub last: CommitVersion,
	pub lsns: VecDeque<(CommitVersion, Lsn)>,
}

pub fn replay(wal: &CommitWal, store: &CommitStore, saved_through: CommitVersion) -> Recovery {
	let floor_name = CheckpointedStore::Multi.floor_name();
	let cursors = wal.cursors().unwrap_or_else(|err| panic!("commit log cursors could not be read at boot: {err}"));
	let from = cursors
		.floors
		.into_iter()
		.find(|(name, _)| name == floor_name)
		.and_then(|(_, lsn)| lsn)
		.map_or(Lsn::FIRST, |lsn| Lsn::new(lsn.as_u64() + 1));
	let mut last = saved_through;
	let mut previous: Option<CommitVersion> = None;
	let mut lsns = VecDeque::new();
	let records = wal
		.replay(from)
		.unwrap_or_else(|err| panic!("commit log replay from lsn {} failed to start: {err}", from.as_u64()));
	for replayed in records {
		let replayed = replayed
			.unwrap_or_else(|err| panic!("commit log replay failed after lsn {}: {err}", from.as_u64()));
		let WalRecord::Multi(record) = replayed.body else {
			continue;
		};
		let version = record.version;
		reifydb_assertions! {
			assert!(
				previous.is_none_or(|previous| version > previous),
				"commit log replay met version {} after version {:?} at lsn {}, so two records claim one \
				 version or a record is out of order",
				version.0,
				previous,
				replayed.lsn.as_u64()
			);
		}
		previous = Some(version);
		last = last.max(version);
		if version > saved_through {
			store.set(version, classify_deltas(&CowVec::new(record.deltas)).batches);
			note_lsn(&mut lsns, version, replayed.lsn);
		}
	}
	Recovery {
		last,
		lsns,
	}
}

pub fn replay_single(wal: &CommitWal, store: &SingleStore, saved_lsn: Option<Lsn>) {
	let cursors = wal.cursors().unwrap_or_else(|err| panic!("commit log cursors could not be read at boot: {err}"));
	let from = saved_lsn.map_or(cursors.start.unwrap_or(Lsn::FIRST), |lsn| Lsn::new(lsn.as_u64() + 1));
	let records = wal.replay(from).unwrap_or_else(|err| match err {
		WalError::Purged {
			oldest,
			..
		} => panic!(
			"single.db is saved through lsn {} but the commit log starts at lsn {}: gc dropped single \
			 records single.db never received",
			from.as_u64() - 1,
			oldest.as_u64()
		),
		err => panic!("commit log replay of single records from lsn {} failed to start: {err}", from.as_u64()),
	});
	for replayed in records {
		let replayed = replayed.unwrap_or_else(|err| {
			panic!("commit log replay of single records failed after lsn {}: {err}", from.as_u64())
		});
		let WalRecord::Single(record) = replayed.body else {
			continue;
		};
		store.commit_logged(CowVec::new(record.deltas), replayed.lsn).unwrap_or_else(|err| {
			panic!(
				"single record at lsn {} is in the commit log but failed to install in the store at boot: {err}",
				replayed.lsn.as_u64()
			)
		});
	}
}

pub fn single_checkpoint(wal: &CommitWal) -> Arc<dyn SingleWalCheckpoint> {
	let name = CheckpointedStore::Single.floor_name();
	let floor = wal.floor(name).unwrap_or_else(|err| panic!("commit log floor {name} could not be taken: {err}"));
	Arc::new(SingleStoreCheckpoint {
		wal: wal.clone(),
		floor,
	})
}

struct SingleStoreCheckpoint {
	wal: CommitWal,
	floor: Floor<WalRecord, Log<Fs, Clock>>,
}

impl SingleWalCheckpoint for SingleStoreCheckpoint {
	fn make_durable(&self, through: Lsn) -> Result<()> {
		self.wal.sync().unwrap_or_else(|err| {
			panic!(
				"commit log failed to flush for the {} checkpoint at lsn {}: {err}",
				self.floor.name(),
				through.as_u64()
			)
		});
		Ok(())
	}

	fn saved_through(&self, through: Lsn) -> Result<()> {
		self.floor.advance(through).unwrap_or_else(|err| {
			panic!("commit log floor {} failed to advance to lsn {}: {err}", self.floor.name(), through.as_u64())
		});
		self.wal.gc().unwrap_or_else(|err| {
			panic!(
				"commit log gc failed after floor {} reached lsn {}: {err}",
				self.floor.name(),
				through.as_u64()
			)
		});
		Ok(())
	}
}
