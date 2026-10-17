// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::path::Path;

use reifydb_codec::log::record::HEADER_BYTES;
use reifydb_core::wal::CommitRecord;
use reifydb_runtime::{
	context::clock::Clock,
	io::fs::{Fs, ReadDir, memory::MemoryFs},
};
use reifydb_store_log::{error::LogError, log::Log, partition::Config};
use reifydb_value::{Result, byte_size::ByteSize};
use reifydb_wal::{error::Result as WalResult, wal::Wal};

use crate::error::TransactionError;

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
