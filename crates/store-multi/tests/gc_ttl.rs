// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, error::Error as StdError, fmt::Write, path::Path};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::EntryKind,
	},
	key::row::RowKey,
	util::encoding::format::raw::Raw,
};
use reifydb_store_multi::{
	gc::row::{
		ScanStats,
		scanner::{
			ScanResult::{Exhausted, Yielded},
			drop_expired_keys, scan_shape_expired,
		},
	},
	tier::{RangeCursor, TierStorage, commit::buffer::MultiCommitBufferTier},
};
use reifydb_testing::{
	tempdir::temp_dir,
	testscript,
	testscript::{
		command::{ArgumentConsumer, Command},
		runner::run_path,
	},
};
use reifydb_value::util::cowvec::CowVec;
use test_each_file::test_each_path;

test_each_path! { in "crates/store-multi/tests/scripts/buffer/ttl" as buffer_ttl_memory => test_memory }
test_each_path! { in "crates/store-multi/tests/scripts/buffer/ttl" as buffer_ttl_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	let storage = MultiCommitBufferTier::memory();
	run_path(&mut Runner::new(storage), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|_db_path| {
		let storage = MultiCommitBufferTier::memory();
		run_path(&mut Runner::new(storage), path)
	})
	.expect("test failed")
}

pub struct Runner {
	storage: MultiCommitBufferTier,
	shape: ShapeId,
}

impl Runner {
	fn new(storage: MultiCommitBufferTier) -> Self {
		Self {
			storage,
			shape: ShapeId::Table(TableId(1)),
		}
	}

	fn table(&self) -> EntryKind {
		EntryKind::Source(self.shape)
	}

	fn row_key(&self, row_number: u64) -> EncodedKey {
		RowKey::encoded(self.shape, row_number)
	}
}

fn parse_u64(args: &mut ArgumentConsumer, name: &str) -> Result<Option<u64>, Box<dyn StdError>> {
	match args.lookup(name) {
		None => Ok(None),
		Some(arg) => Ok(Some(arg.value.parse::<u64>()?)),
	}
}

fn build_row(payload: &str) -> CowVec<u8> {
	CowVec::new(payload.as_bytes().to_vec())
}

impl testscript::runner::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			"use_shape" => {
				let mut args = command.consume_args();
				let id = args.next_pos().ok_or("shape id not given")?.value.parse::<u64>()?;
				args.reject_rest()?;
				self.shape = ShapeId::Table(TableId(id));
				writeln!(output, "ok")?;
			}

			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("row=value not given")?.clone();
				let row_number: u64 = kv.key.unwrap().parse()?;
				let payload = kv.value;
				let version = parse_u64(&mut args, "version")?.ok_or("version=N required")?;
				args.reject_rest()?;

				let key = self.row_key(row_number);
				let value = build_row(&payload);
				self.storage.set(
					CommitVersion(version),
					HashMap::from([(self.table(), vec![(key, Some(value))])]),
				)?;
				writeln!(output, "ok")?;
			}

			"tombstone" => {
				let mut args = command.consume_args();
				let row_number: u64 = args.next_pos().ok_or("row not given")?.value.parse()?;
				let version = parse_u64(&mut args, "version")?.ok_or("version=N required")?;
				args.reject_rest()?;

				let key = self.row_key(row_number);
				self.storage.set(
					CommitVersion(version),
					HashMap::from([(self.table(), vec![(key, None)])]),
				)?;
				writeln!(output, "ok")?;
			}

			"drop_version" => {
				let mut args = command.consume_args();
				let row_number: u64 = args.next_pos().ok_or("row not given")?.value.parse()?;
				let version = parse_u64(&mut args, "version")?.ok_or("version=N required")?;
				args.reject_rest()?;

				let key = self.row_key(row_number);
				self.storage
					.drop(HashMap::from([(self.table(), vec![(key, CommitVersion(version))])]))?;
				writeln!(output, "ok")?;
			}

			"scan_ttl" => {
				let mut args = command.consume_args();
				let cutoff = parse_u64(&mut args, "cutoff")?.ok_or("cutoff=N required")?;
				let batch_size = parse_u64(&mut args, "batch")?.unwrap_or(1024) as usize;
				args.reject_rest()?;

				let cutoff_version = CommitVersion(cutoff);
				let mut cursor = RangeCursor::new();
				let mut total_expired: u64 = 0;
				let mut stats = ScanStats::default();

				loop {
					let (expired, result) = scan_shape_expired(
						&self.storage,
						self.table(),
						cutoff_version,
						batch_size,
						&mut cursor,
					)?;

					total_expired += expired.len() as u64;
					if !expired.is_empty() {
						drop_expired_keys(&self.storage, &expired, &mut stats)?;
					}

					match result {
						Yielded => continue,
						Exhausted => break,
					}
				}

				writeln!(
					output,
					"expired={} versions_dropped={}",
					total_expired, stats.versions_dropped
				)?;
			}

			"count_current" => {
				let args = command.consume_args();
				args.reject_rest()?;
				let count = self.storage.count_current(self.table())?;
				writeln!(output, "current: {}", count)?;
			}

			"count_historical" => {
				let args = command.consume_args();
				args.reject_rest()?;
				let count = self.storage.count_historical(self.table())?;
				writeln!(output, "historical: {}", count)?;
			}

			"versions" => {
				let mut args = command.consume_args();
				let row_number: u64 = args.next_pos().ok_or("row not given")?.value.parse()?;
				args.reject_rest()?;

				let key = self.row_key(row_number);
				let versions = self.storage.get_all_versions(self.table(), &key)?;
				if versions.is_empty() {
					writeln!(output, "(none)")?;
				}
				for (v, value) in versions {
					match value {
						Some(bytes) => {
							writeln!(output, "v{} value={}", v.0, Raw::bytes(&bytes[..]))?;
						}
						None => {
							writeln!(output, "v{} tombstone", v.0)?;
						}
					}
				}
			}

			"visible" => {
				let mut args = command.consume_args();
				let row_number: u64 = args.next_pos().ok_or("row not given")?.value.parse()?;
				args.reject_rest()?;

				let key = self.row_key(row_number);
				let value = self.storage.get(self.table(), &key, CommitVersion(u64::MAX))?.value();
				match value {
					Some(bytes) => {
						writeln!(output, "value={}", Raw::bytes(&bytes[..]))?;
					}
					None => {
						writeln!(output, "(none)")?;
					}
				}
			}

			name => return Err(format!("invalid command {name}").into()),
		}
		Ok(output)
	}
}
