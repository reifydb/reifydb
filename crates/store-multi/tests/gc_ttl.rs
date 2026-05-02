// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, error::Error as StdError, fmt::Write, path::Path};

use reifydb_core::{
	common::CommitVersion,
	encoded::row::{EncodedRow, SHAPE_HEADER_SIZE},
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::EntryKind,
	},
	key::row::RowKey,
	row::{RowTtl, RowTtlAnchor, RowTtlCleanupMode},
	util::encoding::format::raw::Raw,
};
use reifydb_store_multi::{
	gc::ttl::{
		ScanStats,
		scanner::{
			ScanResult::{Exhausted, Yielded},
			drop_expired_keys, scan_shape_by_created_at, scan_shape_by_updated_at,
		},
	},
	hot::storage::HotStorage,
	tier::{RangeCursor, TierStorage},
};
use reifydb_testing::{
	tempdir::temp_dir,
	testscript,
	testscript::{
		command::{ArgumentConsumer, Command},
		runner::run_path,
	},
};
use reifydb_type::util::cowvec::CowVec;
use test_each_file::test_each_path;

test_each_path! { in "crates/store-multi/tests/scripts/hot/ttl" as hot_ttl_memory => test_memory }
test_each_path! { in "crates/store-multi/tests/scripts/hot/ttl" as hot_ttl_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	let storage = HotStorage::memory();
	run_path(&mut Runner::new(storage), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|_db_path| {
		let storage = HotStorage::sqlite_in_memory();
		run_path(&mut Runner::new(storage), path)
	})
	.expect("test failed")
}

pub struct Runner {
	storage: HotStorage,
	shape: ShapeId,
}

impl Runner {
	fn new(storage: HotStorage) -> Self {
		Self {
			storage,
			shape: ShapeId::Table(TableId(1)),
		}
	}

	fn table(&self) -> EntryKind {
		EntryKind::Source(self.shape)
	}

	fn row_key(&self, row_number: u64) -> CowVec<u8> {
		let encoded = RowKey::encoded(self.shape, row_number);
		CowVec::new(encoded.as_slice().to_vec())
	}
}

fn parse_u64(args: &mut ArgumentConsumer, name: &str) -> Result<Option<u64>, Box<dyn StdError>> {
	match args.lookup(name) {
		None => Ok(None),
		Some(arg) => Ok(Some(arg.value.parse::<u64>()?)),
	}
}

fn build_row(payload: &str, created_at: u64, updated_at: u64) -> CowVec<u8> {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[8..16].copy_from_slice(&created_at.to_le_bytes());
	buf[16..24].copy_from_slice(&updated_at.to_le_bytes());
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload.as_bytes());
	CowVec::new(buf)
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
				let created = parse_u64(&mut args, "created")?.unwrap_or(0);
				let updated = parse_u64(&mut args, "updated")?.unwrap_or(created);
				args.reject_rest()?;

				let key = self.row_key(row_number);
				let value = build_row(&payload, created, updated);
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
				let mode = args.lookup("mode").ok_or("mode=created|updated required")?.value.clone();
				let duration_nanos = parse_u64(&mut args, "duration")?.ok_or("duration=N required")?;
				let now_nanos = parse_u64(&mut args, "now")?.ok_or("now=N required")?;
				let batch_size = parse_u64(&mut args, "batch")?.unwrap_or(1024) as usize;
				args.reject_rest()?;

				let ttl = RowTtl {
					duration_nanos,
					anchor: match mode.as_str() {
						"created" => RowTtlAnchor::Created,
						"updated" => RowTtlAnchor::Updated,
						other => return Err(format!("unknown mode: {}", other).into()),
					},
					cleanup_mode: RowTtlCleanupMode::Drop,
				};

				let mut cursor = RangeCursor::new();
				let mut total_expired: u64 = 0;
				let mut stats = ScanStats::default();

				loop {
					let (expired, result) = match ttl.anchor {
						RowTtlAnchor::Created => scan_shape_by_created_at(
							&self.storage,
							self.shape,
							&ttl,
							now_nanos,
							batch_size,
							&mut cursor,
						)?,
						RowTtlAnchor::Updated => scan_shape_by_updated_at(
							&self.storage,
							self.shape,
							&ttl,
							now_nanos,
							batch_size,
							&mut cursor,
						)?,
					};

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
							let row = EncodedRow(bytes.clone());
							let payload = &bytes[SHAPE_HEADER_SIZE..];
							writeln!(
								output,
								"v{} created={} updated={} value={}",
								v.0,
								row.created_at_nanos(),
								row.updated_at_nanos(),
								Raw::bytes(payload),
							)?;
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
				let value = self.storage.get(self.table(), &key, CommitVersion(u64::MAX))?;
				match value {
					Some(bytes) => {
						let row = EncodedRow(bytes.clone());
						let payload = &bytes[SHAPE_HEADER_SIZE..];
						writeln!(
							output,
							"created={} updated={} value={}",
							row.created_at_nanos(),
							row.updated_at_nanos(),
							Raw::bytes(payload),
						)?;
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
