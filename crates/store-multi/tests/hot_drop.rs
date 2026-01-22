// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, error::Error as StdError, fmt::Write, ops::Bound, path::Path};

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{flow::FlowNodeId, id::TableId, primitive::PrimitiveId},
	runtime::compute::ComputePool,
	util::encoding::{binary::decode_binary, format::raw::Raw},
};
use reifydb_store_multi::{
	hot::storage::HotStorage,
	tier::{EntryKind, RangeCursor, TierStorage},
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

test_each_path! { in "crates/store-multi/tests/scripts/hot/drop" as hot_drop_memory => test_memory }
test_each_path! { in "crates/store-multi/tests/scripts/hot/drop" as hot_drop_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	let compute_pool = ComputePool::new(2, 8);
	let storage = HotStorage::memory(compute_pool);
	run_path(&mut Runner::new(storage), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|_db_path| {
		let storage = HotStorage::sqlite_in_memory();
		run_path(&mut Runner::new(storage), path)
	})
	.expect("test failed")
}

/// Runs physical drop tests for hot storage.
pub struct Runner {
	storage: HotStorage,
	table: EntryKind,
	/// Current version counter - increments with each write
	version: u64,
}

impl Runner {
	fn new(storage: HotStorage) -> Self {
		Self {
			storage,
			table: EntryKind::Multi,
			version: 1,
		}
	}

	fn next_version(&mut self) -> CommitVersion {
		let v = CommitVersion(self.version);
		self.version += 1;
		v
	}

	fn parse_table(&self, args: &mut ArgumentConsumer) -> Result<EntryKind, Box<dyn StdError>> {
		let table_arg = args.lookup("table");
		match table_arg {
			None => Ok(self.table),
			Some(arg) => {
				let s = &arg.value;
				if s == "multi" {
					Ok(EntryKind::Multi)
				} else if let Some(id_str) = s.strip_prefix("source:") {
					let id: u64 = id_str.parse()?;
					Ok(EntryKind::Source(PrimitiveId::Table(TableId(id))))
				} else if let Some(id_str) = s.strip_prefix("operator:") {
					let id: u64 = id_str.parse()?;
					Ok(EntryKind::Operator(FlowNodeId(id)))
				} else {
					Err(format!("unknown table: {}", s).into())
				}
			}
		}
	}

	fn collect_range(
		&self,
		table: EntryKind,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
	) -> Result<Vec<(CowVec<u8>, Option<CowVec<u8>>)>, Box<dyn StdError>> {
		let mut cursor = RangeCursor::new();
		let mut results = Vec::new();
		let version = CommitVersion(u64::MAX); // Get latest version

		while !cursor.exhausted {
			let batch = self.storage.range_next(table, &mut cursor, start, end, version, 1000)?;
			for entry in batch.entries {
				results.push((entry.key, entry.value));
			}
		}

		Ok(results)
	}
}

impl testscript::runner::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			// set KEY=VALUE
			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = decode_binary(&kv.key.unwrap());
				let value = decode_binary(&kv.value);
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				let version = self.next_version();
				self.storage.set(version, HashMap::from([(table, vec![(key, Some(value))])]))?;
				writeln!(output, "ok")?;
			}

			// delete KEY - creates tombstone (set with None)
			"delete" => {
				let mut args = command.consume_args();
				let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				let version = self.next_version();
				self.storage.set(version, HashMap::from([(table, vec![(key, None)])]))?;
				writeln!(output, "ok")?;
			}

			// drop KEY - physically removes the latest version of the entry
			"drop" => {
				let mut args = command.consume_args();
				let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				// Look up the latest version of this key to drop
				let all_versions = self.storage.get_all_versions(table, &key)?;
				if let Some((version, _)) = all_versions.first() {
					self.storage.drop(HashMap::from([(table, vec![(key.clone(), *version)])]))?;
				}
				writeln!(output, "ok")?;
			}

			// get KEY
			"get" => {
				let mut args = command.consume_args();
				let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				let version = CommitVersion(u64::MAX); // Get latest
				let value = self.storage.get(table, &key, version)?;
				let key_str = Raw::bytes(key.as_ref());
				match value {
					Some(v) => {
						let val_str = Raw::bytes(v.as_ref());
						writeln!(output, "{} => {}", key_str, val_str)?;
					}
					None => {
						writeln!(output, "{} => None", key_str)?;
					}
				}
			}

			// contains KEY
			"contains" => {
				let mut args = command.consume_args();
				let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				let version = CommitVersion(u64::MAX); // Get latest
				let exists = self.storage.get(table, &key, version)?.is_some();
				writeln!(output, "{}", exists)?;
			}

			// range START..END
			"range" => {
				let mut args = command.consume_args();
				let range_str = args.next_pos().ok_or("range not given")?.value.clone();
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				let parts: Vec<&str> = range_str.split("..").collect();
				if parts.len() != 2 {
					return Err("range must be in format START..END".into());
				}

				let start_key = if parts[0].is_empty() {
					None
				} else {
					Some(decode_binary(parts[0]))
				};
				let end_key = if parts[1].is_empty() {
					None
				} else {
					Some(decode_binary(parts[1]))
				};

				let start_bound = match &start_key {
					None => Bound::Unbounded,
					Some(k) => Bound::Included(k.as_ref()),
				};
				let end_bound = match &end_key {
					None => Bound::Unbounded,
					Some(k) => Bound::Excluded(k.as_ref()),
				};

				let items = self.collect_range(table, start_bound, end_bound)?;
				for (k, v) in items {
					let key_str = Raw::bytes(k.as_ref());
					match v {
						Some(val) => {
							let val_str = Raw::bytes(val.as_ref());
							writeln!(output, "{} => {}", key_str, val_str)?;
						}
						None => {
							writeln!(output, "{} => None", key_str)?;
						}
					}
				}
			}

			// scan [table=TABLE]
			"scan" => {
				let mut args = command.consume_args();
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				let items = self.collect_range(table, Bound::Unbounded, Bound::Unbounded)?;
				for (k, v) in items {
					let key_str = Raw::bytes(k.as_ref());
					match v {
						Some(val) => {
							let val_str = Raw::bytes(val.as_ref());
							writeln!(output, "{} => {}", key_str, val_str)?;
						}
						None => {
							writeln!(output, "{} => None", key_str)?;
						}
					}
				}
			}

			// count_entries [table=TABLE] - counts non-tombstone entries
			"count_entries" => {
				let mut args = command.consume_args();
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				let items = self.collect_range(table, Bound::Unbounded, Bound::Unbounded)?;
				let count = items.iter().filter(|(_, v)| v.is_some()).count();
				writeln!(output, "entries: {}", count)?;
			}

			// count_tombstones [table=TABLE] - counts tombstone entries (value is None)
			"count_tombstones" => {
				let mut args = command.consume_args();
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				let items = self.collect_range(table, Bound::Unbounded, Bound::Unbounded)?;
				let count = items.iter().filter(|(_, v)| v.is_none()).count();
				writeln!(output, "tombstones: {}", count)?;
			}

			// storage_size [table=TABLE] - gets raw storage size (total entries)
			"storage_size" => {
				let mut args = command.consume_args();
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				let items = self.collect_range(table, Bound::Unbounded, Bound::Unbounded)?;
				let size = items.len();
				writeln!(output, "size: {}", size)?;
			}

			// clear_table [table=TABLE]
			"clear_table" => {
				let mut args = command.consume_args();
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				self.storage.clear_table(table)?;
				writeln!(output, "ok")?;
			}

			// use_table TABLE - switches the default table
			"use_table" => {
				let mut args = command.consume_args();
				let table = self.parse_table(&mut args)?;
				args.reject_rest()?;

				self.table = table;
				writeln!(output, "ok")?;
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}
}
