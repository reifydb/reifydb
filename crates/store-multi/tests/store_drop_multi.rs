// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error as StdError, fmt::Write, path::Path, time::Duration};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	interface::store::{MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionValues},
	runtime::compute::ComputePool,
	util::encoding::{
		binary::decode_binary,
		format::{Formatter, raw::Raw},
	},
	value::encoded::{
		encoded::EncodedValues,
		key::{EncodedKey, EncodedKeyRange},
	},
};
use reifydb_store_multi::{
	config::{HotConfig, MultiStoreConfig},
	hot::storage::HotStorage,
	store::StandardMultiStore,
};
use reifydb_testing::{
	tempdir::temp_dir,
	testscript,
	testscript::{command::Command, runner::run_path},
};
use reifydb_type::cow_vec;
use test_each_file::test_each_path;

test_each_path! { in "crates/store-multi/tests/scripts/drop" as store_drop_multi_memory => test_memory }
test_each_path! { in "crates/store-multi/tests/scripts/drop" as store_drop_multi_sqlite => test_sqlite }

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

/// Runs drop tests for multi-version store.
pub struct Runner {
	store: StandardMultiStore,
	version: CommitVersion,
}

impl Runner {
	fn new(storage: HotStorage) -> Self {
		let store = StandardMultiStore::new(MultiStoreConfig {
			hot: Some(HotConfig {
				storage,
				retention_period: Duration::from_millis(200),
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			event_bus: reifydb_core::event::EventBus::new(),
		})
		.unwrap();
		Self {
			store,
			version: CommitVersion(0),
		}
	}
}

impl testscript::runner::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			// get KEY [version=VERSION]
			"get" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let value =
					self.store.get(&key, version)?.map(|sv: MultiVersionValues| sv.values.to_vec());

				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}
			// contains KEY [version=VERSION]
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;
				let contains = self.store.contains(&key, version)?;
				writeln!(output, "{} => {}", Raw::key(&key), contains)?;
			}

			// scan [reverse=BOOL] [version=VERSION]
			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				if !reverse {
					let items: Vec<_> = self
						.store
						.range(EncodedKeyRange::all(), version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.store
						.range_rev(EncodedKeyRange::all(), version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				};
			}
			// range RANGE [reverse=BOOL] [version=VERSION]
			"range" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let range = EncodedKeyRange::parse(
					args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."),
				);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				if !reverse {
					let items: Vec<_> = self
						.store
						.range(range, version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.store
						.range_rev(range, version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				};
			}

			// prefix PREFIX [reverse=BOOL] [version=VERSION]
			"prefix" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				let prefix =
					EncodedKey(decode_binary(&args.next_pos().ok_or("prefix not given")?.value));
				args.reject_rest()?;

				let range = EncodedKeyRange::prefix(&prefix.0);
				if !reverse {
					let items: Vec<_> = self
						.store
						.range(range, version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				} else {
					let items: Vec<_> = self
						.store
						.range_rev(range, version, 1024)
						.collect::<Result<Vec<_>, _>>()?;
					print(&mut output, items.into_iter())
				};
			}

			// set KEY=VALUE [version=VERSION]
			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.store.commit(
					cow_vec![
						(Delta::Set {
							key,
							values
						})
					],
					version,
				)?;
			}

			// remove KEY [version=VERSION]
			"remove" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.store.commit(
					cow_vec![
						(Delta::Remove {
							key
						})
					],
					version,
				)?
			}

			// unset KEY=VALUE [version=VERSION]
			"unset" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.store.commit(
					cow_vec![
						(Delta::Unset {
							key,
							values
						})
					],
					version,
				)?;
			}

			// drop KEY [up_to_version=V] [keep_last_versions=N] [version=VERSION]
			"drop" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let up_to_version = args.lookup_parse::<u64>("up_to_version")?.map(CommitVersion);
				let keep_last_versions = args.lookup_parse::<usize>("keep_last_versions")?;
				let version = if let Some(v) = args.lookup_parse("version")? {
					CommitVersion(v)
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.store.commit(
					cow_vec![
						(Delta::Drop {
							key,
							up_to_version,
							keep_last_versions,
						})
					],
					version,
				)?;
			}

			// count_versions KEY - counts how many versions of a key exist
			"count_versions" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				// Count version boundaries: where value changes and is Some
				// This detects actual stored version entries, not MVCC forward propagation
				let mut count = 0;
				let mut prev_value: Option<Vec<u8>> = None;
				for v in 1..=1000 {
					let current =
						self.store.get(&key, CommitVersion(v))?.map(|sv| sv.values.to_vec());
					if current.is_some() && current != prev_value {
						count += 1;
					}
					prev_value = current;
				}
				writeln!(output, "{} => {} versions", Raw::key(&key), count)?;
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}
}

fn print<I: Iterator<Item = MultiVersionValues>>(output: &mut String, iter: I) {
	for item in iter {
		let fmtkv = Raw::key_value(&item.key, item.values.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
