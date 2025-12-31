// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error as StdError, fmt::Write, path::Path, time::Duration};

use reifydb_core::{
	CommitVersion, EncodedKey, EncodedKeyRange, async_cow_vec,
	delta::Delta,
	interface::MultiVersionValues,
	util::encoding::{binary::decode_binary, format, format::Formatter},
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::{
	BackendConfig, MultiVersionCommit, MultiVersionContains, MultiVersionGet, MultiVersionRange,
	MultiVersionRangeRev, StandardTransactionStore, TransactionStoreConfig, backend::BackendStorage,
};
use reifydb_testing::{tempdir::temp_dir, testscript};
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

test_each_path! { in "crates/store-transaction/tests/scripts/drop/multi" as store_drop_multi_all_memory => test_memory }
test_each_path! { in "crates/store-transaction/tests/scripts/drop/multi" as store_drop_multi_all_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	let runtime = Runtime::new().unwrap();
	let storage = runtime.block_on(async { BackendStorage::memory().await });
	testscript::run_path(&mut Runner::new_with_runtime(storage, runtime), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|_db_path| {
		let runtime = Runtime::new().unwrap();
		let storage = runtime.block_on(async { BackendStorage::sqlite_in_memory().await });
		testscript::run_path(&mut Runner::new_with_runtime(storage, runtime), path)
	})
	.expect("test failed")
}

/// Runs drop tests for multi-version store.
pub struct Runner {
	store: StandardTransactionStore,
	version: CommitVersion,
	runtime: Runtime,
}

impl Runner {
	fn new_with_runtime(storage: BackendStorage, runtime: Runtime) -> Self {
		let store = runtime.block_on(async {
			StandardTransactionStore::new(TransactionStoreConfig {
				hot: Some(BackendConfig {
					storage,
					retention_period: Duration::from_millis(200),
				}),
				warm: None,
				cold: None,
				retention: Default::default(),
				merge_config: Default::default(),
				stats: Default::default(),
			})
			.unwrap()
		});
		Self {
			store,
			version: CommitVersion(0),
			runtime,
		}
	}
}

impl testscript::Runner for Runner {
	fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			// get KEY [version=VERSION]
			"get" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				let value = self
					.runtime
					.block_on(async { self.store.get(&key, version).await })?
					.map(|sv: MultiVersionValues| sv.values.to_vec());

				writeln!(output, "{}", format::Raw::key_maybe_value(&key, value))?;
			}
			// contains KEY [version=VERSION]
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;
				let contains =
					self.runtime.block_on(async { self.store.contains(&key, version).await })?;
				writeln!(output, "{} => {}", format::Raw::key(&key), contains)?;
			}

			// scan [reverse=BOOL] [version=VERSION]
			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				if !reverse {
					let batch = self.runtime.block_on(async {
						self.store.range(EncodedKeyRange::all(), version).await
					})?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch = self.runtime.block_on(async {
						self.store.range_rev(EncodedKeyRange::all(), version).await
					})?;
					print(&mut output, batch.items.into_iter())
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
					let batch = self
						.runtime
						.block_on(async { self.store.range(range, version).await })?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch = self
						.runtime
						.block_on(async { self.store.range_rev(range, version).await })?;
					print(&mut output, batch.items.into_iter())
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

				if !reverse {
					let batch = self
						.runtime
						.block_on(async { self.store.prefix(&prefix, version).await })?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch = self
						.runtime
						.block_on(async { self.store.prefix_rev(&prefix, version).await })?;
					print(&mut output, batch.items.into_iter())
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

				self.runtime.block_on(async {
					self.store
						.commit(
							async_cow_vec![
								(Delta::Set {
									key,
									values
								})
							],
							version,
						)
						.await
				})?;
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

				self.runtime.block_on(async {
					self.store
						.commit(
							async_cow_vec![
								(Delta::Remove {
									key
								})
							],
							version,
						)
						.await
				})?
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

				self.runtime.block_on(async {
					self.store
						.commit(
							async_cow_vec![
								(Delta::Drop {
									key,
									up_to_version,
									keep_last_versions,
								})
							],
							version,
						)
						.await
				})?;
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
					let current = self
						.runtime
						.block_on(async { self.store.get(&key, CommitVersion(v)).await })?
						.map(|sv| sv.values.to_vec());
					if current.is_some() && current != prev_value {
						count += 1;
					}
					prev_value = current;
				}
				writeln!(output, "{} => {} versions", format::Raw::key(&key), count)?;
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
		let fmtkv = format::Raw::key_value(&item.key, item.values.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
