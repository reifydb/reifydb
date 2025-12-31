// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error as StdError, fmt::Write, path::Path, time::Duration};

use reifydb_core::{
	EncodedKey, EncodedKeyRange, async_cow_vec,
	delta::Delta,
	interface::SingleVersionValues,
	util::encoding::{binary::decode_binary, format, format::Formatter},
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::{
	BackendConfig, SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange,
	SingleVersionRangeRev, StandardTransactionStore, TransactionStoreConfig, backend::BackendStorage,
};
use reifydb_testing::{tempdir::temp_dir, testscript};
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

test_each_path! { in "crates/store-transaction/tests/scripts/drop/single" as store_drop_single_memory => test_memory }
test_each_path! { in "crates/store-transaction/tests/scripts/drop/single" as store_drop_single_sqlite => test_sqlite }

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

/// Runs drop tests for single-version store.
pub struct Runner {
	store: StandardTransactionStore,
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
			runtime,
		}
	}
}

impl testscript::Runner for Runner {
	fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			// get KEY
			"get" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;
				let value: Option<SingleVersionValues> =
					self.runtime.block_on(async { self.store.get(&key).await })?.into();
				let value = value.map(|sv| sv.values.to_vec());
				writeln!(output, "{}", format::Raw::key_maybe_value(&key, value))?;
			}
			// contains KEY
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;
				let contains = self.runtime.block_on(async { self.store.contains(&key).await })?;
				writeln!(output, "{} => {}", format::Raw::key(&key), contains)?;
			}

			// scan [reverse=BOOL]
			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				args.reject_rest()?;

				if !reverse {
					let batch = self
						.runtime
						.block_on(async { self.store.range(EncodedKeyRange::all()).await })?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch = self.runtime.block_on(async {
						self.store.range_rev(EncodedKeyRange::all()).await
					})?;
					print(&mut output, batch.items.into_iter())
				};
			}
			// range RANGE [reverse=BOOL]
			"range" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let range = EncodedKeyRange::parse(
					args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."),
				);
				args.reject_rest()?;

				if !reverse {
					let batch = self.runtime.block_on(async { self.store.range(range).await })?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch =
						self.runtime.block_on(async { self.store.range_rev(range).await })?;
					print(&mut output, batch.items.into_iter())
				};
			}

			// prefix PREFIX [reverse=BOOL]
			"prefix" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let prefix =
					EncodedKey(decode_binary(&args.next_pos().ok_or("prefix not given")?.value));
				args.reject_rest()?;

				if !reverse {
					let batch =
						self.runtime.block_on(async { self.store.prefix(&prefix).await })?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch = self
						.runtime
						.block_on(async { self.store.prefix_rev(&prefix).await })?;
					print(&mut output, batch.items.into_iter())
				};
			}

			// set KEY=VALUE
			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				args.reject_rest()?;

				self.runtime.block_on(async {
					self.store
						.commit(async_cow_vec![
							(Delta::Set {
								key,
								values
							})
						])
						.await
				})?
			}

			// remove KEY
			"remove" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				self.runtime.block_on(async {
					self.store
						.commit(async_cow_vec![
							(Delta::Remove {
								key
							})
						])
						.await
				})?
			}

			// drop KEY
			"drop" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				self.runtime.block_on(async {
					self.store
						.commit(async_cow_vec![
							(Delta::Drop {
								key,
								up_to_version: None,
								keep_last_versions: None,
							})
						])
						.await
				})?
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}
}

fn print<I: Iterator<Item = SingleVersionValues>>(output: &mut String, iter: I) {
	for item in iter {
		let fmtkv = format::Raw::key_value(&item.key, item.values.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
