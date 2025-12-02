// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

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

test_each_path! { in "crates/store-transaction/tests/scripts/store/single" as store_single_memory => test_memory }
test_each_path! { in "crates/store-transaction/tests/scripts/store/single" as store_single_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	testscript::run_path(&mut Runner::new(BackendStorage::memory()), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|_db_path| testscript::run_path(&mut Runner::new(BackendStorage::sqlite_in_memory()), path))
		.expect("test failed")
}

/// Runs engine tests.
pub struct Runner {
	store: StandardTransactionStore,
}

impl Runner {
	fn new(storage: BackendStorage) -> Self {
		Self {
			store: StandardTransactionStore::new(TransactionStoreConfig {
				hot: Some(BackendConfig {
					storage,
					retention_period: Duration::from_millis(200),
				}),
				warm: None,
				cold: None,
				retention: Default::default(),
				merge_config: Default::default(),
			})
			.unwrap(),
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
				let value: Option<SingleVersionValues> = self.store.get(&key).unwrap().into();
				let value = value.map(|sv| sv.values.to_vec());
				writeln!(output, "{}", format::Raw::key_maybe_value(&key, value))?;
			}
			// contains KEY
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;
				let contains = self.store.contains(&key).unwrap();
				writeln!(output, "{} => {}", format::Raw::key(&key), contains)?;
			}

			// scan [reverse=BOOL]
			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				args.reject_rest()?;

				if !reverse {
					print(&mut output, self.store.range(EncodedKeyRange::all()).unwrap())
				} else {
					print(&mut output, self.store.range_rev(EncodedKeyRange::all()).unwrap())
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
					print(&mut output, self.store.range(range).unwrap())
				} else {
					print(&mut output, self.store.range_rev(range).unwrap())
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
					print(&mut output, self.store.prefix(&prefix).unwrap())
				} else {
					print(&mut output, self.store.prefix_rev(&prefix).unwrap())
				};
			}

			// set KEY=VALUE
			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				args.reject_rest()?;

				self.store
					.commit(async_cow_vec![
						(Delta::Set {
							key,
							values
						})
					])
					.unwrap()
			}

			// remove KEY
			"remove" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				self.store
					.commit(async_cow_vec![
						(Delta::Remove {
							key
						})
					])
					.unwrap()
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
