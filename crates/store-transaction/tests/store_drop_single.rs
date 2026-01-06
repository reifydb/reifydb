// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error as StdError, fmt::Write, path::Path, time::Duration};

use reifydb_core::{
	EncodedKey, EncodedKeyRange, cow_vec,
	delta::Delta,
	interface::SingleVersionValues,
	util::encoding::{binary::decode_binary, format, format::Formatter},
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::{
	HotConfig, SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange,
	SingleVersionRangeRev, StandardTransactionStore, TransactionStoreConfig, hot::HotStorage,
};
use reifydb_testing::{tempdir::temp_dir, testscript};
use test_each_file::test_each_path;

test_each_path! { in "crates/store-transaction/tests/scripts/drop/single" as store_drop_single_memory => test_memory }
test_each_path! { in "crates/store-transaction/tests/scripts/drop/single" as store_drop_single_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	let storage = HotStorage::memory();
	testscript::run_path(&mut Runner::new(storage), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|_db_path| {
		let storage = HotStorage::sqlite_in_memory();
		testscript::run_path(&mut Runner::new(storage), path)
	})
	.expect("test failed")
}

/// Runs drop tests for single-version store.
pub struct Runner {
	store: StandardTransactionStore,
}

impl Runner {
	fn new(storage: HotStorage) -> Self {
		let store = StandardTransactionStore::new(TransactionStoreConfig {
			hot: Some(HotConfig {
				storage,
				retention_period: Duration::from_millis(200),
			}),
			warm: None,
			cold: None,
			retention: Default::default(),
			merge_config: Default::default(),
			stats: Default::default(),
		})
		.unwrap();
		Self {
			store,
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
				let value: Option<SingleVersionValues> = self.store.get(&key)?.into();
				let value = value.map(|sv| sv.values.to_vec());
				writeln!(output, "{}", format::Raw::key_maybe_value(&key, value))?;
			}
			// contains KEY
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;
				let contains = self.store.contains(&key)?;
				writeln!(output, "{} => {}", format::Raw::key(&key), contains)?;
			}

			// scan [reverse=BOOL]
			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				args.reject_rest()?;

				if !reverse {
					let batch = SingleVersionRange::range(&self.store, EncodedKeyRange::all())?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch =
						SingleVersionRangeRev::range_rev(&self.store, EncodedKeyRange::all())?;
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
					let batch = SingleVersionRange::range(&self.store, range)?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch = SingleVersionRangeRev::range_rev(&self.store, range)?;
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
					let batch = SingleVersionRange::prefix(&self.store, &prefix)?;
					print(&mut output, batch.items.into_iter())
				} else {
					let batch = SingleVersionRangeRev::prefix_rev(&self.store, &prefix)?;
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

				self.store.commit(cow_vec![
					(Delta::Set {
						key,
						values
					})
				])?
			}

			// remove KEY
			"remove" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				self.store.commit(cow_vec![
					(Delta::Remove {
						key
					})
				])?
			}

			// drop KEY
			"drop" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				self.store.commit(cow_vec![
					(Delta::Drop {
						key,
						up_to_version: None,
						keep_last_versions: None,
					})
				])?
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
