// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error as StdError, fmt::Write, path::Path};

use reifydb_core::{
	delta::Delta,
	event::EventBus,
	interface::store::{
		SingleVersionCommit, SingleVersionContains, SingleVersionGet, SingleVersionRange,
		SingleVersionRangeRev, SingleVersionValues,
	},
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
use reifydb_store_single::{
	config::{HotConfig, SingleStoreConfig},
	hot::tier::HotTier,
	store::StandardSingleStore,
};
use reifydb_testing::{
	tempdir::temp_dir,
	testscript,
	testscript::{command::Command, runner::run_path},
};
use reifydb_type::cow_vec;
use test_each_file::test_each_path;

test_each_path! { in "crates/store-single/tests/scripts/drop" as store_drop_single_memory => test_memory }
test_each_path! { in "crates/store-single/tests/scripts/drop" as store_drop_single_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	let compute_pool = ComputePool::new(2, 8);
	let storage = HotTier::memory(compute_pool);
	run_path(&mut Runner::new(storage), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|_db_path| {
		let storage = HotTier::sqlite_in_memory();
		run_path(&mut Runner::new(storage), path)
	})
	.expect("test failed")
}

/// Runs drop tests for single-version store.
pub struct Runner {
	store: StandardSingleStore,
}

impl Runner {
	fn new(storage: HotTier) -> Self {
		let store = StandardSingleStore::new(SingleStoreConfig {
			hot: Some(HotConfig {
				storage,
			}),
			event_bus: EventBus::new(),
		})
		.unwrap();
		Self {
			store,
		}
	}
}

impl testscript::runner::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			// get KEY
			"get" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;
				let value: Option<SingleVersionValues> = self.store.get(&key)?.into();
				let value = value.map(|sv| sv.values.to_vec());
				writeln!(output, "{}", Raw::key_maybe_value(&key, value))?;
			}
			// contains KEY
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;
				let contains = self.store.contains(&key)?;
				writeln!(output, "{} => {}", Raw::key(&key), contains)?;
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

			// unset KEY=VALUE
			"unset" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				args.reject_rest()?;

				self.store.commit(cow_vec![
					(Delta::Unset {
						key,
						values
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
		let fmtkv = Raw::key_value(&item.key, item.values.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
