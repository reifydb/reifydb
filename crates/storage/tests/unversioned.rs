// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::{error::Error as StdError, fmt::Write, path::Path};

use reifydb_core::{
	EncodedKey, EncodedKeyRange, async_cow_vec,
	delta::Delta,
	interface::{Unversioned, UnversionedStorage},
	util::encoding::{binary::decode_binary, format, format::Formatter},
	value::row::EncodedRow,
};
use reifydb_storage::{
	memory::Memory,
	sqlite::{Sqlite, SqliteConfig},
};
use reifydb_testing::{tempdir::temp_dir, testscript};
use test_each_file::test_each_path;

test_each_path! { in "crates/storage/tests/scripts/unversioned" as unversioned_memory => test_memory }
test_each_path! { in "crates/storage/tests/scripts/unversioned" as unversioned_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	testscript::run_path(&mut Runner::new(Memory::default()), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|db_path| testscript::run_path(&mut Runner::new(Sqlite::new(SqliteConfig::fast(db_path))), path))
		.expect("test failed")
}

/// Runs engine tests.
pub struct Runner<US: UnversionedStorage> {
	storage: US,
}

impl<US: UnversionedStorage> Runner<US> {
	fn new(storage: US) -> Self {
		Self {
			storage,
		}
	}
}

impl<US: UnversionedStorage> testscript::Runner for Runner<US> {
	fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			// get KEY
			"get" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;
				let value = self.storage.get(&key).unwrap().map(|sv| sv.row.to_vec());
				writeln!(output, "{}", format::Raw::key_maybe_row(&key, value))?;
			}
			// contains KEY
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;
				let contains = self.storage.contains(&key).unwrap();
				writeln!(output, "{} => {}", format::Raw::key(&key), contains)?;
			}

			// scan [reverse=BOOL]
			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				args.reject_rest()?;

				if !reverse {
					print(&mut output, self.storage.scan().unwrap())
				} else {
					print(&mut output, self.storage.scan_rev().unwrap())
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
					print(&mut output, self.storage.range(range).unwrap())
				} else {
					print(&mut output, self.storage.range_rev(range).unwrap())
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
					print(&mut output, self.storage.prefix(&prefix).unwrap())
				} else {
					print(&mut output, self.storage.prefix_rev(&prefix).unwrap())
				};
			}

			// set KEY=VALUE
			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let row = EncodedRow(decode_binary(&kv.value));
				args.reject_rest()?;

				self.storage
					.commit(async_cow_vec![
						(Delta::Set {
							key,
							row
						})
					])
					.unwrap()
			}

			// remove KEY
			"remove" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				args.reject_rest()?;

				self.storage
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

fn print<I: Iterator<Item = Unversioned>>(output: &mut String, iter: I) {
	for sv in iter {
		let fmtkv = format::Raw::key_row(&sv.key, sv.row.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
