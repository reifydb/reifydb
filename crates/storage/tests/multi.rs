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
	CommitVersion, EncodedKey, EncodedKeyRange, async_cow_vec,
	delta::Delta,
	interface::{MultiVersionRow, MultiVersionStorage, TransactionId},
	util::encoding::{binary::decode_binary, format, format::Formatter},
	value::row::EncodedRow,
};
use reifydb_storage::{
	memory::Memory,
	sqlite::{Sqlite, SqliteConfig},
};
use reifydb_testing::{tempdir::temp_dir, testscript};
use test_each_file::test_each_path;

test_each_path! { in "crates/storage/tests/scripts/multi" as multi_memory => test_memory }
test_each_path! { in "crates/storage/tests/scripts/multi" as multi_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	testscript::run_path(&mut Runner::new(Memory::default()), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|db_path| testscript::run_path(&mut Runner::new(Sqlite::new(SqliteConfig::fast(db_path))), path))
		.expect("test failed")
}

/// Runs engine tests.
pub struct Runner<MVS: MultiVersionStorage> {
	storage: MVS,
	version: CommitVersion,
}

impl<MVS: MultiVersionStorage> Runner<MVS> {
	fn new(storage: MVS) -> Self {
		Self {
			storage,
			version: 0,
		}
	}
}

impl<MVS: MultiVersionStorage> testscript::Runner for Runner<MVS> {
	fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();
		match command.name.as_str() {
			// get KEY [version=VERSION]
			"get" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = args.lookup_parse("version")?.unwrap_or(self.version);
				args.reject_rest()?;
				let value = self.storage.get(&key, version)?.map(|sv| sv.row.to_vec());
				writeln!(output, "{}", format::Raw::key_maybe_row(&key, value))?;
			}
			// contains KEY [version=VERSION]
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = args.lookup_parse("version")?.unwrap_or(self.version);
				args.reject_rest()?;
				let contains = self.storage.contains(&key, version)?;
				writeln!(output, "{} => {}", format::Raw::key(&key), contains)?;
			}

			// scan [reverse=BOOL] [version=VERSION]
			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = args.lookup_parse("version")?.unwrap_or(self.version);
				args.reject_rest()?;

				if !reverse {
					print(&mut output, self.storage.scan(version)?)
				} else {
					print(&mut output, self.storage.scan_rev(version)?)
				};
			}
			// range RANGE [reverse=BOOL] [version=VERSION]
			"range" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let range = EncodedKeyRange::parse(
					args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."),
				);
				let version = args.lookup_parse("version")?.unwrap_or(self.version);
				args.reject_rest()?;

				if !reverse {
					print(&mut output, self.storage.range(range, version)?)
				} else {
					print(&mut output, self.storage.range_rev(range, version)?)
				};
			}

			// prefix PREFIX [reverse=BOOL] [version=VERSION]
			"prefix" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = args.lookup_parse("version")?.unwrap_or(self.version);
				let prefix =
					EncodedKey(decode_binary(&args.next_pos().ok_or("prefix not given")?.value));
				args.reject_rest()?;

				if !reverse {
					print(&mut output, self.storage.prefix(&prefix, version)?)
				} else {
					print(&mut output, self.storage.prefix_rev(&prefix, version)?)
				};
			}

			// set KEY=VALUE [version=VERSION]
			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let row = EncodedRow(decode_binary(&kv.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					v
				} else {
					self.version += 1;
					self.version
				};
				args.reject_rest()?;

				self.storage.commit(
					async_cow_vec![
						(Delta::Set {
							key,
							row
						})
					],
					version,
					TransactionId::default(),
				)?;
			}

			// remove KEY [version=VERSION]
			"remove" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					v
				} else {
					self.version += 1;
					self.version
				};
				args.reject_rest()?;

				self.storage.commit(
					async_cow_vec![
						(Delta::Remove {
							key
						})
					],
					version,
					TransactionId::default(),
				)?
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}
}

fn print<I: Iterator<Item = MultiVersionRow>>(output: &mut String, iter: I) {
	for sv in iter {
		let fmtkv = format::Raw::key_row(&sv.key, sv.row.as_slice());
		writeln!(output, "{fmtkv}").unwrap();
	}
}
