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
	interface::MultiVersionValues,
	util::encoding::{binary::decode_binary, format, format::Formatter},
	value::encoded::EncodedValues,
};
use reifydb_store_transaction::{
	backend::{multi::BackendMultiVersion, result::MultiVersionIterResult},
	memory::MemoryBackend,
	sqlite::{SqliteBackend, SqliteConfig},
};
use reifydb_testing::{tempdir::temp_dir, testscript};
use test_each_file::test_each_path;

test_each_path! { in "crates/store-transaction/tests/scripts/backend/multi" as backend_multi_memory => test_memory }
test_each_path! { in "crates/store-transaction/tests/scripts/backend/multi" as backend_multi_sqlite => test_sqlite }

fn test_memory(path: &Path) {
	testscript::run_path(&mut Runner::new(MemoryBackend::default()), path).expect("test failed")
}

fn test_sqlite(path: &Path) {
	temp_dir(|db_path| {
		testscript::run_path(&mut Runner::new(SqliteBackend::new(SqliteConfig::fast(db_path))), path)
	})
	.expect("test failed")
}

/// Runs engine tests.
pub struct Runner<BMV: BackendMultiVersion> {
	backend: BMV,
	version: CommitVersion,
}

impl<BMV: BackendMultiVersion> Runner<BMV> {
	fn new(backend: BMV) -> Self {
		Self {
			backend,
			version: CommitVersion(0),
		}
	}
}

impl<BMV: BackendMultiVersion> testscript::Runner for Runner<BMV> {
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
					.backend
					.get(&key, version)?
					.into_option()
					.map(|sv: MultiVersionValues| sv.values.to_vec());

				writeln!(output, "{}", format::Raw::key_maybe_value(&key, value))?;
			}
			// contains KEY [version=VERSION]
			"contains" => {
				let mut args = command.consume_args();
				let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;
				let contains = self.backend.contains(&key, version)?;
				writeln!(output, "{} => {}", format::Raw::key(&key), contains)?;
			}

			// scan [reverse=BOOL] [version=VERSION]
			"scan" => {
				let mut args = command.consume_args();
				let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
				let version = CommitVersion(args.lookup_parse("version")?.unwrap_or(self.version.0));
				args.reject_rest()?;

				if !reverse {
					print(&mut output, self.backend.scan(version)?)
				} else {
					print(&mut output, self.backend.scan_rev(version)?)
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
					print(&mut output, self.backend.range(range, version)?)
				} else {
					print(&mut output, self.backend.range_rev(range, version)?)
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
					print(&mut output, self.backend.prefix(&prefix, version)?)
				} else {
					print(&mut output, self.backend.prefix_rev(&prefix, version)?)
				};
			}

			// set KEY=VALUE [version=VERSION]
			"set" => {
				let mut args = command.consume_args();
				let kv = args.next_key().ok_or("key=value not given")?.clone();
				let key = EncodedKey(decode_binary(&kv.key.unwrap()));
				let values = EncodedValues(decode_binary(&kv.value));
				let version = if let Some(v) = args.lookup_parse("version")? {
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.backend.commit(
					async_cow_vec![
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
					v
				} else {
					self.version.0 += 1;
					self.version
				};
				args.reject_rest()?;

				self.backend.commit(
					async_cow_vec![
						(Delta::Remove {
							key
						})
					],
					version,
				)?
			}

			name => {
				return Err(format!("invalid command {name}").into());
			}
		}
		Ok(output)
	}
}

fn print<I: Iterator<Item = MultiVersionIterResult>>(output: &mut String, iter: I) {
	for item in iter {
		match item {
			MultiVersionIterResult::Value(sv) => {
				let fmtkv = format::Raw::key_value(&sv.key, sv.values.as_slice());
				writeln!(output, "{fmtkv}").unwrap();
			}
			MultiVersionIterResult::Tombstone {
				key,
				..
			} => {
				let fmtkv = format::Raw::key_value(&key, "tombstone".as_bytes());
				writeln!(output, "{fmtkv}").unwrap();
			}
		}
	}
}
