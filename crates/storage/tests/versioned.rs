// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::delta::Delta;
use reifydb_core::encoding::binary::decode_binary;
use reifydb_core::encoding::format;
use reifydb_core::encoding::format::Formatter;
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange, async_cow_vec};
use reifydb_storage::memory::Memory;
use reifydb_storage::{VersionedStorage, Versioned};
use reifydb_testing::testscript;
use std::error::Error as StdError;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;

test_each_path! { in "crates/storage/tests/scripts/versioned" as versioned_memory => test_memory }

fn test_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(Memory::default()), path).expect("test failed")
}

/// Runs engine tests.
pub struct Runner<VS: VersionedStorage> {
    storage: VS,
}

impl<VS: VersionedStorage> Runner<VS> {
    fn new(storage: VS) -> Self {
        Self { storage }
    }
}

impl<VS: VersionedStorage> testscript::Runner for Runner<VS> {
    fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
        let mut output = String::new();
        match command.name.as_str() {
            // get KEY [version=VERSION]
            "get" => {
                let mut args = command.consume_args();
                let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
                let version = args.lookup_parse("version")?.unwrap_or(0u64);
                args.reject_rest()?;
                let value = self.storage.get(&key, version).map(|sv| sv.row.to_vec());
                writeln!(output, "{}", format::Raw::key_maybe_row(&key, value))?;
            }
            // contains KEY [version=VERSION]
            "contains" => {
                let mut args = command.consume_args();
                let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
                let version = args.lookup_parse("version")?.unwrap_or(0u64);
                args.reject_rest()?;
                let contains = self.storage.contains(&key, version);
                writeln!(output, "{} => {}", format::Raw::key(&key), contains)?;
            }

            // scan [reverse=BOOL] [version=VERSION]
            "scan" => {
                let mut args = command.consume_args();
                let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                let version = args.lookup_parse("version")?.unwrap_or(0u64);
                args.reject_rest()?;

                if !reverse {
                    print(&mut output, self.storage.scan(version))
                } else {
                    print(&mut output, self.storage.scan_rev(version))
                };
            }
            // scan_range RANGE [reverse=BOOL] [version=VERSION]
            "scan_range" => {
                let mut args = command.consume_args();
                let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                let range = EncodedKeyRange::parse(
                    args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."),
                );
                let version = args.lookup_parse("version")?.unwrap_or(0u64);
                args.reject_rest()?;

                if !reverse {
                    print(&mut output, self.storage.scan_range(range, version))
                } else {
                    print(&mut output, self.storage.scan_range_rev(range, version))
                };
            }

            // scan_prefix PREFIX [reverse=BOOL] [version=VERSION]
            "scan_prefix" => {
                let mut args = command.consume_args();
                let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                let version = args.lookup_parse("version")?.unwrap_or(0u64);
                let prefix =
                    EncodedKey(decode_binary(&args.next_pos().ok_or("prefix not given")?.value));
                args.reject_rest()?;

                if !reverse {
                    print(&mut output, self.storage.scan_prefix(&prefix, version))
                } else {
                    print(&mut output, self.storage.scan_prefix_rev(&prefix, version))
                };
            }

            // set KEY=VALUE [version=VERSION]
            "set" => {
                let mut args = command.consume_args();
                let kv = args.next_key().ok_or("key=value not given")?.clone();
                let key = EncodedKey(decode_binary(&kv.key.unwrap()));
                let row = EncodedRow(decode_binary(&kv.value));
                let version = args.lookup_parse("version")?.unwrap_or(0u64);
                args.reject_rest()?;

                self.storage.apply(async_cow_vec![(Delta::Set { key, row })], version)
            }

            // remove KEY [version=VERSION]
            "remove" => {
                let mut args = command.consume_args();
                let key = EncodedKey(decode_binary(&args.next_pos().ok_or("key not given")?.value));
                let version = args.lookup_parse("version")?.unwrap_or(0u64);
                args.reject_rest()?;

                self.storage.apply(async_cow_vec![(Delta::Remove { key })], version)
            }

            name => return Err(format!("invalid command {name}").into()),
        }
        Ok(output)
    }
}

fn print<I: Iterator<Item =Versioned>>(output: &mut String, mut iter: I) {
    while let Some(sv) = iter.next() {
        let fmtkv = format::Raw::key_row(&sv.key, sv.row.as_slice());
        writeln!(output, "{fmtkv}").unwrap();
    }
}
