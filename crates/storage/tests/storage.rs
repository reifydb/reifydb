// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use reifydb_core::encoding::binary::decode_binary;
use reifydb_core::encoding::format;
use reifydb_core::encoding::format::Formatter;
use reifydb_persistence::{Action, KeyRange};
use reifydb_storage::memory::Memory;
use reifydb_storage::{Storage, StoredValue};
use reifydb_testing::testscript;
use std::error::Error as StdError;
use std::fmt::Write;
use std::ops::Deref;
use std::path::Path;
use test_each_file::test_each_path;

test_each_path! { in "crates/storage/tests/scripts" as memory => test_memory }

fn test_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(Memory::default()), path).expect("test failed")
}

/// Runs engine tests.
pub struct Runner<S: Storage> {
    storage: S,
}

impl<S: Storage> Runner<S> {
    fn new(storage: S) -> Self {
        Self { storage }
    }
}

impl<S: Storage> testscript::Runner for Runner<S> {
    fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
        let mut output = String::new();
        match command.name.as_str() {
            // // get KEY
            "get" => {
                let mut args = command.consume_args();
                let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
                args.reject_rest()?;
                let value = self.storage.get(&key, 0).map(|sv| sv.value.to_vec());
                writeln!(output, "{}", format::Raw::key_maybe_value(&key, value))?;
            }

            // scan [reverse=BOOL]
            "scan" => {
                let mut args = command.consume_args();
                let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                args.reject_rest()?;

                if !reverse {
                    print(&mut output, self.storage.scan(0))
                } else {
                    print(&mut output, self.storage.scan_rev(0))
                };
            }
            // scan_range RANGE [reverse=BOOL]
            "scan_range" => {
                let mut args = command.consume_args();
                let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                let range =
                    KeyRange::parse(args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."));
                args.reject_rest()?;

                if !reverse {
                    print(&mut output, self.storage.scan_range(range, 0))
                } else {
                    print(&mut output, self.storage.scan_range_rev(range, 0))
                };
            }

            // scan_prefix PREFIX [reverse=BOOL]
            "scan_prefix" => {
                let mut args = command.consume_args();
                let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
                args.reject_rest()?;

                if !reverse {
                    print(&mut output, self.storage.scan_prefix(&prefix, 0))
                } else {
                    print(&mut output, self.storage.scan_prefix_rev(&prefix, 0))
                };
            }

            // set KEY=VALUE
            "set" => {
                let mut args = command.consume_args();
                let kv = args.next_key().ok_or("key=value not given")?.clone();
                let key = decode_binary(&kv.key.unwrap());
                let value = decode_binary(&kv.value);
                args.reject_rest()?;

                self.storage.apply(vec![(Action::Set { key, value }, 0)])
            }

            // remove KEY
            "remove" => {
                let mut args = command.consume_args();
                let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
                args.reject_rest()?;

                self.storage.apply(vec![(Action::Remove { key }, 0)])
            }

            name => return Err(format!("invalid command {name}").into()),
        }
        Ok(output)
    }
}

fn print<I: Iterator<Item = StoredValue>>(output: &mut String, mut iter: I) {
    while let Some(sv) = iter.next() {
        let fmtkv = format::Raw::key_value(&sv.key, &sv.value.deref());
        writeln!(output, "{fmtkv}").unwrap();
    }
}
