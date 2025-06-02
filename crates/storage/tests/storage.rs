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
use reifydb_core::encoding::format::Formatter;
use reifydb_core::encoding::{format, keycode};
use reifydb_persistence::{Action, KeyRange};
use reifydb_storage::Storage;
use reifydb_storage::memory::Memory;
use reifydb_testing::testscript;
use reifydb_testing::util::parse_key_range;
use std::error::Error as StdError;
use std::fmt::Write;
use std::ops::{Bound, Deref};
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
                let range =
                    parse_key_range(args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."))?;
                args.reject_rest()?;

                let range = KeyRange { start: range.0, end: range.1 };

                let mut kvs = Vec::new();
                for sv in self.storage.scan_range(range, 0) {
                    kvs.push((sv.key, sv.value));
                }

                if reverse {
                    kvs.reverse();
                }

                for (key, value) in kvs {
                    let fmtkv = format::Raw::key_value(&key, &value.deref());
                    writeln!(output, "{fmtkv}")?;
                }
            }
            // scan_range [reverse=BOOL] PREFIX
            "scan_range" => {
                let mut args = command.consume_args();
                let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
                args.reject_rest()?;

                let range = keycode::prefix_range(&prefix);
                let range: KeyRange = range.into();

                let mut scan = self.storage.scan_range(range, 1);
                while let Some(sv) = scan.next() {
                    let fmtkv = format::Raw::key_value(&sv.key, &sv.value.deref());
                    writeln!(output, "{fmtkv}")?;
                }
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
