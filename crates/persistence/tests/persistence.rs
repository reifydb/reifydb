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
use reifydb_persistence::{Memory, Persistence};
use reifydb_testing::testscript;
use reifydb_testing::util::parse_key_range;
use std::error::Error as StdError;
use std::fmt::Write;
use std::ops::Deref;
use std::path::Path;
use test_each_file::test_each_path;

test_each_path! { in "crates/persistence/tests/persistence" as memory => test_memory }

fn test_memory(path: &Path) {
    testscript::run_path(&mut PersistenceRunner::new(Memory::default()), path).expect("test failed")
}

/// Runs engine tests.
pub struct PersistenceRunner<P: Persistence> {
    persistence: P,
}

impl<P: Persistence> PersistenceRunner<P> {
    fn new(persistence: P) -> Self {
        Self { persistence }
    }
}

impl<P: Persistence> testscript::Runner for PersistenceRunner<P> {
    fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
        let mut output = String::new();
        match command.name.as_str() {
            // remove KEY
            "remove" => {
                let mut args = command.consume_args();
                let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
                args.reject_rest()?;

                self.persistence.remove(&key)?;
            }

            // // get KEY
            "get" => {
                let mut args = command.consume_args();
                let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
                args.reject_rest()?;
                let value = self.persistence.get(&key)?;
                writeln!(output, "{}", format::Raw::key_maybe_value(&key, value.as_deref()))?;
            }

            // scan [reverse=BOOL] RANGE
            "scan" => {
                let mut args = command.consume_args();
                let reverse = args.lookup_parse("reverse")?.unwrap_or(false);
                let range =
                    parse_key_range(args.next_pos().map(|a| a.value.as_str()).unwrap_or(".."))?;
                args.reject_rest()?;

                let mut kvs = Vec::new();
                for item in self.persistence.scan(range) {
                    let (key, value) = item?;
                    kvs.push((key, value));
                }

                if reverse {
                    kvs.reverse();
                }

                for (key, value) in kvs {
                    let fmtkv = format::Raw::key_value(&key, &value.deref());
                    writeln!(output, "{fmtkv}")?;
                }
            }
            // scan_prefix PREFIX
            "scan_prefix" => {
                let mut args = command.consume_args();
                let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
                args.reject_rest()?;

                let mut scan = self.persistence.scan_prefix(&prefix);
                while let Some((key, value)) = scan.next().transpose()? {
                    let fmtkv = format::Raw::key_value(&key, &value.deref());
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

                self.persistence.set(&key, value)?;
            }

            // status
            // "status" => {
            //     command.consume_args().reject_rest()?;
            //     writeln!(output, "{:#?}", self.reifydb_engine.status()?)?;
            // }
            name => return Err(format!("invalid command {name}").into()),
        }
        Ok(output)
    }
}
