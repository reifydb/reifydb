// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use base::encoding::binary::decode_binary;
use base::encoding::format;
use base::encoding::format::Formatter;
use std::error::Error as StdError;
use std::fmt::Write;
use std::path::Path;
use storage::{Memory, StorageEngineMut};
use test_each_file::test_each_path;
use testing::testscript;
use testing::util::parse_key_range;

test_each_path! { in "crates/storage/tests/engine" as memory => test_memory }

fn test_memory(path: &Path) {
    testscript::run_path(&mut EngineRunner::new(Memory::default()), path).expect("test failed")
}

/// Runs engine tests.
pub struct EngineRunner<S: StorageEngineMut> {
    engine: S,
}

impl<S: StorageEngineMut> EngineRunner<S> {
    fn new(engine: S) -> Self {
        Self { engine }
    }
}

impl<S: StorageEngineMut> testscript::Runner for EngineRunner<S> {
    fn run(&mut self, command: &testscript::Command) -> Result<String, Box<dyn StdError>> {
        let mut output = String::new();
        match command.name.as_str() {
            // remove KEY
            "remove" => {
                let mut args = command.consume_args();
                let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
                args.reject_rest()?;

                self.engine.remove(&key)?;
            }

            // // get KEY
            "get" => {
                let mut args = command.consume_args();
                let key = decode_binary(&args.next_pos().ok_or("key not given")?.value);
                args.reject_rest()?;
                let value = self.engine.get(&key)?;
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
                for item in self.engine.scan(range) {
                    let (key, value) = item?;
                    kvs.push((key, value));
                }

                if reverse {
                    kvs.reverse();
                }

                for (key, value) in kvs {
                    let fmtkv = format::Raw::key_value(&key, &value);
                    writeln!(output, "{fmtkv}")?;
                }
            }
            // scan_prefix PREFIX
            "scan_prefix" => {
                let mut args = command.consume_args();
                let prefix = decode_binary(&args.next_pos().ok_or("prefix not given")?.value);
                args.reject_rest()?;

                let mut scan = self.engine.scan_prefix(&prefix);
                while let Some((key, value)) = scan.next().transpose()? {
                    let fmtkv = format::Raw::key_value(&key, &value);
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

                self.engine.set(&key, value)?;
            }

            // status
            // "status" => {
            //     command.consume_args().reject_rest()?;
            //     writeln!(output, "{:#?}", self.engine.status()?)?;
            // }
            name => return Err(format!("invalid command {name}").into()),
        }
        Ok(output)
    }
}
