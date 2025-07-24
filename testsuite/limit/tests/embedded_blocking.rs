// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::core::hook::Hooks;
use reifydb::core::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb::embedded_blocking::Embedded;
use reifydb::{ReifyDB, memory, optimistic};
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;

pub struct Runner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    engine: Embedded<VS, US, T>,
}

impl<VS, US, T> Runner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(input: (T, Hooks)) -> Self {
        Self { engine: ReifyDB::embedded_blocking_with(input) }
    }
}

impl<VS, US, T> testscript::Runner for Runner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "tx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("tx: {query}");

                for frame in self.engine.tx_as_root(query.as_str())? {
                    writeln!(output, "{}", frame)?;
                }
            }
            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                for frame in self.engine.rx_as_root(query.as_str())? {
                    writeln!(output, "{}", frame)?;
                }
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

test_each_path! { in "testsuite/limit/tests/scripts" as embedded_blocking => test_embedded_blocking }

fn test_embedded_blocking(path: &Path) {
    testscript::run_path(&mut Runner::new(optimistic(memory())), path).expect("test failed")
}
