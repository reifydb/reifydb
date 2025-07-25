// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::core::hook::Hooks;
use reifydb::core::interface::{Transaction, UnversionedStorage, VersionedStorage};
use reifydb::embedded::Embedded;
use reifydb::{DB, ReifyDB, memory, optimistic};
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct Runner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    engine: Embedded<VS, US, T>,
    runtime: Runtime,
}

impl<VS, US, T> Runner<VS, US, T>
where
    VS: VersionedStorage,
    US: UnversionedStorage,
    T: Transaction<VS, US>,
{
    pub fn new(input: (T, Hooks)) -> Self {
        let (transaction, hooks) = input;
        Self {
            engine: ReifyDB::embedded_with(transaction, hooks),
            runtime: Runtime::new().unwrap(),
        }
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

                let engine = self.engine.clone();
                self.runtime.block_on(async {
                    for frame in engine.tx_as_root(query.as_str()).await? {
                        writeln!(output, "{}", frame).unwrap();
                    }
                    Ok::<(), reifydb::Error>(())
                })?;
            }
            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                let engine = self.engine.clone();
                self.runtime.block_on(async {
                    for frame in engine.rx_as_root(query.as_str()).await? {
                        writeln!(output, "{}", frame).unwrap();
                    }
                    Ok::<(), reifydb::Error>(())
                })?;
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

test_each_path! { in "testsuite/regression/tests/scripts" as embedded => test_embedded }

fn test_embedded(path: &Path) {
    testscript::run_path(&mut Runner::new(optimistic(memory())), path).expect("test failed")
}
