// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::core::hook::Hooks;
use reifydb::core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb::variant::embedded::Embedded;
use reifydb::{DB, ReifyDB, memory, optimistic};
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct Runner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    instance: Embedded<VT, UT>,
    runtime: Runtime,
}

impl<VT, UT> Runner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(input: (VT, UT, Hooks)) -> Self {
        Self { instance: ReifyDB::embedded_with(input).build(), runtime: Runtime::new().unwrap() }
    }
}

impl<VT, UT> testscript::Runner for Runner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "write" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("tx: {query}");

                let engine = self.instance.clone();
                self.runtime.block_on(async {
                    for frame in engine.write_as_root(query.as_str()).await? {
                        writeln!(output, "{}", frame).unwrap();
                    }
                    Ok::<(), reifydb::Error>(())
                })?;
            }
            "read" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                let engine = self.instance.clone();
                self.runtime.block_on(async {
                    for frame in engine.read_as_root(query.as_str()).await? {
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
