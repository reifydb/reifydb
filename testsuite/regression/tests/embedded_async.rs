// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::core::hook::Hooks;
use reifydb::core::interface::{Params, UnversionedTransaction, VersionedTransaction};
use reifydb::{Database, ReifyDB, SessionAsync, memory, optimistic};
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
    instance: Database<VT, UT>,
    runtime: Runtime,
}

impl<VT, UT> Runner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(input: (VT, UT, Hooks)) -> Self {
        Self { instance: ReifyDB::new_async_with(input).build(), runtime: Runtime::new().unwrap() }
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
            "command" => {
                let rql =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("command: {rql}");

                let instance = &self.instance;
                self.runtime.block_on(async {
                    for frame in instance.command_as_root(rql.as_str(), Params::None).await? {
                        writeln!(output, "{}", frame).unwrap();
                    }
                    Ok::<(), reifydb::Error>(())
                })?;
            }
            "query" => {
                let rql =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("query: {rql}");

                let instance = &self.instance;
                self.runtime.block_on(async {
                    for frame in instance.query_as_root(rql.as_str(), Params::None).await? {
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

test_each_path! { in "testsuite/regression/tests/scripts" as embedded_async => test_embedded_async }

fn test_embedded_async(path: &Path) {
    testscript::run_path(&mut Runner::new(optimistic(memory())), path).expect("test failed")
}
