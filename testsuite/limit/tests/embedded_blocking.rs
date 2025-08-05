// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::core::hook::Hooks;
use reifydb::core::interface::{UnversionedTransaction, VersionedTransaction};
use reifydb::variant::embedded_blocking::EmbeddedBlocking;
use reifydb::{ReifyDB, memory, serializable};
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;

pub struct Runner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    engine: EmbeddedBlocking<VT, UT>,
}

impl<VT, UT> Runner<VT, UT>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
{
    pub fn new(input: (VT, UT, Hooks)) -> Self {
        Self { engine: ReifyDB::embedded_blocking_with(input).build() }
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
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("command: {query}");

                for frame in self.engine.command_as_root(query.as_str())? {
                    writeln!(output, "{}", frame)?;
                }
            }
            "query" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("query: {query}");

                for frame in self.engine.query_as_root(query.as_str())? {
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
    testscript::run_path(&mut Runner::new(serializable(memory())), path).expect("test failed")
}
