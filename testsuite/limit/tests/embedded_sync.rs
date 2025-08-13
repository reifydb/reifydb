// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb::core::hook::Hooks;
use reifydb::core::interface::{
    CdcTransaction, Params, StandardTransaction, UnversionedTransaction, VersionedTransaction,
};
use reifydb::{memory, serializable, Database, SessionSync, SyncBuilder};
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;

pub struct Runner<VT, UT, C>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    C: CdcTransaction,
{
    instance: Database<StandardTransaction<VT, UT, C>>,
}

impl<VT, UT, C> Runner<VT, UT, C>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    C: CdcTransaction,
{
    pub fn new(input: (VT, UT, C, Hooks)) -> Self {
        let (versioned, unversioned, cdc, hooks) = input;
        Self { instance: SyncBuilder::new(versioned, unversioned, cdc, hooks).build() }
    }
}

impl<VT, UT, C> testscript::Runner for Runner<VT, UT, C>
where
    VT: VersionedTransaction,
    UT: UnversionedTransaction,
    C: CdcTransaction,
{
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "command" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("command: {query}");

                for frame in self.instance.command_as_root(query.as_str(), Params::None)? {
                    writeln!(output, "{}", frame)?;
                }
            }
            "query" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("query: {query}");

                for frame in self.instance.query_as_root(query.as_str(), Params::None)? {
                    writeln!(output, "{}", frame)?;
                }
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

test_each_path! { in "testsuite/limit/tests/scripts" as embedded_sync => test_embedded_sync }

fn test_embedded_sync(path: &Path) {
    testscript::run_path(&mut Runner::new(serializable(memory())), path).expect("test failed")
}
