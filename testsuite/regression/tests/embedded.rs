// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::embedded::Embedded;
use reifydb::storage::StorageEngine;
use reifydb::transaction::TransactionEngine;
use reifydb::{DB, Principal, ReifyDB, memory, mvcc, svl};
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use testing::testscript;
use testing::testscript::Command;

pub struct Runner<S: StorageEngine, T: TransactionEngine<S>> {
    engine: Embedded<S, T>,
    root: Principal,
}

impl<S: StorageEngine, T: TransactionEngine<S>> Runner<S, T> {
    pub fn new(transaction: T) -> Self {
        let (engine, root) = ReifyDB::embedded_with(transaction);
        Self { engine, root }
    }
}

impl<S: StorageEngine, T: TransactionEngine<S>> testscript::Runner for Runner<S, T> {
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "tx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("tx: {query}");
                for line in self.engine.tx_execute_as(&self.root, query.as_str()) {
                    writeln!(output, "{}", line)?;
                }
            }
            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                for line in self.engine.rx_execute_as(&self.root, query.as_str()) {
                    writeln!(output, "{}", line)?;
                }
            }
            "list_schema" => {
                writeln!(output, "test")?;
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

test_each_path! { in "testsuite/regression/tests/scripts" as embedded_svl_memory => test_svl_memory }
test_each_path! { in "testsuite/regression/tests/scripts" as embedded_mvcc_memory => test_mvcc_memory }

fn test_svl_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(svl(memory())), path).expect("test failed")
}

fn test_mvcc_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(mvcc(memory())), path).expect("test failed")
}
