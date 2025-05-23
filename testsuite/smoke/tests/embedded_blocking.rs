// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::embedded_blocking::Embedded;
use reifydb::storage::StorageEngine;
use reifydb::transaction::TransactionEngine;
use reifydb::{DB, Principal, ReifyDB, memory, mvcc, svl};
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use testing::testscript;
use testing::testscript::Command;

pub struct Runner<S: StorageEngine + 'static, T: TransactionEngine<S> + 'static> {
    engine: Embedded<S, T>,
    root: Principal,
}

impl<S: StorageEngine + 'static, T: TransactionEngine<S> + 'static> Runner<S, T> {
    pub fn new(transaction: T) -> Self {
        let (engine, root) = ReifyDB::embedded_blocking_with(transaction);
        Self { engine, root }
    }
}

impl<S: StorageEngine + 'static, T: TransactionEngine<S> + 'static> testscript::Runner
    for Runner<S, T>
{
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "tx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("tx: {query}");

                for line in self.engine.tx_execute(&self.root, query.as_str()) {
                    writeln!(output, "{}", line);
                }
            }
            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                for line in self.engine.rx_execute(&self.root, query.as_str()) {
                    writeln!(output, "{}", line);
                }
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_blocking_svl_memory => test_svl_memory }
test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_blocking_mvcc_memory => test_mvcc_memory }

fn test_svl_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(svl(memory())), path).expect("test failed")
}

fn test_mvcc_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(mvcc(memory())), path).expect("test failed")
}
