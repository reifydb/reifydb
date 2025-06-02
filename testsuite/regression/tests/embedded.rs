// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::embedded::Embedded;
use reifydb::reifydb_storage::Storage;
use reifydb::reifydb_storage::memory::Memory;
use reifydb::reifydb_transaction::Transaction;
use reifydb::reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb::reifydb_transaction::mvcc::transaction::serializable::Serializable;
use reifydb::{DB, Principal, ReifyDB, memory, optimistic, serializable};
use reifydb_testing::tempdir::temp_dir;
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct Runner<S: Storage + 'static, T: Transaction<S> + 'static> {
    engine: Embedded<S, T>,
    root: Principal,
    runtime: Runtime,
}

impl<S: Storage + 'static, T: Transaction<S> + 'static> Runner<S, T> {
    pub fn new(transaction: T) -> Self {
        let (engine, root) = ReifyDB::embedded_with(transaction);
        Self { engine, root, runtime: Runtime::new().unwrap() }
    }
}

impl<S: Storage + 'static, T: Transaction<S> + 'static> testscript::Runner for Runner<S, T> {
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "tx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("tx: {query}");

                let engine = self.engine.clone();
                self.runtime.block_on(async {
                    for line in engine.tx_as(&self.root, query.as_str()).await {
                        writeln!(output, "{}", line);
                    }
                });
            }
            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                let engine = self.engine.clone();
                self.runtime.block_on(async {
                    for line in engine.rx_as(&self.root, query.as_str()).await {
                        writeln!(output, "{}", line);
                    }
                });
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

// test_each_path! { in "testsuite/regression/tests/scripts" as embedded_serializable_memory => test_serializable_memory }
test_each_path! { in "testsuite/regression/tests/scripts" as embedded_optimistic_memory => test_optimistic_memory }

fn test_serializable_memory(path: &Path) {
    testscript::run_path(&mut Runner::<Memory, Serializable>::new(serializable(memory())), path)
        .expect("test failed")
}

fn test_optimistic_memory(path: &Path) {
    testscript::run_path(&mut Runner::<Memory, Optimistic<Memory>>::new(optimistic(memory())), path)
        .expect("test failed")
}
