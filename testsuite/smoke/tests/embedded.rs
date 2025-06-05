// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::embedded::Embedded;
use reifydb::reifydb_storage::Storage;
use reifydb::reifydb_transaction::Transaction;
use reifydb::{DB, Principal, ReifyDB, lmdb, memory, optimistic, serializable};
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
                    for line in engine.tx_as(&self.root, query.as_str()).await? {
                        writeln!(output, "{}", line).unwrap();
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
                    for line in engine.rx_as(&self.root, query.as_str()).await? {
                        writeln!(output, "{}", line).unwrap();
                    }
                    Ok::<(), reifydb::Error>(())
                })?;
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_optimistic_memory => test_optimistic_memory }
test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_optimistic_lmdb => test_optimistic_lmdb }

fn test_optimistic_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(optimistic(memory())), path).expect("test failed")
}

fn test_optimistic_lmdb(path: &Path) {
    temp_dir(|db_path| {
        testscript::run_path(&mut Runner::new(optimistic(lmdb(db_path))), path)
            .expect("test failed")
    })
}

test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_serializable_memory => test_serializable_memory }
test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_serializable_lmdb => test_serializable_lmdb }

fn test_serializable_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(serializable(memory())), path).expect("test failed")
}

fn test_serializable_lmdb(path: &Path) {
    temp_dir(|db_path| {
        testscript::run_path(&mut Runner::new(serializable(lmdb(db_path))), path)
            .expect("test failed")
    })
}
