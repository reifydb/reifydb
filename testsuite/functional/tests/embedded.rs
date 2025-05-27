// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::embedded::Embedded;
use reifydb::persistence::{Lmdb, Persistence};
use reifydb::transaction::Transaction;
use reifydb::{DB, Principal, ReifyDB, memory, mvcc, svl};
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use testing::testscript;
use testing::testscript::Command;
use tokio::runtime::Runtime;
use testing::tempdir::temp_dir;

pub struct Runner<P: Persistence + 'static, T: Transaction<P> + 'static> {
    engine: Embedded<P, T>,
    root: Principal,
    runtime: Runtime,
}

impl<P: Persistence + 'static, T: Transaction<P> + 'static> Runner<P, T> {
    pub fn new(transaction: T) -> Self {
        let (engine, root) = ReifyDB::embedded_with(transaction);
        Self { engine, root, runtime: Runtime::new().unwrap() }
    }
}

impl<P: Persistence + 'static, T: Transaction<P> + 'static> testscript::Runner
    for Runner<P, T>
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
                    for line in engine.tx_as(&self.root, query.as_str()).await {
                        writeln!(output, "{}", line).unwrap();
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
                        writeln!(output, "{}", line).unwrap();
                    }
                });
            }
            "list_schema" => {
                writeln!(output, "test")?;
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

test_each_path! { in "testsuite/functional/tests/scripts" as embedded_mvcc_memory => test_mvcc_memory }
test_each_path! { in "testsuite/functional/tests/scripts" as embedded_svl_memory => test_svl_memory }

test_each_path! { in "testsuite/functional/tests/scripts" as embedded_svl_lmdb => test_svl_lmdb }
test_each_path! { in "testsuite/functional/tests/scripts" as embedded_mvcc_lmdb => test_mvcc_lmdb }


fn test_mvcc_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(mvcc(memory())), path).expect("test failed")
}


fn test_svl_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(svl(memory())), path).expect("test failed")
}

fn test_mvcc_lmdb(path: &Path) {
    temp_dir(|db_path| {
        testscript::run_path(&mut Runner::new(mvcc(Lmdb::new(db_path).unwrap())), path)
            .expect("test failed")
    })
}

fn test_svl_lmdb(path: &Path) {
    temp_dir(|db_path| {
        testscript::run_path(&mut Runner::new(svl(Lmdb::new(db_path).unwrap())), path)
            .expect("test failed")
    })
}
