// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::embedded::Embedded;
use reifydb::reifydb_persistence::{Lmdb, Memory, Persistence};
use reifydb::reifydb_transaction::Transaction;
use reifydb::{DB, Principal, ReifyDB, memory, mvcc, svl, serializable, optimistic};
use reifydb_testing::tempdir::temp_dir;
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use tokio::runtime::Runtime;
use reifydb::reifydb_transaction::skipdb::skipdb::optimistic::OptimisticDb;
use reifydb::reifydb_transaction::skipdb::skipdb::serializable::SerializableDb;

pub struct Runner<P: Persistence + 'static, T: Transaction<P> + 'static> {
    reifydb_engine: Embedded<P, T>,
    root: Principal,
    runtime: Runtime,
}

impl<P: Persistence + 'static, T: Transaction<P> + 'static> Runner<P, T> {
    pub fn new(transaction: T) -> Self {
        let (reifydb_engine, root) = ReifyDB::embedded_with(transaction);
        Self { reifydb_engine, root, runtime: Runtime::new().unwrap() }
    }
}

impl<P: Persistence + 'static, T: Transaction<P> + 'static> testscript::Runner for Runner<P, T> {
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "tx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("tx: {query}");

                let reifydb_engine = self.reifydb_engine.clone();
                self.runtime.block_on(async {
                    for line in reifydb_engine.tx_as(&self.root, query.as_str()).await {
                        writeln!(output, "{}", line);
                    }
                });
            }
            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                let reifydb_engine = self.reifydb_engine.clone();
                self.runtime.block_on(async {
                    for line in reifydb_engine.rx_as(&self.root, query.as_str()).await {
                        writeln!(output, "{}", line);
                    }
                });
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

test_each_path! { in "testsuite/regression/tests/scripts" as embedded_serializable_memory => test_serializable_memory }
test_each_path! { in "testsuite/regression/tests/scripts" as embedded_optimistic_memory => test_optimistic_memory }
test_each_path! { in "testsuite/regression/tests/scripts" as embedded_svl_memory => test_svl_memory }

test_each_path! { in "testsuite/regression/tests/scripts" as embedded_mvcc_memory => test_mvcc_memory }

test_each_path! { in "testsuite/regression/tests/scripts" as embedded_svl_lmdb => test_svl_lmdb }
test_each_path! { in "testsuite/regression/tests/scripts" as embedded_mvcc_lmdb => test_mvcc_lmdb }

fn test_serializable_memory(path: &Path) {
    testscript::run_path(
        &mut Runner::<Memory, SerializableDb<Vec<u8>, Vec<u8>>>::new(serializable()),
        path,
    )
        .expect("test failed")
}

fn test_optimistic_memory(path: &Path) {
    testscript::run_path(
        &mut Runner::<Memory, OptimisticDb<Vec<u8>, Vec<u8>>>::new(optimistic()),
        path,
    )
        .expect("test failed")
}

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
