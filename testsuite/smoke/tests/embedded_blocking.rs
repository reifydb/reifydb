// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::embedded_blocking::Embedded;
use reifydb::reifydb_persistence::{Lmdb, Memory, Persistence};
use reifydb::reifydb_transaction::Transaction;
use reifydb::reifydb_transaction::mvcc::transaction::optimistic::Optimistic;
use reifydb::reifydb_transaction::mvcc::transaction::serializable::Serializable;
use reifydb::{DB, Principal, ReifyDB, memory, optimistic, serializable, svl};
use reifydb_testing::tempdir::temp_dir;
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;

pub struct Runner<P: Persistence + 'static, T: Transaction<P> + 'static> {
    engine: Embedded<P, T>,
    root: Principal,
}

impl<P: Persistence + 'static, T: Transaction<P> + 'static> Runner<P, T> {
    pub fn new(transaction: T) -> Self {
        let (engine, root) = ReifyDB::embedded_blocking_with(transaction);
        Self { engine, root }
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

                for line in self.engine.tx_as(&self.root, query.as_str()) {
                    writeln!(output, "{}", line);
                }
            }
            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                for line in self.engine.rx_as(&self.root, query.as_str()) {
                    writeln!(output, "{}", line);
                }
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

// test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_blocking_serializable_memory => test_serializable_memory }
test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_blocking_optimistic_memory => test_optimistic_memory }

test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_blocking_svl_memory => test_svl_memory }
test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_blocking_svl_lmdb => test_svl_lmdb }

fn test_serializable_memory(path: &Path) {
    testscript::run_path(
        &mut Runner::<Memory, Serializable>::new(serializable()),
        path,
    )
    .expect("test failed")
}

fn test_optimistic_memory(path: &Path) {
    testscript::run_path(
        &mut Runner::<Memory, Optimistic>::new(optimistic()),
        path,
    )
    .expect("test failed")
}

fn test_svl_memory(path: &Path) {
    testscript::run_path(&mut Runner::new(svl(memory())), path).expect("test failed")
}

fn test_svl_lmdb(path: &Path) {
    temp_dir(|db_path| {
        testscript::run_path(&mut Runner::new(svl(Lmdb::new(db_path).unwrap())), path)
            .expect("test failed")
    })
}
