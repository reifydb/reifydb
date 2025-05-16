// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::storage::StorageEngineMut;
use reifydb::transaction::TransactionEngineMut;
use reifydb::{DB, Embedded, ReifyDB, memory, mvcc, svl};
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use std::ptr::NonNull;
use test_each_file::test_each_path;
use testing::testscript;
use testing::testscript::Command;

pub struct EmbeddedRunner<'a, S: StorageEngineMut + 'a, T: TransactionEngineMut<'a, S> + 'a> {
    engine: NonNull<Embedded<'a, S, T>>,
}

impl<'a, S: StorageEngineMut + 'a, T: TransactionEngineMut<'a, S> + 'a> EmbeddedRunner<'a, S, T> {
    // otherwise runs into lifetime issues with the runner
    pub fn new(transaction: T) -> Self {
        let boxed = Box::new(ReifyDB::embedded_with(transaction));
        let ptr = NonNull::new(Box::into_raw(boxed)).expect("Box::into_raw returned null");
        Self { engine: ptr }
    }

    pub fn engine(&self) -> &'a Embedded<'a, S, T> {
        unsafe { self.engine.as_ref() }
    }

    pub fn engine_mut(&mut self) -> &'a mut Embedded<'a, S, T> {
        unsafe { self.engine.as_mut() }
    }
}

impl<'a, S: StorageEngineMut + 'a, T: TransactionEngineMut<'a, S> + 'a> Drop
    for EmbeddedRunner<'a, S, T>
{
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.engine.as_ptr()));
        }
    }
}

impl<'a, S: StorageEngineMut + 'a, T: TransactionEngineMut<'a, S> + 'a> testscript::Runner
    for EmbeddedRunner<'a, S, T>
{
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "tx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("tx: {query}");

                for line in self.engine_mut().tx_execute(query.as_str()) {
                    writeln!(output, "{}", line)?;
                }
            }
            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                for line in self.engine().rx_execute(query.as_str()) {
                    writeln!(output, "{}", line)?;
                }
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

test_each_path! { in "testsuite/smoke/tests/scripts" as svl_memory => test_embedded_svl_memory }
test_each_path! { in "testsuite/smoke/tests/scripts" as mvcc_memory => test_embedded_mvcc_memory }

fn test_embedded_svl_memory(path: &Path) {
    testscript::run_path(&mut EmbeddedRunner::new(svl(memory())), path).expect("test failed")
}

fn test_embedded_mvcc_memory(path: &Path) {
    testscript::run_path(&mut EmbeddedRunner::new(mvcc(memory())), path).expect("test failed")
}
