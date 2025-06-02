// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::embedded::Embedded;
use reifydb::reifydb_storage::Storage;
use reifydb::reifydb_transaction::Transaction;
use reifydb::{DB, Principal, ReifyDB};
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
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

                let reifydb_engine = self.engine.clone();
                self.runtime.block_on(async {
                    for line in reifydb_engine.tx_as(&self.root, query.as_str()).await {
                        writeln!(output, "{}", line).unwrap();
                    }
                });
            }
            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                let reifydb_engine = self.engine.clone();
                self.runtime.block_on(async {
                    for line in reifydb_engine.rx_as(&self.root, query.as_str()).await {
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
