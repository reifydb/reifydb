// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb::{DB, Embedded, ReifyDB};
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;
use testing::testscript;
use testing::testscript::Command;

pub struct EmbeddedRunner {
    pub db: Embedded,
}

impl EmbeddedRunner {
    pub fn new() -> Self {
        Self { db: ReifyDB::embedded() }
    }
}

impl testscript::Runner for EmbeddedRunner {
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            "tx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("tx: {query}");
                    
                for line in self.db.tx_execute(query.as_str()) {
                    writeln!(output, "{}", line)?;
                }
            }
            "rx" => {
                let query =
                    command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

                println!("rx: {query}");

                for line in self.db.rx_execute(query.as_str()) {
                    writeln!(output, "{}", line)?;
                }
            }
            name => return Err(format!("invalid command {name}").into()),
        }

        Ok(output)
    }
}

test_each_path! { in "testsuite/smoke/tests/scripts" as embedded_svl_memory => test_embedded_svl_memory }

fn test_embedded_svl_memory(path: &Path) {
    testscript::run_path(&mut EmbeddedRunner::new(), path).expect("test failed")
}
