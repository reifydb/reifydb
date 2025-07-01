// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::Explain;
use reifydb_rql::{ExplainAst, ExplainLex};
use reifydb_testing::testscript;
use reifydb_testing::testscript::Command;
use std::error::Error;
use std::fmt::Write;
use std::path::Path;
use test_each_file::test_each_path;

test_each_path! { in "crates/rql/tests/scripts/lex" as lex => run_test }
test_each_path! { in "crates/rql/tests/scripts/ast" as ast => run_test }

fn run_test(path: &Path) {
    testscript::run_path(&mut Runner {}, path).expect("test failed")
}

pub struct Runner {}

impl testscript::Runner for Runner {
    fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
        let mut output = String::new();
        match command.name.as_str() {
            // lex QUERY
            "lex" => {
                let mut args = command.consume_args();
                let query = args.next_pos().ok_or("args not given")?.value.as_str();
                args.reject_rest()?;
                let result = ExplainLex::explain(query).unwrap();
                writeln!(output, "{}", result).unwrap();
            }
            // ast QUERY
            "ast" => {
                let mut args = command.consume_args();
                let query = args.next_pos().ok_or("args not given")?.value.as_str();
                args.reject_rest()?;
                let result = ExplainAst::explain(query).unwrap();
                writeln!(output, "{}", result).unwrap();
            }
            _ => unimplemented!(),
        }
        Ok(output)
    }
}
