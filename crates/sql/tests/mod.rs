// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error, fmt::Write, path::Path};

use reifydb_sql::transpile;
use reifydb_testing::testscript::{
	command::Command,
	runner::{Runner, run_path},
};
use test_each_file::test_each_path;

test_each_path! { in "crates/sql/tests/scripts/transpile" as transpile_tests => run_test }

fn run_test(path: &Path) {
	run_path(&mut TestRunner {}, path).expect("test failed")
}

pub struct TestRunner {}

impl Runner for TestRunner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			"transpile" => {
				let mut args = command.consume_args();
				let query = args.next_pos().ok_or("args not given")?.value.as_str();
				args.reject_rest()?;
				match transpile(query) {
					Ok(result) => writeln!(output, "{}", result).unwrap(),
					Err(e) => writeln!(output, "error: {}", e).unwrap(),
				}
			}
			_ => unimplemented!("unknown command: {}", command.name),
		}
		Ok(output)
	}
}
