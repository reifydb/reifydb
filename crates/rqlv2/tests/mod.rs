// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error, fmt::Write, path::Path};

use reifydb_rqlv2::token::explain_tokenize;
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;

test_each_path! { in "crates/rqlv2/tests/scripts/token" as tokenize => run_test }

fn run_test(path: &Path) {
	testscript::run_path(&mut Runner {}, path).expect("test failed")
}

pub struct Runner {}

impl testscript::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			"tokenize" => {
				let mut args = command.consume_args();
				let query = args.next_pos().ok_or("args not given")?.value.as_str();
				args.reject_rest()?;
				let result = explain_tokenize(query)?;
				writeln!(output, "{}", result)?;
			}
			_ => unimplemented!("unknown command: {}", command.name),
		}
		Ok(output)
	}
}
