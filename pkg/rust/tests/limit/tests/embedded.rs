// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error, fmt::Write, path::Path};

use db_embedded::memory;
use reifydb::{Database, Params, embedded as db_embedded};
use reifydb_testing::{testscript, testscript::command::Command};
use test_each_file::test_each_path;

pub struct Runner {
	instance: Database,
}

impl Runner {
	pub fn new() -> Self {
		Self {
			instance: memory().build().unwrap(),
		}
	}
}

impl testscript::runner::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			"admin" => {
				let query = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("admin: {query}");

				for frame in self.instance.admin_as_root(query.as_str(), Params::None)? {
					writeln!(output, "{}", frame)?;
				}
			}
			"command" => {
				let query = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("command: {query}");

				for frame in self.instance.command_as_root(query.as_str(), Params::None)? {
					writeln!(output, "{}", frame)?;
				}
			}
			"query" => {
				let query = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("query: {query}");

				for frame in self.instance.query_as_root(query.as_str(), Params::None)? {
					writeln!(output, "{}", frame)?;
				}
			}
			name => {
				return Err(format!("invalid command {name}").into());
			}
		}

		Ok(output)
	}

	fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
		self.instance.start()?;
		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		self.instance.stop()?;
		Ok(())
	}
}

test_each_path! { in "pkg/rust/tests/limit/tests/scripts" as embedded => test_embedded }

fn test_embedded(path: &Path) {
	testscript::runner::run_path(&mut Runner::new(), path).expect("test failed")
}
