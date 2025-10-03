// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error, fmt::Write, path::Path};

use reifydb::{
	Database, EmbeddedBuilder, Session,
	core::{event::EventBus, interface::Params},
	memory, optimistic,
	transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingleVersion},
};
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;

pub struct Runner {
	instance: Database,
}

impl Runner {
	pub fn new(input: (TransactionMultiVersion, TransactionSingleVersion, TransactionCdc, EventBus)) -> Self {
		let (multi, single, cdc, eventbus) = input;
		Self {
			instance: EmbeddedBuilder::new(multi, single, cdc, eventbus).build().unwrap(),
		}
	}
}

impl testscript::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			"command" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("command: {rql}");

				let instance = &self.instance;
				for frame in instance.command_as_root(rql.as_str(), Params::None)? {
					writeln!(output, "{}", frame).unwrap();
				}
			}
			"query" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("query: {rql}");

				let instance = &self.instance;
				for frame in instance.query_as_root(rql.as_str(), Params::None)? {
					writeln!(output, "{}", frame).unwrap();
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

test_each_path! { in "pkg/rust/tests/regression/tests/scripts" as embedded => test_embedded }

fn test_embedded(path: &Path) {
	testscript::run_path(&mut Runner::new(optimistic(memory())), path).expect("test failed")
}
