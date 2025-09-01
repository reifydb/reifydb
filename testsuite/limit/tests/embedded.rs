// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error, fmt::Write, path::Path};

use reifydb::{
	Database, EmbeddedBuilder, Session,
	core::{
		event::EventBus,
		interface::{
			CdcTransaction, Params, UnversionedTransaction,
			VersionedTransaction,
		},
	},
	engine::EngineTransaction,
	memory, serializable,
};
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;

pub struct Runner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	instance: Database<EngineTransaction<VT, UT, C>>,
}

impl<VT, UT, C> Runner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	pub fn new(input: (VT, UT, C, EventBus)) -> Self {
		let (versioned, unversioned, cdc, eventbus) = input;
		Self {
			instance: EmbeddedBuilder::new(
				versioned,
				unversioned,
				cdc,
				eventbus,
			)
			.build()
			.unwrap(),
		}
	}
}

impl<VT, UT, C> testscript::Runner for Runner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			"command" => {
				let query = command
					.args
					.iter()
					.map(|a| a.value.as_str())
					.collect::<Vec<_>>()
					.join(" ");

				println!("command: {query}");

				for frame in self.instance.command_as_root(
					query.as_str(),
					Params::None,
				)? {
					writeln!(output, "{}", frame)?;
				}
			}
			"query" => {
				let query = command
					.args
					.iter()
					.map(|a| a.value.as_str())
					.collect::<Vec<_>>()
					.join(" ");

				println!("query: {query}");

				for frame in self.instance.query_as_root(
					query.as_str(),
					Params::None,
				)? {
					writeln!(output, "{}", frame)?;
				}
			}
			name => {
				return Err(format!("invalid command {name}")
					.into());
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

test_each_path! { in "testsuite/limit/tests/scripts" as embedded_sync => test_embedded_sync }

fn test_embedded_sync(path: &Path) {
	testscript::run_path(&mut Runner::new(serializable(memory())), path)
		.expect("test failed")
}
