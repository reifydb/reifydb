// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error, fmt::Write, path::Path};

use reifydb::{
	AsyncBuilder, Database, SessionAsync,
	core::{
		event::EventBus,
		interface::{
			CdcTransaction, Params, UnversionedTransaction,
			VersionedTransaction,
		},
	},
	engine::EngineTransaction,
	memory, optimistic,
};
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct Runner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	instance: Database<EngineTransaction<VT, UT, C>>,
	runtime: Runtime,
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
			instance: AsyncBuilder::new(
				versioned,
				unversioned,
				cdc,
				eventbus,
			)
			.build()
			.unwrap(),
			runtime: Runtime::new().unwrap(),
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
				let rql = command
					.args
					.iter()
					.map(|a| a.value.as_str())
					.collect::<Vec<_>>()
					.join(" ");

				println!("command: {rql}");

				let instance = &self.instance;
				self.runtime.block_on(async {
					for frame in instance
						.command_as_root(
							rql.as_str(),
							Params::None,
						)
						.await?
					{
						writeln!(output, "{}", frame)
							.unwrap();
					}
					Ok::<(), reifydb::Error>(())
				})?;
			}
			"query" => {
				let rql = command
					.args
					.iter()
					.map(|a| a.value.as_str())
					.collect::<Vec<_>>()
					.join(" ");

				println!("query: {rql}");

				let instance = &self.instance;
				self.runtime.block_on(async {
					for frame in instance
						.query_as_root(
							rql.as_str(),
							Params::None,
						)
						.await?
					{
						writeln!(output, "{}", frame)
							.unwrap();
					}
					Ok::<(), reifydb::Error>(())
				})?;
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

test_each_path! { in "testsuite/regression/tests/scripts" as embedded_async => test_embedded_async }

fn test_embedded_async(path: &Path) {
	testscript::run_path(&mut Runner::new(optimistic(memory())), path)
		.expect("test failed")
}
