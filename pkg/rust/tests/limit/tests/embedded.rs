// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error, fmt::Write, path::Path, sync::Arc};

use reifydb::{
	Database, EmbeddedBuilder,
	core::{event::EventBus, interface::Params},
	memory, serializable,
	transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingle},
};
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct Runner {
	instance: Database,
	runtime: Arc<Runtime>,
}

impl Runner {
	pub fn new(
		input: (TransactionMultiVersion, TransactionSingle, TransactionCdc, EventBus),
		runtime: Arc<Runtime>,
	) -> Self {
		let (multi, single, cdc, eventbus) = input;
		Self {
			instance: runtime.block_on(EmbeddedBuilder::new(multi, single, cdc, eventbus).build()).unwrap(),
			runtime,
		}
	}
}

impl testscript::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			"command" => {
				let query = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("command: {query}");

				for frame in self
					.runtime
					.block_on(self.instance.command_as_root(query.as_str(), Params::None))?
				{
					writeln!(output, "{}", frame)?;
				}
			}
			"query" => {
				let query = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("query: {query}");

				for frame in self
					.runtime
					.block_on(self.instance.query_as_root(query.as_str(), Params::None))?
				{
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
		self.runtime.block_on(self.instance.start())?;
		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		self.instance.stop()?;
		Ok(())
	}
}

test_each_path! { in "pkg/rust/tests/limit/tests/scripts" as embedded => test_embedded }

fn test_embedded(path: &Path) {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	let input = runtime.block_on(async { serializable(memory().await).await }).unwrap();
	testscript::run_path(&mut Runner::new(input, Arc::clone(&runtime)), path).expect("test failed")
}
