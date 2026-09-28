// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{error::Error, fmt::Write, path::Path, sync::Arc};

use reifydb::{Database, Params, RuntimeConfig, WithSubsystem, embedded as db_embedded};
use reifydb_testing::{testscript, testscript::command::Command};
use reifydb_value::value::duration::Duration as ValueDuration;
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct Runner {
	instance: Database,
}

impl Runner {
	pub fn new() -> Self {
		Self {
			instance: db_embedded::memory()
				.with_runtime_config(RuntimeConfig::default().seeded(0))
				.with_flow(|c| c)
				.build()
				.unwrap(),
		}
	}
}

impl Default for Runner {
	fn default() -> Self {
		Self::new()
	}
}

impl testscript::runner::Runner for Runner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();
		match command.name.as_str() {
			"admin" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("admin: {rql}");

				for frame in self.instance.admin_as_root(rql.as_str(), Params::None)? {
					writeln!(output, "{}", frame).unwrap();
				}
			}
			"command" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("command: {rql}");

				for frame in self.instance.command_as_root(rql.as_str(), Params::None)? {
					writeln!(output, "{}", frame).unwrap();
				}
			}
			"query" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("query: {rql}");

				for frame in self.instance.query_as_root(rql.as_str(), Params::None)? {
					writeln!(output, "{}", frame).unwrap();
				}
			}
			"await" => {
				let watermarks = self.instance.watermarks();
				let target = watermarks.tx().current()?;
				if !watermarks.cdc().wait_for_flow_consumer(
					target,
					ValueDuration::from_nanos_infallible(10_000_000_000),
				)? {
					return Err("flows did not catch up".into());
				}
			}
			name => {
				return Err(format!("invalid command {name}").into());
			}
		}

		Ok(output)
	}

	fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		self.instance.stop()?;
		Ok(())
	}
}

test_each_path! { in "pkg/rust/tests/regression/tests/scripts" as embedded => test_embedded }

fn test_embedded(path: &Path) {
	let runtime = Arc::new(Runtime::new().unwrap());
	let _guard = runtime.enter();
	testscript::runner::run_path(&mut Runner::new(), path).expect("test failed")
}
