// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error as StdError, fmt::Write, thread, time::Duration};

use reifydb::{MemoryDatabaseOptimistic, SessionSync, sync};
use reifydb_core::{Frame, interface::Params};
use reifydb_testing::testscript;

/// Test runner for Flow tests with Frame-based output
pub struct FlowTestRunner {
	instance: MemoryDatabaseOptimistic,
}

impl FlowTestRunner {
	pub fn new() -> Self {
		let db = sync::memory_optimistic();
		Self {
			instance: db,
		}
	}

	/// Format a Frame for output
	fn format_frame(&self, frame: &Frame) -> String {
		format!("{}", frame)
	}

	/// Read the entire RQL command from the command body
	fn read_rql_command(&self, command: &testscript::Command) -> String {
		command.args
			.iter()
			.map(|arg| arg.value.clone())
			.collect::<Vec<_>>()
			.join(" ")
	}
}

impl testscript::Runner for FlowTestRunner {
	fn start_script(&mut self) -> Result<(), Box<dyn StdError>> {
		self.instance.start()?;
		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn StdError>> {
		self.instance.stop()?;
		Ok(())
	}

	fn run(
		&mut self,
		command: &testscript::Command,
	) -> Result<String, Box<dyn StdError>> {
		let mut output = String::new();

		match command.name.as_str() {
			"command" => {
				let rql = self.read_rql_command(command);
				self.instance
					.command_as_root(&rql, Params::None)?;
				writeln!(output, "ok")?;
			}

			"query" => {
				let rql = self.read_rql_command(command);
				let frames = self
					.instance
					.query_as_root(&rql, Params::None)?;
				for frame in frames {
					write!(
						output,
						"{}",
						self.format_frame(&frame)
					)?;
				}
			}

			"wait" => {
				if command.args.is_empty() {
					return Err("wait command requires milliseconds argument".into());
				}
				let ms: u64 =
					command.args[0].value.parse().map_err(
						|_| "wait argument must be a valid number of milliseconds",
					)?;
				thread::sleep(Duration::from_millis(ms));
				writeln!(output, "ok")?;
			}

			name => {
				return Err(format!("invalid command {name}")
					.into());
			}
		}

		// Ensure output ends with newline if not empty
		if !output.is_empty() && !output.ends_with('\n') {
			writeln!(output)?;
		}

		Ok(output)
	}
}
