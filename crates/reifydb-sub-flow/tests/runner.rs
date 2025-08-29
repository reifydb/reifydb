// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error as StdError, fmt::Write, thread, time::Duration};

use reifydb::{MemoryDatabaseOptimistic, SessionSync, sync};
use reifydb_core::interface::{
	CdcCheckpoint, ConsumerId, Engine, Params, VersionedQueryTransaction,
};
use reifydb_testing::testscript;

/// Test runner for Flow tests with Frame-based output
pub struct FlowTestRunner {
	instance: MemoryDatabaseOptimistic,
}

impl FlowTestRunner {
	pub fn new() -> Self {
		let db = sync::memory_optimistic().build().unwrap();
		Self {
			instance: db,
		}
	}

	fn read_rql(&self, command: &testscript::Command) -> String {
		command.args
			.iter()
			.map(|arg| arg.value.clone())
			.collect::<Vec<_>>()
			.join(" ")
	}

	fn await_flows_completion(
		&mut self,
		timeout: Duration,
	) -> Result<(), Box<dyn StdError>> {
		let start = std::time::Instant::now();
		let poll_interval = Duration::from_millis(10);
		let consumer_id = ConsumerId::flow_consumer();

		// Wait for initial catch-up
		let latest_version = {
			let txn = self.instance.engine().begin_query()?;
			txn.version()
		};

		loop {
			let flow_checkpoint = {
				let mut txn =
					self.instance.engine().begin_query()?;
				CdcCheckpoint::fetch(&mut txn, &consumer_id)?
			};

			if flow_checkpoint >= latest_version {
				break;
			}

			if start.elapsed() > timeout {
				return Err(format!(
					"Timeout waiting for flows to complete after {}ms. Latest version: {}, Flow checkpoint: {}",
					timeout.as_millis(),
					latest_version,
					flow_checkpoint
				).into());
			}

			thread::sleep(poll_interval);
		}
		Ok(())
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
				let rql = self.read_rql(command);
				println!("command: {rql}");

				for frame in self.instance.command_as_root(
					rql.as_str(),
					Params::None,
				)? {
					writeln!(output, "{}", frame)?;
				}
			}

			"await" => {
				let timeout_ms = command
					.args
					.first()
					.and_then(|arg| {
						arg.value.parse::<u64>().ok()
					})
					.unwrap_or(5000);

				self.await_flows_completion(
					Duration::from_millis(timeout_ms),
				)?;
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
