// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error, fmt::Write, path::Path, sync::Arc};

use reifydb::{Database, core::util::retry::retry, server, sub_server_ws::factory::WsConfig};
use reifydb_client::WsClient;
use reifydb_testing::{testscript, testscript::command::Command};
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct WsRunner {
	instance: Option<Database>,
	client: Option<WsClient>,
	runtime: Arc<Runtime>,
}

impl WsRunner {
	pub fn new(runtime: Arc<Runtime>) -> Self {
		let instance = server::memory().with_ws(WsConfig::default().bind_addr("::1:0")).build().unwrap();

		Self {
			instance: Some(instance),
			client: None,
			runtime,
		}
	}
}

impl testscript::runner::Runner for WsRunner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();

		let client = self.client.as_ref().ok_or("No client available")?;

		match command.name.as_str() {
			"admin" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("admin: {rql}");

				let result = self.runtime.block_on(client.admin(&rql, None))?;
				for frame in result.frames {
					writeln!(output, "{}", frame).unwrap();
				}
			}

			"command" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("command: {rql}");

				let result = self.runtime.block_on(client.command(&rql, None))?;
				for frame in result.frames {
					writeln!(output, "{}", frame).unwrap();
				}
			}

			"query" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("query: {rql}");

				let result = self.runtime.block_on(client.query(&rql, None))?;
				for frame in result.frames {
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
		let server = self.instance.as_mut().unwrap();
		server.start()?;

		let port = server.sub_server_ws().unwrap().port().unwrap();

		let mut client = self.runtime.block_on(WsClient::connect(&format!("ws://[::1]:{}", port)))?;
		self.runtime.block_on(client.authenticate("mysecrettoken"))?;

		self.client = Some(client);

		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		// Close the client connection
		if let Some(client) = self.client.take() {
			let _ = self.runtime.block_on(client.close());
		}

		// Stop the server
		if let Some(mut server) = self.instance.take() {
			let _ = server.stop();
			drop(server);
		}

		Ok(())
	}
}

test_each_path! { in "pkg/rust/tests/regression/tests/scripts" as ws => test_ws }

fn test_ws(path: &Path) {
	retry(3, || {
		let runtime = Arc::new(Runtime::new().unwrap());
		let _guard = runtime.enter();
		testscript::runner::run_path(&mut WsRunner::new(Arc::clone(&runtime)), path)
	})
	.expect("test failed")
}
