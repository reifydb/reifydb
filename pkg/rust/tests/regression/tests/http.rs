// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{error::Error, fmt::Write, path::Path, sync::Arc};

use reifydb::{Database, core::retry, server, sub_server_http::HttpConfig};
use reifydb_client::HttpClient;
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct HttpRunner {
	instance: Option<Database>,
	client: Option<HttpClient>,
	runtime: Arc<Runtime>,
}

impl HttpRunner {
	pub fn new(runtime: Arc<Runtime>) -> Self {
		let instance = server::memory()
			.with_http(HttpConfig::default().bind_addr("::1:0"))
			.build()
			.unwrap();

		Self {
			instance: Some(instance),
			client: None,
			runtime,
		}
	}
}

impl testscript::Runner for HttpRunner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();

		let client = self.client.as_ref().ok_or("No client available")?;

		match command.name.as_str() {
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

		let port = server.sub_server_http().unwrap().port().unwrap();

		let mut client = self.runtime.block_on(HttpClient::connect(&format!("http://[::1]:{}", port)))?;
		client.authenticate("mysecrettoken");

		self.client = Some(client);

		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		// Drop the client
		self.client = None;

		// Stop the server
		if let Some(mut server) = self.instance.take() {
			let _ = server.stop();
			drop(server);
		}

		Ok(())
	}
}

test_each_path! { in "pkg/rust/tests/regression/tests/scripts" as http => test_http }

fn test_http(path: &Path) {
	retry(3, || {
		let runtime = Arc::new(Runtime::new().unwrap());
		let _guard = runtime.enter();
		testscript::run_path(&mut HttpRunner::new(Arc::clone(&runtime)), path)
	})
	.expect("test failed")
}
