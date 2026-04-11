// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg(not(reifydb_single_threaded))]

mod common;

use std::{error::Error, path::Path, sync::Arc};

use common::{cleanup_server, create_server_instance, start_server_and_get_http_port};
use reifydb::{Database, core::util::retry::retry};
use reifydb_client::{Encoding, HttpClient};
use reifydb_testing::{testscript, testscript::command::Command};
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

use crate::common::{parse_named_params, parse_positional_params, parse_rql, write_frames};

pub struct HttpRunner {
	instance: Option<Database>,
	client: Option<HttpClient>,
	runtime: Arc<Runtime>,
	encoding: Encoding,
}

impl HttpRunner {
	pub fn new(runtime: Arc<Runtime>, encoding: Encoding) -> Self {
		Self {
			instance: Some(create_server_instance(&runtime)),
			client: None,
			runtime,
			encoding,
		}
	}
}

impl testscript::runner::Runner for HttpRunner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let client = self.client.as_ref().ok_or("No client available")?;

		match command.name.as_str() {
			"admin" => {
				let rql = parse_rql(command);
				println!("admin: {rql}");

				let result = self.runtime.block_on(client.admin(&rql, None))?;
				write_frames(result.frames)
			}

			"command" => {
				let rql = parse_rql(command);
				println!("command: {rql}");

				let result = self.runtime.block_on(client.command(&rql, None))?;
				write_frames(result.frames)
			}

			"command_positional" => {
				let (rql, params) = parse_positional_params(command);
				println!("command_positional: {rql}");

				let result = self.runtime.block_on(client.command(&rql, Some(params)))?;
				write_frames(result.frames)
			}

			"command_named" => {
				let (rql, params) = parse_named_params(command);
				println!("command_named: {rql}");

				let result = self.runtime.block_on(client.command(&rql, Some(params)))?;
				write_frames(result.frames)
			}

			"query" => {
				let rql = parse_rql(command);
				println!("query: {rql}");

				let result = self.runtime.block_on(client.query(&rql, None))?;
				write_frames(result.frames)
			}

			"query_positional" => {
				let (rql, params) = parse_positional_params(command);
				println!("query_positional: {rql}");

				let result = self.runtime.block_on(client.query(&rql, Some(params)))?;
				write_frames(result.frames)
			}

			"query_named" => {
				let (rql, params) = parse_named_params(command);
				println!("query_named: {rql}");

				let result = self.runtime.block_on(client.query(&rql, Some(params)))?;
				write_frames(result.frames)
			}

			name => Err(format!("invalid command {name}").into()),
		}
	}

	fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
		let server = self.instance.as_mut().unwrap();
		let port = start_server_and_get_http_port(&self.runtime, server)?;

		let mut client =
			self.runtime.block_on(HttpClient::connect(&format!("http://[::1]:{}", port), self.encoding))?;
		client.authenticate("mysecrettoken");

		self.client = Some(client);

		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		self.client = None;
		cleanup_server(self.instance.take());
		Ok(())
	}
}

test_each_path! { in "pkg/rust/reifydb-client/tests/scripts" as scripts_http_json => test_http_json }
test_each_path! { in "pkg/rust/reifydb-client/tests/scripts" as scripts_http_rbcf => test_http_rbcf }

fn test_http_json(path: &Path) {
	retry(3, || {
		let runtime = Arc::new(Runtime::new().unwrap());
		let _guard = runtime.enter();
		testscript::runner::run_path(&mut HttpRunner::new(Arc::clone(&runtime), Encoding::Json), path)
	})
	.expect("test failed with Json");
}

fn test_http_rbcf(path: &Path) {
	retry(3, || {
		let runtime = Arc::new(Runtime::new().unwrap());
		let _guard = runtime.enter();
		testscript::runner::run_path(&mut HttpRunner::new(Arc::clone(&runtime), Encoding::Rbcf), path)
	})
	.expect("test failed with Rbcf");
}
