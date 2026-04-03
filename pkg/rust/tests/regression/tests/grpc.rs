// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{error::Error, fmt::Write, path::Path, sync::Arc};

use reifydb::{Database, SharedRuntimeConfig, core::util::retry::retry, server};
use reifydb_client::GrpcClient;
use reifydb_testing::{testscript, testscript::command::Command};
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct GrpcRunner {
	instance: Option<Database>,
	client: Option<GrpcClient>,
	admin_client: Option<GrpcClient>,
	runtime: Arc<Runtime>,
}

impl GrpcRunner {
	pub fn new(runtime: Arc<Runtime>) -> Self {
		let instance = server::memory()
			.with_runtime_config(SharedRuntimeConfig::default().deterministic_testing(0))
			.with_grpc(|c| c.bind_addr("::1:0").admin_bind_addr("::1:0"))
			.build()
			.unwrap();

		Self {
			instance: Some(instance),
			client: None,
			admin_client: None,
			runtime,
		}
	}
}

impl testscript::runner::Runner for GrpcRunner {
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();

		let client = self.client.as_ref().ok_or("No client available")?;

		match command.name.as_str() {
			"admin" => {
				let admin_client = self.admin_client.as_ref().ok_or("No admin client available")?;
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("admin: {rql}");

				let result = self.runtime.block_on(admin_client.admin(&rql, None))?;
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
		server.admin_as_root(
			"CREATE AUTHENTICATION FOR root { method: token; token: 'mysecrettoken' }",
			reifydb_type::params::Params::None,
		)
		.unwrap();

		let grpc = server.sub_server_grpc().unwrap();
		let port = grpc.port().unwrap();
		let admin_port = grpc.admin_port().unwrap();

		let mut client = self.runtime.block_on(GrpcClient::connect(&format!("http://[::1]:{}", port)))?;
		client.authenticate("mysecrettoken");
		self.client = Some(client);

		let mut admin_client =
			self.runtime.block_on(GrpcClient::connect(&format!("http://[::1]:{}", admin_port)))?;
		admin_client.authenticate("mysecrettoken");
		self.admin_client = Some(admin_client);

		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		self.client = None;
		self.admin_client = None;

		if let Some(mut server) = self.instance.take() {
			let _ = server.stop();
			drop(server);
		}

		Ok(())
	}
}

test_each_path! { in "pkg/rust/tests/regression/tests/scripts" as grpc => test_grpc }

fn test_grpc(path: &Path) {
	retry(3, || {
		let runtime = Arc::new(Runtime::new().unwrap());
		let _guard = runtime.enter();
		testscript::runner::run_path(&mut GrpcRunner::new(Arc::clone(&runtime)), path)
	})
	.expect("test failed")
}
