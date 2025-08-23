// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error, fmt::Write, path::Path};

use reifydb::engine::StandardTransaction;
use reifydb::{
	core::{
		hook::Hooks,
		interface::{
			CdcTransaction, Params, UnversionedTransaction,
			VersionedTransaction,
		},
		retry,
	}, memory,
	network::grpc::{client::GrpcClient, server::GrpcConfig},
	optimistic,
	Database,
	ServerBuilder,
};
use reifydb_testing::{network::busy_wait, testscript, testscript::Command};
use test_each_file::test_each_path;
use tokio::runtime::Runtime;

pub struct GrpcRunner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	instance: Option<Database<StandardTransaction<VT, UT, C>>>,
	client: Option<GrpcClient>,
	runtime: Option<Runtime>,
}

impl<VT, UT, C> GrpcRunner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	pub fn new(input: (VT, UT, C, Hooks)) -> Self {
		let (versioned, unversioned, cdc, hooks) = input;
		let instance =
			ServerBuilder::new(versioned, unversioned, cdc, hooks)
				.with_grpc(GrpcConfig {
					socket: Some("[::1]:0"
						.parse()
						.unwrap()),
				})
				.build()
				.unwrap();

		Self {
			instance: Some(instance),
			client: None,
			runtime: None,
		}
	}
}

impl<VT, UT, C> testscript::Runner for GrpcRunner<VT, UT, C>
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

				let Some(runtime) = &self.runtime else {
					panic!()
				};

				runtime.block_on(async {
					for frame in self
						.client
						.as_ref()
						.unwrap()
						.command(&rql, Params::None)
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

				let Some(runtime) = &self.runtime else {
					panic!()
				};

				runtime.block_on(async {
					for frame in self
						.client
						.as_ref()
						.unwrap()
						.query(&rql, Params::None)
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
		let runtime = Runtime::new()?;
		let server = self.instance.as_mut().unwrap();
		server.start()?;
		let socket_addr = busy_wait(|| server.grpc_socket_addr());

		self.client = Some(GrpcClient {
			socket_addr,
		});
		self.runtime = Some(runtime);

		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		if let Some(mut server) = self.instance.take() {
			let _ = server.stop();
			drop(server);
		}

		if let Some(runtime) = self.runtime.take() {
			drop(runtime);
		}

		Ok(())
	}
}

test_each_path! { in "testsuite/regression/tests/scripts" as grpc => test_grpc }

fn test_grpc(path: &Path) {
	retry(3, || {
		testscript::run_path(
			&mut GrpcRunner::new(optimistic(memory())),
			path,
		)
	})
	.expect("test failed")
}
