// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error, fmt::Write, path::Path};

use reifydb::{
	Database, ServerBuilder,
	core::{
		event::EventBus,
		interface::{CdcTransaction, UnversionedTransaction, VersionedTransaction},
		retry,
	},
	memory, optimistic,
	sub_server::ServerConfig,
};
use reifydb_client::{Client, WsBlockingSession, WsClient};
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;

pub struct WsRunner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	instance: Option<Database<VT, UT, C>>,
	client: Option<WsClient>,
	session: Option<WsBlockingSession>,
}

impl<VT, UT, C> WsRunner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	pub fn new(input: (VT, UT, C, EventBus)) -> Self {
		let (versioned, unversioned, cdc, eventbus) = input;
		let instance = ServerBuilder::new(versioned, unversioned, cdc, eventbus)
			.with_config(ServerConfig::new().bind_addr("::1:0"))
			.build()
			.unwrap();

		Self {
			instance: Some(instance),
			client: None,
			session: None,
		}
	}
}

impl<VT, UT, C> testscript::Runner for WsRunner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let mut output = String::new();

		let session = self.session.as_mut().ok_or("No session available")?;

		match command.name.as_str() {
			"command" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("command: {rql}");

				let result = session.command(&rql, None)?;
				for frame in result.frames {
					writeln!(output, "{}", frame).unwrap();
				}
			}

			"query" => {
				let rql = command.args.iter().map(|a| a.value.as_str()).collect::<Vec<_>>().join(" ");

				println!("query: {rql}");

				let result = session.query(&rql, None)?;
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

		let port = server.sub_server().unwrap().port().unwrap();

		let client = Client::ws_from_url(&format!("ws://::1:{}", port))?;

		let session = client.blocking_session(Some("mysecrettoken".to_string()))?;

		self.client = Some(client);
		self.session = Some(session);

		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		// Drop the session first
		if let Some(session) = self.session.take() {
			drop(session);
		}

		// Close the client connection
		if let Some(client) = self.client.take() {
			let _ = client.close();
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
	retry(3, || testscript::run_path(&mut WsRunner::new(optimistic(memory())), path)).expect("test failed")
}
