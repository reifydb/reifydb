// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{error::Error, fmt::Write, path::Path};

use reifydb::{
	Database, ServerBuilder,
	core::{event::EventBus, retry},
	memory, optimistic,
	sub_server::ServerConfig,
	transaction::{cdc::TransactionCdc, multi::TransactionMultiVersion, single::TransactionSingleVersion},
};
use reifydb_client::{Client, HttpBlockingSession, HttpClient};
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;

pub struct HttpRunner {
	instance: Option<Database>,
	client: Option<HttpClient>,
	session: Option<HttpBlockingSession>,
}

impl HttpRunner {
	pub fn new(input: (TransactionMultiVersion, TransactionSingleVersion, TransactionCdc, EventBus)) -> Self {
		let (multi, single, cdc, eventbus) = input;
		let instance = ServerBuilder::new(multi, single, cdc, eventbus)
			.with_config(ServerConfig::new()
				.http_bind_addr(Some("::1:0"))
				.ws_bind_addr(None::<&str>))
			.build()
			.unwrap();

		Self {
			instance: Some(instance),
			client: None,
			session: None,
		}
	}
}

impl testscript::Runner for HttpRunner {
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

		let port = server.sub_server().unwrap().http_port().unwrap();

		let client = Client::http_from_url(&format!("http://::1:{}", port))?;

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

test_each_path! { in "pkg/rust/tests/regression/tests/scripts" as http => test_http }

fn test_http(path: &Path) {
	retry(3, || testscript::run_path(&mut HttpRunner::new(optimistic(memory())), path)).expect("test failed")
}
