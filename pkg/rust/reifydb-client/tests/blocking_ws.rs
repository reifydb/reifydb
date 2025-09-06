// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT
mod common;

use std::{error::Error, path::Path};

use common::{cleanup_server, cleanup_ws_client, create_server_instance};
use reifydb::{
	Database,
	core::{
		event::EventBus,
		interface::{
			CdcTransaction, UnversionedTransaction,
			VersionedTransaction,
		},
		retry,
	},
	memory, optimistic,
};
use reifydb_client::{WsBlockingSession, WsClient};
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;

use crate::common::{
	parse_named_params, parse_positional_params, parse_rql, write_frames,
};

pub struct BlockingRunner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	instance: Option<Database<VT, UT, C>>,
	client: Option<WsClient>,
	session: Option<WsBlockingSession>,
}

impl<VT, UT, C> BlockingRunner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	pub fn new(input: (VT, UT, C, EventBus)) -> Self {
		Self {
			instance: Some(create_server_instance(input)),
			client: None,
			session: None,
		}
	}
}

impl<VT, UT, C> testscript::Runner for BlockingRunner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let session =
			self.session.as_mut().ok_or("No session available")?;

		match command.name.as_str() {
			"command" => {
				let rql = parse_rql(command);
				println!("command: {rql}");

				let result = session.command(&rql, None)?;
				write_frames(result.frames)
			}

			"command_positional" => {
				let (rql, params) =
					parse_positional_params(command);
				println!("command_positional: {rql}");

				let result =
					session.command(&rql, Some(params))?;
				write_frames(result.frames)
			}

			"command_named" => {
				let (rql, params) = parse_named_params(command);
				println!("command_named: {rql}");

				let result =
					session.command(&rql, Some(params))?;
				write_frames(result.frames)
			}

			"query" => {
				let rql = parse_rql(command);
				println!("query: {rql}");

				let result = session.query(&rql, None)?;
				write_frames(result.frames)
			}

			"query_positional" => {
				let (rql, params) =
					parse_positional_params(command);
				println!("query_positional: {rql}");

				let result =
					session.query(&rql, Some(params))?;
				write_frames(result.frames)
			}

			"query_named" => {
				let (rql, params) = parse_named_params(command);
				println!("query_named: {rql}");

				let result =
					session.query(&rql, Some(params))?;
				write_frames(result.frames)
			}

			name => Err(format!("invalid command {name}").into()),
		}
	}

	fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
		let server = self.instance.as_mut().unwrap();
		let port = common::start_server_and_get_port(server)?;

		let client = common::connect_ws(("::1", port))?;
		let session = client
			.blocking_session(Some("mysecrettoken".to_string()))?;

		self.client = Some(client);
		self.session = Some(session);

		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		// Drop the session first
		if let Some(session) = self.session.take() {
			drop(session);
		}

		cleanup_ws_client(self.client.take());
		cleanup_server(self.instance.take());
		Ok(())
	}
}

test_each_path! { in "pkg/rust/reifydb-client/tests/scripts" as blocking_ws => test_blocking }

fn test_blocking(path: &Path) {
	retry(3, || {
		testscript::run_path(
			&mut BlockingRunner::new(optimistic(memory())),
			path,
		)
	})
	.expect("test failed")
}
