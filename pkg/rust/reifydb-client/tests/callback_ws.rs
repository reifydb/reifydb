// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod common;

use std::{
	error::Error,
	path::Path,
	sync::{Arc, Mutex},
	thread,
	time::Duration,
};

use common::{
	cleanup_server, cleanup_ws_client, connect_ws, parse_named_params, parse_positional_params, parse_rql,
	start_server_and_get_port, write_frames,
};
use reifydb::{
	Database,
	core::{
		event::EventBus,
		interface::{CdcTransaction, MultiVersionTransaction, SingleVersionTransaction},
		retry,
	},
	memory, optimistic,
};
use reifydb_client::{
	WsCallbackSession, WsClient,
	session::{CommandResult, QueryResult},
};
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;
use thread::sleep;

use crate::common::create_server_instance;

pub struct CallbackRunner<MVT, SVT, C>
where
	MVT: MultiVersionTransaction,
	SVT: SingleVersionTransaction,
	C: CdcTransaction,
{
	instance: Option<Database<MVT, SVT, C>>,
	client: Option<WsClient>,
	session: Option<WsCallbackSession>,
	last_command_result: Arc<Mutex<Option<Result<CommandResult, String>>>>,
	last_query_result: Arc<Mutex<Option<Result<QueryResult, String>>>>,
}

impl<MVT, SVT, C> CallbackRunner<MVT, SVT, C>
where
	MVT: MultiVersionTransaction,
	SVT: SingleVersionTransaction,
	C: CdcTransaction,
{
	pub fn new(input: (MVT, SVT, C, EventBus)) -> Self {
		Self {
			instance: Some(create_server_instance(input)),
			client: None,
			session: None,
			last_command_result: Arc::new(Mutex::new(None)),
			last_query_result: Arc::new(Mutex::new(None)),
		}
	}
}

impl<MVT, SVT, C> testscript::Runner for CallbackRunner<MVT, SVT, C>
where
	MVT: MultiVersionTransaction,
	SVT: SingleVersionTransaction,
	C: CdcTransaction,
{
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let session = self.session.as_ref().ok_or("No session available")?;

		match command.name.as_str() {
			"command" => {
				let rql = parse_rql(command);
				println!("command: {rql}");

				let result_holder = self.last_command_result.clone();

				// Clear previous result
				*result_holder.lock().unwrap() = None;

				let _request_id = session.command(&rql, None, move |result| {
					let mut holder = result_holder.lock().unwrap();
					*holder = Some(result.map_err(|e| e.to_string()));
				})?;

				// Wait for callback to execute
				let mut attempts = 0;
				loop {
					sleep(Duration::from_millis(100));
					let result = self.last_command_result.lock().unwrap();
					if result.is_some() {
						break;
					}
					attempts += 1;
					if attempts > 50 {
						// 5 second timeout
						return Err("Callback timeout".into());
					}
				}

				// Get the result
				let result = self.last_command_result.lock().unwrap().take().unwrap();
				match result {
					Ok(command_result) => write_frames(command_result.frames),
					Err(e) => Err(e.into()),
				}
			}

			"query" => {
				let rql = parse_rql(command);
				println!("query: {rql}");

				let result_holder = self.last_query_result.clone();

				// Clear previous result
				*result_holder.lock().unwrap() = None;

				let _request_id = session.query(&rql, None, move |result| {
					let mut holder = result_holder.lock().unwrap();
					*holder = Some(result.map_err(|e| e.to_string()));
				})?;

				// Wait for callback to execute
				let mut attempts = 0;
				loop {
					sleep(Duration::from_millis(100));
					let result = self.last_query_result.lock().unwrap();
					if result.is_some() {
						break;
					}
					attempts += 1;
					if attempts > 50 {
						// 5 second timeout
						return Err("Callback timeout".into());
					}
				}

				// Get the result
				let result = self.last_query_result.lock().unwrap().take().unwrap();
				match result {
					Ok(query_result) => write_frames(query_result.frames),
					Err(e) => Err(e.into()),
				}
			}

			"command_positional" => {
				let (rql, params) = parse_positional_params(command);
				println!("command_positional: {rql}");

				let result_holder = self.last_command_result.clone();
				*result_holder.lock().unwrap() = None;

				let _request_id = session.command(&rql, Some(params), move |result| {
					let mut holder = result_holder.lock().unwrap();
					*holder = Some(result.map_err(|e| e.to_string()));
				})?;

				// Wait for callback to execute
				let mut attempts = 0;
				loop {
					sleep(Duration::from_millis(100));
					let result = self.last_command_result.lock().unwrap();
					if result.is_some() {
						break;
					}
					attempts += 1;
					if attempts > 50 {
						return Err("Callback timeout".into());
					}
				}

				let result = self.last_command_result.lock().unwrap().take().unwrap();
				match result {
					Ok(command_result) => write_frames(command_result.frames),
					Err(e) => Err(e.into()),
				}
			}

			"command_named" => {
				let (rql, params) = parse_named_params(command);
				println!("command_named: {rql}");

				let result_holder = self.last_command_result.clone();
				*result_holder.lock().unwrap() = None;

				let _request_id = session.command(&rql, Some(params), move |result| {
					let mut holder = result_holder.lock().unwrap();
					*holder = Some(result.map_err(|e| e.to_string()));
				})?;

				// Wait for callback to execute
				let mut attempts = 0;
				loop {
					sleep(Duration::from_millis(100));
					let result = self.last_command_result.lock().unwrap();
					if result.is_some() {
						break;
					}
					attempts += 1;
					if attempts > 50 {
						return Err("Callback timeout".into());
					}
				}

				let result = self.last_command_result.lock().unwrap().take().unwrap();
				match result {
					Ok(command_result) => write_frames(command_result.frames),
					Err(e) => Err(e.into()),
				}
			}

			"query_positional" => {
				let (rql, params) = parse_positional_params(command);
				println!("query_positional: {rql}");

				let result_holder = self.last_query_result.clone();
				*result_holder.lock().unwrap() = None;

				let _request_id = session.query(&rql, Some(params), move |result| {
					let mut holder = result_holder.lock().unwrap();
					*holder = Some(result.map_err(|e| e.to_string()));
				})?;

				// Wait for callback to execute
				let mut attempts = 0;
				loop {
					sleep(Duration::from_millis(100));
					let result = self.last_query_result.lock().unwrap();
					if result.is_some() {
						break;
					}
					attempts += 1;
					if attempts > 50 {
						return Err("Callback timeout".into());
					}
				}

				let result = self.last_query_result.lock().unwrap().take().unwrap();
				match result {
					Ok(query_result) => write_frames(query_result.frames),
					Err(e) => Err(e.into()),
				}
			}

			"query_named" => {
				let (rql, params) = parse_named_params(command);
				println!("query_named: {rql}");

				let result_holder = self.last_query_result.clone();
				*result_holder.lock().unwrap() = None;

				let _request_id = session.query(&rql, Some(params), move |result| {
					let mut holder = result_holder.lock().unwrap();
					*holder = Some(result.map_err(|e| e.to_string()));
				})?;

				// Wait for callback to execute
				let mut attempts = 0;
				loop {
					sleep(Duration::from_millis(100));
					let result = self.last_query_result.lock().unwrap();
					if result.is_some() {
						break;
					}
					attempts += 1;
					if attempts > 50 {
						return Err("Callback timeout".into());
					}
				}

				let result = self.last_query_result.lock().unwrap().take().unwrap();
				match result {
					Ok(query_result) => write_frames(query_result.frames),
					Err(e) => Err(e.into()),
				}
			}

			name => Err(format!("invalid command {name}").into()),
		}
	}

	fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
		let server = self.instance.as_mut().unwrap();
		let port = start_server_and_get_port(server)?;

		let client = connect_ws(("::1", port))?;
		let session = client.callback_session(Some("mysecrettoken".to_string()))?;

		self.client = Some(client);
		self.session = Some(session);

		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		if let Some(session) = self.session.take() {
			drop(session);
		}

		cleanup_ws_client(self.client.take());
		cleanup_server(self.instance.take());
		Ok(())
	}
}

test_each_path! { in "pkg/rust/reifydb-client/tests/scripts" as callback_ws => test_callback }

fn test_callback(path: &Path) {
	retry(3, || testscript::run_path(&mut CallbackRunner::new(optimistic(memory())), path)).expect("test failed")
}
