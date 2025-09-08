// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod common;

use std::{error::Error, path::Path, sync::mpsc::Receiver, time::Duration};

use common::{
	cleanup_server, cleanup_ws_client, create_server_instance,
	parse_named_params, parse_positional_params, parse_rql, write_frames,
};
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
use reifydb_client::{
	ChannelResponse, ResponseMessage, WsChannelSession, WsClient,
};
use reifydb_testing::{testscript, testscript::Command};
use test_each_file::test_each_path;

pub struct ChannelRunner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	instance: Option<Database<VT, UT, C>>,
	client: Option<WsClient>,
	session: Option<WsChannelSession>,
	receiver: Option<Receiver<ResponseMessage>>,
}

impl<VT, UT, C> ChannelRunner<VT, UT, C>
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
			receiver: None,
		}
	}
}

impl<VT, UT, C> testscript::Runner for ChannelRunner<VT, UT, C>
where
	VT: VersionedTransaction,
	UT: UnversionedTransaction,
	C: CdcTransaction,
{
	fn run(&mut self, command: &Command) -> Result<String, Box<dyn Error>> {
		let session =
			self.session.as_ref().ok_or("No session available")?;
		let receiver = self
			.receiver
			.as_ref()
			.ok_or("No receiver available")?;

		match command.name.as_str() {
			"command" => {
				let rql = parse_rql(command);
				println!("command: {rql}");

				let request_id = session.command(&rql, None)?;

				// Wait for response
				match receiver.recv_timeout(Duration::from_secs(5)) {
					Ok(msg) => {
						match msg.response {
							Ok(ChannelResponse::Command { request_id: resp_id, result }) => {
								if resp_id != request_id {
									return Err(format!(
										"Unexpected request_id: {} (expected {})",
										resp_id, request_id
									).into());
								}
								write_frames(result.frames)
							}
							Ok(_) => {
								Err("Unexpected response type for command".into())
							}
							Err(e) => {
								Err(e.to_string().into())
							}
						}
					}
					Err(e) => {
						Err(format!("Failed to receive response: {}", e).into())
					}
				}
			}

			"command_positional" => {
				let (rql, params) =
					parse_positional_params(command);
				println!("command_positional: {rql}");

				let request_id =
					session.command(&rql, Some(params))?;

				// Wait for response
				match receiver.recv_timeout(Duration::from_secs(5)) {
					Ok(msg) => {
						match msg.response {
							Ok(ChannelResponse::Command { request_id: resp_id, result }) => {
								if resp_id != request_id {
									return Err(format!(
										"Unexpected request_id: {} (expected {})",
										resp_id, request_id
									).into());
								}
								write_frames(result.frames)
							}
							Ok(_) => {
								Err("Unexpected response type for command".into())
							}
							Err(e) => {
								Err(e.to_string().into())
							}
						}
					}
					Err(e) => {
						Err(format!("Failed to receive response: {}", e).into())
					}
				}
			}

			"command_named" => {
				let (rql, params) = parse_named_params(command);
				println!("command_named: {rql}");

				let request_id =
					session.command(&rql, Some(params))?;

				// Wait for response
				match receiver.recv_timeout(Duration::from_secs(5)) {
					Ok(msg) => {
						match msg.response {
							Ok(ChannelResponse::Command { request_id: resp_id, result }) => {
								if resp_id != request_id {
									return Err(format!(
										"Unexpected request_id: {} (expected {})",
										resp_id, request_id
									).into());
								}
								write_frames(result.frames)
							}
							Ok(_) => {
								Err("Unexpected response type for command".into())
							}
							Err(e) => {
								Err(e.to_string().into())
							}
						}
					}
					Err(e) => {
						Err(format!("Failed to receive response: {}", e).into())
					}
				}
			}

			"query" => {
				let rql = parse_rql(command);
				println!("query: {rql}");

				let request_id = session.query(&rql, None)?;

				// Wait for response
				match receiver.recv_timeout(Duration::from_secs(5)) {
					Ok(msg) => {
						match msg.response {
							Ok(ChannelResponse::Query { request_id: resp_id, result }) => {
								if resp_id != request_id {
									return Err(format!(
										"Unexpected request_id: {} (expected {})",
										resp_id, request_id
									).into());
								}
								write_frames(result.frames)
							}
							Ok(_) => {
								Err("Unexpected response type for query".into())
							}
							Err(e) => {
								Err(e.to_string().into())
							}
						}
					}
					Err(e) => {
						Err(format!("Failed to receive response: {}", e).into())
					}
				}
			}

			"query_positional" => {
				let (rql, params) =
					parse_positional_params(command);
				println!("query_positional: {rql}");

				let request_id =
					session.query(&rql, Some(params))?;

				// Wait for response
				match receiver.recv_timeout(Duration::from_secs(5)) {
					Ok(msg) => {
						match msg.response {
							Ok(ChannelResponse::Query { request_id: resp_id, result }) => {
								if resp_id != request_id {
									return Err(format!(
										"Unexpected request_id: {} (expected {})",
										resp_id, request_id
									).into());
								}
								write_frames(result.frames)
							}
							Ok(_) => {
								Err("Unexpected response type for query".into())
							}
							Err(e) => {
								Err(e.to_string().into())
							}
						}
					}
					Err(e) => {
						Err(format!("Failed to receive response: {}", e).into())
					}
				}
			}

			"query_named" => {
				let (rql, params) = parse_named_params(command);
				println!("query_named: {rql}");

				let request_id =
					session.query(&rql, Some(params))?;

				// Wait for response
				match receiver.recv_timeout(Duration::from_secs(5)) {
					Ok(msg) => {
						match msg.response {
							Ok(ChannelResponse::Query { request_id: resp_id, result }) => {
								if resp_id != request_id {
									return Err(format!(
										"Unexpected request_id: {} (expected {})",
										resp_id, request_id
									).into());
								}
								write_frames(result.frames)
							}
							Ok(_) => {
								Err("Unexpected response type for query".into())
							}
							Err(e) => {
								Err(e.to_string().into())
							}
						}
					}
					Err(e) => {
						Err(format!("Failed to receive response: {}", e).into())
					}
				}
			}

			name => Err(format!("invalid command {name}").into()),
		}
	}

	fn start_script(&mut self) -> Result<(), Box<dyn Error>> {
		let server = self.instance.as_mut().unwrap();
		let port = common::start_server_and_get_port(server)?;

		let client = common::connect_ws(("::1", port))?;
		let (session, receiver) = client
			.channel_session(Some("mysecrettoken".to_string()))?;

		// Consume the authentication response
		match receiver.recv_timeout(Duration::from_millis(100)) {
			Ok(msg) => {
				match msg.response {
					Ok(ChannelResponse::Auth {
						..
					}) => {
						// Authentication successful
					}
					Ok(_) => {
						return Err("Expected Auth response, got different type".into());
					}
					Err(e) => {
						return Err(format!(
							"Authentication failed: {}",
							e
						)
						.into());
					}
				}
			}
			Err(_) => {
				// No auth response (shouldn't happen with
				// token)
			}
		}

		self.client = Some(client);
		self.session = Some(session);
		self.receiver = Some(receiver);

		Ok(())
	}

	fn end_script(&mut self) -> Result<(), Box<dyn Error>> {
		// Drop the session and receiver first
		if let Some(session) = self.session.take() {
			drop(session);
		}

		if let Some(receiver) = self.receiver.take() {
			drop(receiver);
		}

		cleanup_ws_client(self.client.take());
		cleanup_server(self.instance.take());
		Ok(())
	}
}

test_each_path! { in "pkg/rust/reifydb-client/tests/scripts" as channel_ws => test_channel }

fn test_channel(path: &Path) {
	retry(3, || {
		testscript::run_path(
			&mut ChannelRunner::new(optimistic(memory())),
			path,
		)
	})
	.expect("test failed")
}
