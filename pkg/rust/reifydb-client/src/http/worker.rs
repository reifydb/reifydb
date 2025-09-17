// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{
	sync::mpsc,
	time::{Duration, Instant},
};

use crate::{
	http::{
		client::HttpClientConfig,
		message::{HttpInternalMessage, HttpResponseRoute},
		session::{HttpChannelResponse, HttpResponseMessage},
	},
	session::{CommandResult, QueryResult, convert_execute_response, convert_query_response},
};

/// HTTP worker thread that handles all requests for a client
pub(crate) fn http_worker_thread(client: HttpClientConfig, command_rx: mpsc::Receiver<HttpInternalMessage>) {
	// Process messages from the command channel with timeout to prevent
	// hanging
	loop {
		match command_rx.recv_timeout(Duration::from_millis(100)) {
			Ok(msg) => {
				match msg {
					HttpInternalMessage::Command {
						id,
						request,
						route,
					} => {
						let timestamp = Instant::now();

						// Send the HTTP request
						let response = match client.send_command(&request) {
							Ok(response) => Ok(HttpChannelResponse::Command {
								request_id: id.clone(),
								result: CommandResult {
									frames: convert_execute_response(response),
								},
							}),
							Err(e) => Err(e),
						};

						// Route the response
						match route {
							HttpResponseRoute::Channel(tx) => {
								let message = HttpResponseMessage {
									request_id: id,
									response,
									timestamp,
								};
								let _ = tx.send(message);
							}
						}
					}
					HttpInternalMessage::Query {
						id,
						request,
						route,
					} => {
						let timestamp = Instant::now();

						// Send the HTTP request
						let response = match client.send_query(&request) {
							Ok(response) => Ok(HttpChannelResponse::Query {
								request_id: id.clone(),
								result: QueryResult {
									frames: convert_query_response(response),
								},
							}),
							Err(e) => Err(e),
						};

						// Route the response
						match route {
							HttpResponseRoute::Channel(tx) => {
								let message = HttpResponseMessage {
									request_id: id,
									response,
									timestamp,
								};
								let _ = tx.send(message);
							}
						}
					}
					HttpInternalMessage::Auth {
						id,
						_token: _,
						route,
					} => {
						// For HTTP, authentication is
						// stateless, so we
						// just send a success response
						// In a real implementation,
						// this might send an auth
						// request to /v1/auth
						let timestamp = Instant::now();
						let response = Ok(HttpChannelResponse::Auth {
							request_id: id.clone(),
						});

						match route {
							HttpResponseRoute::Channel(tx) => {
								let message = HttpResponseMessage {
									request_id: id,
									response,
									timestamp,
								};
								let _ = tx.send(message);
							}
						}
					}
					HttpInternalMessage::Close => {
						break;
					}
				}
			}
			Err(mpsc::RecvTimeoutError::Timeout) => {
				// Continue loop to check for messages
				continue;
			}
			Err(mpsc::RecvTimeoutError::Disconnected) => {
				// Channel disconnected, exit worker
				break;
			}
		}
	}
}
