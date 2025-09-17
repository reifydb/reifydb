// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT, see license.md file

use std::{collections::HashMap, time::Instant};

use crate::{
	Response, ResponsePayload,
	session::{parse_command_response, parse_query_response},
	ws::{ChannelResponse, ResponseMessage, message::ResponseRoute},
};

/// Routes responses to the appropriate session
pub(crate) struct RequestRouter {
	pub routes: HashMap<String, ResponseRoute>,
}

impl RequestRouter {
	pub fn new() -> Self {
		Self {
			routes: HashMap::new(),
		}
	}

	pub fn add_route(&mut self, id: String, route: ResponseRoute) {
		self.routes.insert(id, route);
	}

	pub fn remove_route(&mut self, id: &str) -> Option<ResponseRoute> {
		self.routes.remove(id)
	}
}

/// Route an error to the appropriate destination
pub(crate) fn route_error(id: &str, error: String, _route: ResponseRoute) {
	panic!("Route error: {} - {}", id, error);
	// let err = Err(Error::new(error));
	// match route {
	// 	ResponseRoute::Blocking(tx) => {
	// 		let _ = tx.send(err);
	// 	}
	// 	ResponseRoute::Callback(callback) => {
	// 		callback(err);
	// 	}
	// 	ResponseRoute::Channel(tx) => {
	// 		let _ = tx.send(ResponseMessage {
	// 			request_id: id.to_string(),
	// 			response: err,
	// 			timestamp: Instant::now(),
	// 		});
	// 	}
	// }
}

/// Route a successful response to the appropriate destination
pub(crate) fn route_response(response: Response, route: ResponseRoute) {
	match route {
		ResponseRoute::Channel(tx) => {
			// Parse the response based on its type
			let request_id = response.id.clone();
			let channel_response = match response.payload {
				ResponsePayload::Auth(_) => Ok(ChannelResponse::Auth {
					request_id: request_id.clone(),
				}),
				ResponsePayload::Command(_) => {
					parse_command_response(response).map(|result| ChannelResponse::Command {
						request_id: request_id.clone(),
						result,
					})
				}
				ResponsePayload::Query(_) => {
					parse_query_response(response).map(|result| ChannelResponse::Query {
						request_id: request_id.clone(),
						result,
					})
				}
				ResponsePayload::Err(ref err) => {
					reifydb_type::err!(err.diagnostic.clone())
				}
			};

			let _ = tx.send(ResponseMessage {
				request_id,
				response: channel_response,
				timestamp: Instant::now(),
			});
		}
	}
}
