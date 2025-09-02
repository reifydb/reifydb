// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::{collections::HashMap, time::Instant};

use super::message::ResponseRoute;
use crate::{Response, session::ResponseMessage};

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
		ResponseRoute::Blocking(tx) => {
			let _ = tx.send(Ok(response));
		}
		ResponseRoute::Callback(callback) => {
			callback(Ok(response));
		}
		ResponseRoute::Channel(tx) => {
			let _ = tx.send(ResponseMessage {
				request_id: response.id.clone(),
				response: Ok(response),
				timestamp: Instant::now(),
			});
		}
	}
}
