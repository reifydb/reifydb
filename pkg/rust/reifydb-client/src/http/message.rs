// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::sync::mpsc;

use crate::{http::session::HttpResponseMessage, CommandRequest, QueryRequest};

/// Internal messages sent to the HTTP worker thread
pub(crate) enum HttpInternalMessage {
	Command {
		id: String,
		request: CommandRequest,
		route: HttpResponseRoute,
	},
	Query {
		id: String,
		request: QueryRequest,
		route: HttpResponseRoute,
	},
	Auth {
		id: String,
		_token: Option<String>,
		route: HttpResponseRoute,
	},
	Close,
}

/// Routes HTTP responses to the appropriate session
pub(crate) enum HttpResponseRoute {
	Channel(mpsc::Sender<HttpResponseMessage>),
}
