// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

use std::sync::mpsc;

use crate::{Request, Response, session::ResponseMessage};

/// Internal messages sent to the background thread
pub(crate) enum InternalMessage {
	Request {
		id: String,
		request: Request,
		route: ResponseRoute,
	},
	Close,
}

/// Routes responses to the appropriate session
pub(crate) enum ResponseRoute {
	Blocking(mpsc::Sender<Result<Response, String>>),
	Callback(Box<dyn FnOnce(Result<Response, String>) + Send>),
	Channel(mpsc::Sender<ResponseMessage>),
}
