// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::sync::mpsc;

use crate::{ws::ResponseMessage, Request};

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
	Channel(mpsc::Sender<ResponseMessage>),
}
