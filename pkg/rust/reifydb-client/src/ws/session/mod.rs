// Copyright (c) reifydb.com 2025
// This file is licensed under the MIT

mod blocking;
mod callback;
mod channel;

use std::time::Instant;

pub use blocking::BlockingSession;
pub use callback::CallbackSession;
pub use channel::ChannelSession;
use reifydb_type::Error;

// Re-export common types from session module
pub use crate::session::{CommandResult, QueryResult};

/// Channel response enum for different response types (WebSocket-specific)
#[derive(Debug)]
pub enum ChannelResponse {
	/// Authentication response
	Auth {
		request_id: String,
	},
	/// Command execution response with frames
	Command {
		request_id: String,
		result: CommandResult,
	},
	/// Query execution response with frames
	Query {
		request_id: String,
		result: QueryResult,
	},
}

/// Response message for channel sessions (WebSocket-specific)
#[derive(Debug)]
pub struct ResponseMessage {
	pub request_id: String,
	pub response: Result<ChannelResponse, Error>,
	pub timestamp: Instant,
}
