// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub mod http;
pub mod utils;
pub mod ws;

use std::fmt;

pub use http::HttpHandler;
use reifydb_core::interface::Transaction;
pub use ws::WebSocketHandler;

use crate::core::Connection;

/// Result type for protocol operations
pub type ProtocolResult<T> = Result<T, ProtocolError>;

/// Errors that can occur during protocol handling
#[derive(Debug)]
pub enum ProtocolError {
	Io(std::io::Error),
	InvalidFrame,
	InvalidHandshake,
	UnsupportedProtocol,
	ConnectionClosed,
	BufferOverflow,
	Custom(String),
}

impl fmt::Display for ProtocolError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Io(e) => write!(f, "I/O error: {}", e),
			Self::InvalidFrame => {
				write!(f, "Invalid protocol frame")
			}
			Self::InvalidHandshake => {
				write!(f, "Invalid protocol handshake")
			}
			Self::UnsupportedProtocol => {
				write!(f, "Unsupported protocol")
			}
			Self::ConnectionClosed => {
				write!(f, "Connection closed")
			}
			Self::BufferOverflow => write!(f, "Buffer overflow"),
			Self::Custom(msg) => write!(f, "{}", msg),
		}
	}
}

impl std::error::Error for ProtocolError {}

impl From<std::io::Error> for ProtocolError {
	fn from(err: std::io::Error) -> Self {
		Self::Io(err)
	}
}

/// Trait for protocol handlers
pub trait ProtocolHandler<T: Transaction>: Send + Sync {
	/// Protocol name for identification
	fn name(&self) -> &'static str;

	/// Detect if this protocol can handle the given connection buffer
	fn can_handle(&self, buffer: &[u8]) -> bool;

	/// Handle a connection using this protocol
	fn handle_connection(
		&self,
		conn: &mut Connection<T>,
	) -> ProtocolResult<()>;

	/// Handle readable events for this connection
	fn handle_read(&self, conn: &mut Connection<T>) -> ProtocolResult<()>;

	/// Handle writable events for this connection
	fn handle_write(&self, conn: &mut Connection<T>) -> ProtocolResult<()>;

	/// Check if the connection should be closed
	fn should_close(&self, conn: &Connection<T>) -> bool;
}
