// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! ReifyDB Logging System
//!
//! High-performance, extensible logging system built on top of the worker pool subsystem.
//! Supports multiple backends, structured logging, and automatic crate name formatting.

mod backend;
mod buffer;
mod builder;
mod macros;
mod processor;
mod record;
mod subsystem;

pub use backend::{ConsoleBackend, DatabaseBackend, LogBackend};
pub use builder::LoggingBuilder;
pub use record::{LogLevel, LogRecord};
pub use subsystem::LoggingSubsystem;

use crossbeam_channel::{SendError, Sender};
use std::sync::OnceLock;

/// Lightweight logger interface that sends logs to the subsystem
pub struct Logger {
	sender: Sender<LogRecord>,
}

impl Logger {
	/// Create a new logger with a channel to the subsystem
	pub fn new(sender: Sender<LogRecord>) -> Self {
		Self {
			sender,
		}
	}

	/// Log a record by sending it through the channel
	pub fn log(
		&self,
		record: LogRecord,
	) -> Result<(), SendError<LogRecord>> {
		self.sender.send(record)
	}
}

/// Global logger instance - lightweight, only holds a channel sender
static LOGGER: OnceLock<Logger> = OnceLock::new();

/// Initialize the global logger with a sender channel
/// This can only be called once - subsequent calls will be ignored
pub fn init_logger(sender: Sender<LogRecord>) {
	let _ = LOGGER.set(Logger::new(sender));
}

/// Get the global logger
pub fn logger() -> Option<&'static Logger> {
	LOGGER.get()
}

/// Send a log record through the global logger
pub fn log(record: LogRecord) {
	if let Some(logger) = logger() {
		// Ignore send errors - logging should not crash the application
		let _ = logger.log(record);
	}
}
