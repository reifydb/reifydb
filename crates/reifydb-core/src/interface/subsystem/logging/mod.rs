// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::util;
use crate::value::{IntoValue, Value};
use chrono::{DateTime, Utc};
use crossbeam_channel::{SendError, Sender};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use std::sync::OnceLock;
use std::thread::current;

mod macros;
pub mod mock;
pub mod timed;

#[derive(
	Debug,
	Clone,
	Copy,
	PartialEq,
	Eq,
	PartialOrd,
	Ord,
	Serialize,
	Deserialize,
)]
pub enum LogLevel {
	Off = 0,
	Trace = 1,
	Debug = 2,
	Info = 3,
	Warn = 4,
	Error = 5,
	Critical = 6,
}

impl LogLevel {
	pub fn as_str(&self) -> &'static str {
		match self {
			LogLevel::Off => "off",
			LogLevel::Trace => "trace",
			LogLevel::Debug => "debug",
			LogLevel::Info => "info",
			LogLevel::Warn => "warn",
			LogLevel::Error => "error",
			LogLevel::Critical => "critical",
		}
	}
}

impl fmt::Display for LogLevel {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.as_str())
	}
}

/// Structured log record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
	/// Timestamp when the log was created
	pub timestamp: DateTime<Utc>,
	/// Log severity level
	pub level: LogLevel,
	/// Source module/crate (with reifydb- prefix stripped)
	pub module: String,
	/// Log message
	pub message: String,
	/// Structured fields (key-value pairs using ReifyDB Values)
	pub fields: HashMap<Value, Value>,
	/// File location where log was generated
	pub file: Option<String>,
	/// Line number where log was generated
	pub line: Option<u32>,
	/// Thread ID that generated the log
	pub thread_id: String,
}

impl Record {
	pub fn new(
		level: LogLevel,
		module: impl Into<String>,
		message: impl Into<String>,
	) -> Self {
		Self {
			timestamp: util::now(),
			level,
			module: module.into(),
			message: message.into(),
			fields: HashMap::new(),
			file: None,
			line: None,
			thread_id: format!("{:?}", current().id()),
		}
	}

	pub fn with_field(
		mut self,
		key: impl IntoValue,
		value: impl IntoValue,
	) -> Self {
		self.fields.insert(key.into_value(), value.into_value());
		self
	}

	pub fn with_location(
		mut self,
		file: impl Into<String>,
		line: u32,
	) -> Self {
		self.file = Some(file.into());
		self.line = Some(line);
		self
	}
}

pub trait LogBackend: Send + Sync + Debug {
	fn name(&self) -> &str;

	fn write(&self, records: &[Record]) -> crate::Result<()>;

	fn flush(&self) -> crate::Result<()> {
		Ok(())
	}
}

pub struct Logger {
	sender: Sender<Record>,
}

impl Logger {
	/// Create a new logger with a channel to the subsystem
	pub fn new(sender: Sender<Record>) -> Self {
		Self {
			sender,
		}
	}

	/// Log a record by sending it through the channel
	pub fn log(&self, record: Record) -> Result<(), SendError<Record>> {
		self.sender.send(record)
	}
}

/// Global logger instance - lightweight, only holds a channel sender
static LOGGER: OnceLock<Logger> = OnceLock::new();

/// Initialize the global logger with a sender channel
/// This can only be called once - subsequent calls will be ignored
pub fn init_logger(sender: Sender<Record>) {
	let _ = LOGGER.set(Logger::new(sender));
}

/// Get the global logger
pub fn logger() -> Option<&'static Logger> {
	LOGGER.get()
}

/// Send a log record through the global logger
/// In debug builds, checks for a thread-local mock logger first
pub fn log(record: Record) {
	// Check for mock logger in debug builds
	#[cfg(debug_assertions)]
	{
		if let Some(sender) = mock::get_mock_logger() {
			// Send to mock logger instead of global logger
			let _ = sender.send(record);
			return;
		}
	}

	// Normal path: use global logger
	if let Some(logger) = logger() {
		// Ignore send errors - logging should not crash the application
		let _ = logger.log(record);
	}
}
