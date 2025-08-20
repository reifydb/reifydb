// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Log record types and structured logging support

use chrono::{DateTime, Utc};
use reifydb_core::util;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::thread::current;

/// Log severity levels
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
	Trace = 0,
	Debug = 1,
	Info = 2,
	Warn = 3,
	Error = 4,
	Critical = 5,
}

impl LogLevel {
	/// Convert to worker pool priority
	pub fn to_priority(
		&self,
	) -> reifydb_core::interface::worker_pool::Priority {
		use reifydb_core::interface::worker_pool::Priority;
		match self {
			LogLevel::Trace | LogLevel::Debug => Priority::Low,
			LogLevel::Info | LogLevel::Warn => Priority::Normal,
			LogLevel::Error | LogLevel::Critical => Priority::High,
		}
	}

	/// Check if this level requires synchronous delivery
	pub fn is_synchronous(&self) -> bool {
		matches!(self, LogLevel::Critical)
	}

	pub fn as_str(&self) -> &'static str {
		match self {
			LogLevel::Trace => "TRACE",
			LogLevel::Debug => "DEBUG",
			LogLevel::Info => "INFO",
			LogLevel::Warn => "WARN",
			LogLevel::Error => "ERROR",
			LogLevel::Critical => "CRITICAL",
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
pub struct LogRecord {
	/// Timestamp when the log was created
	pub timestamp: DateTime<Utc>,
	/// Log severity level
	pub level: LogLevel,
	/// Source module/crate (with reifydb- prefix stripped)
	pub module: String,
	/// Log message
	pub message: String,
	/// Structured fields (key-value pairs)
	pub fields: HashMap<String, serde_json::Value>,
	/// File location where log was generated
	pub file: Option<String>,
	/// Line number where log was generated
	pub line: Option<u32>,
	/// Thread ID that generated the log
	pub thread_id: String,
}

impl LogRecord {
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
		key: impl Into<String>,
		value: impl Serialize,
	) -> Self {
		if let Ok(json_value) = serde_json::to_value(value) {
			self.fields.insert(key.into(), json_value);
		}
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
