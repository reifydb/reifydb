// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Console logging backend with colored output

use std::io::{self, Write};

use parking_lot::Mutex;
use reifydb_core::{
	interface::logging::{LogBackend, LogLevel, Record},
	util::colored::*,
};
use reifydb_type::Result;

/// Console backend for logging
#[derive(Debug)]
pub struct ConsoleBackend {
	/// Whether to use colored output
	use_color: bool,
	/// Output stream (stdout or stderr)
	stderr_for_errors: bool,
	/// Mutex for synchronized output
	stdout_lock: Mutex<io::Stdout>,
	stderr_lock: Mutex<io::Stderr>,
}

impl ConsoleBackend {
	pub fn new() -> Self {
		Self {
			use_color: true,
			stderr_for_errors: true,
			stdout_lock: Mutex::new(io::stdout()),
			stderr_lock: Mutex::new(io::stderr()),
		}
	}

	pub fn with_color(mut self, use_color: bool) -> Self {
		self.use_color = use_color;
		self
	}

	pub fn with_stderr_for_errors(mut self, stderr_for_errors: bool) -> Self {
		self.stderr_for_errors = stderr_for_errors;
		self
	}

	fn format_module(&self, module: &str) -> String {
		// If module contains "::", take everything after the last "::"
		if let Some(pos) = module.rfind("::") {
			let after_colons = &module[pos + 2..];
			after_colons.to_string()
		} else {
			// No "::" found, use the module as is
			module.to_string()
		}
	}
}

impl Default for ConsoleBackend {
	fn default() -> Self {
		Self::new()
	}
}

impl LogBackend for ConsoleBackend {
	fn name(&self) -> &str {
		"console"
	}

	fn write(&self, records: &[Record]) -> Result<()> {
		let mut stdout_records = Vec::new();
		let mut stderr_records = Vec::new();

		for record in records {
			if record.level == LogLevel::Off {
				continue;
			}

			// Format: 2025-11-20 14:32:45.123 [INFO] module_name: Log message {field1=value1}
			let timestamp = format!(
				"{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
				record.timestamp.year(),
				record.timestamp.month(),
				record.timestamp.day(),
				record.timestamp.hour(),
				record.timestamp.minute(),
				record.timestamp.second(),
				record.timestamp.timestamp_millis() % 1000
			);

			let level_str = match record.level {
				LogLevel::Off => unreachable!(),
				LogLevel::Trace => "TRACE",
				LogLevel::Debug => "DEBUG",
				LogLevel::Info => "INFO",
				LogLevel::Warn => "WARN",
				LogLevel::Error => "ERROR",
				LogLevel::Critical => "CRITICAL",
			};

			let module = self.format_module(&record.module);

			// Colorize only the log level
			let colored_level = if self.use_color {
				match record.level {
					LogLevel::Off => unreachable!(),
					LogLevel::Trace => level_str.bright_black().to_string(),
					LogLevel::Debug => level_str.bright_blue().to_string(),
					LogLevel::Info => level_str.green().to_string(),
					LogLevel::Warn => level_str.yellow().to_string(),
					LogLevel::Error => level_str.red().to_string(),
					LogLevel::Critical => level_str.bright_magenta().bold().to_string(),
				}
			} else {
				level_str.to_string()
			};

			// Build the log line with default color text and only the level colorized
			let log_line = if self.use_color {
				// Build with colored level, everything else uses default terminal color
				let mut line =
					format!("{} [{}] {}: {}", timestamp, colored_level, module, record.message);

				// Add structured fields if present
				if !record.fields.is_empty() {
					let fields: Vec<String> =
						record.fields.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
					line.push_str(&format!(" {{{}}}", fields.join(", ")));
				}
				line
			} else {
				let mut line = format!("{} [{}] {}: {}", timestamp, level_str, module, record.message);

				// Add structured fields if present
				if !record.fields.is_empty() {
					let fields: Vec<String> =
						record.fields.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
					line.push_str(&format!(" {{{}}}", fields.join(", ")));
				}
				line
			};

			let formatted = format!("{}\n", log_line);

			// Route to stderr or stdout
			if self.stderr_for_errors && record.level >= LogLevel::Error {
				stderr_records.push(formatted);
			} else {
				stdout_records.push(formatted);
			}
		}

		if !stdout_records.is_empty() {
			let mut stdout = self.stdout_lock.lock();
			for record in stdout_records {
				// Best effort - ignore errors on console output
				let _ = stdout.write_all(record.as_bytes());
			}
			let _ = stdout.flush();
		}

		if !stderr_records.is_empty() {
			let mut stderr = self.stderr_lock.lock();
			for record in stderr_records {
				// Best effort - ignore errors on console output
				let _ = stderr.write_all(record.as_bytes());
			}
			let _ = stderr.flush();
		}

		Ok(())
	}
}
