// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Console logging backend with colored output

use colored::*;
use parking_lot::Mutex;
use reifydb_core::interface::subsystem::logging::{LogBackend, LogLevel, Record};
use reifydb_core::Result;
use std::io::{self, Write};

/// Format style for console output
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FormatStyle {
	/// Box drawing style (original format)
	Box,
	/// Timeline with module branching
	Timeline,
}

/// Console backend for logging
#[derive(Debug)]
pub struct ConsoleBackend {
	/// Whether to use colored output
	use_color: bool,
	/// Output stream (stdout or stderr)
	stderr_for_errors: bool,
	/// Format style
	format_style: FormatStyle,
	/// Last timestamp for timeline format (second precision in seconds since epoch)
	last_second: Mutex<Option<i64>>,
	/// Last module for grouping consecutive logs
	last_module: Mutex<Option<String>>,
	/// Mutex for synchronized output
	stdout_lock: Mutex<io::Stdout>,
	stderr_lock: Mutex<io::Stderr>,
}

impl ConsoleBackend {
	pub fn new() -> Self {
		Self {
			use_color: true,
			stderr_for_errors: true,
			format_style: FormatStyle::Box,
			last_second: Mutex::new(None),
			last_module: Mutex::new(None),
			stdout_lock: Mutex::new(io::stdout()),
			stderr_lock: Mutex::new(io::stderr()),
		}
	}

	pub fn with_color(mut self, use_color: bool) -> Self {
		self.use_color = use_color;
		self
	}

	pub fn with_stderr_for_errors(
		mut self,
		stderr_for_errors: bool,
	) -> Self {
		self.stderr_for_errors = stderr_for_errors;
		self
	}

	pub fn with_format_style(mut self, format_style: FormatStyle) -> Self {
		self.format_style = format_style;
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

	fn format_timeline_records(&self, records: &[Record]) -> Vec<String> {
		let mut output = Vec::new();
		let mut last_second = self.last_second.lock();
		let mut last_module = self.last_module.lock();
		let mut current_group: Vec<&Record> = Vec::new();
		let mut current_module: Option<String> = None;

		for record in records {
			if record.level == LogLevel::Off {
				continue;
			}

			let module = self.format_module(&record.module);
			
			// Check if this is a new module or we need to flush the current group
			if current_module.as_ref() != Some(&module) {
				// Flush the current group if any
				if !current_group.is_empty() {
					if let Some(ref mod_name) = current_module {
						output.push(self.format_timeline_group(&current_group, mod_name, &mut last_second));
					}
				}
				// Start a new group
				current_module = Some(module.clone());
				current_group = vec![record];
			} else {
				// Add to current group
				current_group.push(record);
			}
		}

		// Flush any remaining group
		if !current_group.is_empty() {
			if let Some(ref mod_name) = current_module {
				output.push(self.format_timeline_group(&current_group, mod_name, &mut last_second));
			}
		}

		*last_module = current_module;
		output
	}

	fn format_timeline_group(&self, records: &[&Record], module: &str, last_second: &mut Option<i64>) -> String {
		if records.is_empty() {
			return String::new();
		}

		let mut output = String::new();
		let first_record = records[0];
		let level = first_record.level;
		let timestamp = first_record.timestamp;
		
		// Get seconds since epoch and milliseconds
		let total_seconds = timestamp.timestamp();
		let millis = timestamp.timestamp_subsec_millis();
		
		// Determine if we need to show full timestamp or delta
		let time_str = if last_second.as_ref() != Some(&total_seconds) {
			*last_second = Some(total_seconds);
			// Show full second precision
			format!("{} ─┬─", timestamp.format("%H:%M:%S"))
		} else {
			// Show millisecond delta
			format!("    +{:03}ms├─", millis)
		};

		// Format level string
		let level_str = match level {
			LogLevel::Off => unreachable!(),
			LogLevel::Trace => "[TRACE]",
			LogLevel::Debug => "[DEBUG]",
			LogLevel::Info => "[INFO]",
			LogLevel::Warn => "[WARN]",
			LogLevel::Error => "[ERROR]",
			LogLevel::Critical => "[CRITICAL]",
		};

		// Apply colors
		let apply_color = |text: &str| -> String {
			if self.use_color {
				match level {
					LogLevel::Off => unreachable!(),
					LogLevel::Trace => text.bright_black().to_string(),
					LogLevel::Debug => text.bright_blue().to_string(),
					LogLevel::Info => text.green().to_string(),
					LogLevel::Warn => text.yellow().to_string(),
					LogLevel::Error => text.red().to_string(),
					LogLevel::Critical => text.bright_magenta().bold().to_string(),
				}
			} else {
				text.to_string()
			}
		};

		// Start the group header
		output.push_str(&apply_color(&format!("{} {} {}", time_str, level_str, module)));
		output.push('\n');

		// Maximum width for content (accounting for the indent and tree characters)
		const MAX_WIDTH: usize = 120;
		const INDENT: &str = "          │  ";
		
		// Format each message in the group
		for (i, record) in records.iter().enumerate() {
			let is_last = i == records.len() - 1;
			let branch_char = if is_last { "└─" } else { "├─" };
			let continuation = if is_last { "   " } else { "│  "};
			
			// Wrap long messages
			let wrapped_lines = self.wrap_text(&record.message, MAX_WIDTH);
			
			for (j, line) in wrapped_lines.iter().enumerate() {
				if j == 0 {
					// First line with branch
					output.push_str(&apply_color(&format!("{}{} ", INDENT, branch_char)));
					output.push_str(&format!("{}\n", line));
				} else {
					// Continuation lines
					output.push_str(&apply_color(&format!("{}{} ", INDENT, continuation)));
					output.push_str(&format!("{}\n", line));
				}
			}
		}
		
		// Add separator line
		output.push_str(&apply_color("          │\n"));
		
		output
	}

	/// Wrap text to fit within the specified width
	fn wrap_text(&self, text: &str, max_width: usize) -> Vec<String> {
		let mut result = Vec::new();
		
		for line in text.lines() {
			if line.len() <= max_width {
				result.push(line.to_string());
			} else {
				// Split long lines
				let mut current = String::new();
				let mut parts = line.split_whitespace().peekable();
				
				while let Some(word) = parts.next() {
					// If word itself is longer than max width, split it
					if word.len() > max_width {
						// Flush current line if not empty
						if !current.is_empty() {
							result.push(current);
							current = String::new();
						}
						// Split the long word
						for chunk in word.chars().collect::<Vec<_>>().chunks(max_width) {
							result.push(chunk.iter().collect());
						}
					} else if current.is_empty() {
						current = word.to_string();
					} else if current.len() + 1 + word.len() <= max_width {
						current.push(' ');
						current.push_str(word);
					} else {
						// Current line is full, start a new one
						result.push(current);
						current = word.to_string();
					}
				}
				
				// Push any remaining content
				if !current.is_empty() {
					result.push(current);
				}
			}
		}
		
		// If no lines were produced, return the original text as a single line
		if result.is_empty() {
			result.push(text.to_string());
		}
		
		result
	}

	fn format_record(&self, record: &Record) -> String {
		let mut output = String::new();

		// Create the header content
		let timestamp =
			record.timestamp.format("%Y-%m-%d %H:%M:%S%.3f");
		let module = self.format_module(&record.module);

		// Build the header text
		let header_text = format!(
			"{} [{}] {}",
			timestamp,
			match record.level {
				LogLevel::Off => unreachable!(),
				LogLevel::Trace => "TRACE",
				LogLevel::Debug => "DEBUG",
				LogLevel::Info => "INFO",
				LogLevel::Warn => "WARN",
				LogLevel::Error => "ERROR",
				LogLevel::Critical => "CRITICAL",
			},
			module
		);

		// Add structured fields if present
		let header_with_fields = if !record.fields.is_empty() {
			let fields: Vec<String> = record
				.fields
				.iter()
				.map(|(k, v)| format!("{}={}", k, v))
				.collect();
			format!("{} {{{}}}", header_text, fields.join(", "))
		} else {
			header_text
		};

		// Apply color to the entire header based on log level
		if self.use_color {
			let (
				top_border,
				header_colored,
				side_border,
				bottom_border,
			) = match record.level {
				LogLevel::Off => unreachable!(),
				LogLevel::Trace => (
					"┌─ ".bright_black().to_string(),
					header_with_fields
						.bright_black()
						.to_string(),
					"│ ".bright_black().to_string(),
					"└────".bright_black().to_string(),
				),
				LogLevel::Debug => (
					"┌─ ".bright_blue().to_string(),
					header_with_fields
						.bright_blue()
						.to_string(),
					"│ ".bright_blue().to_string(),
					"└────".bright_blue().to_string(),
				),
				LogLevel::Info => (
					"┌─ ".green().to_string(),
					header_with_fields.green().to_string(),
					"│ ".green().to_string(),
					"└────".green().to_string(),
				),
				LogLevel::Warn => (
					"┌─ ".yellow().to_string(),
					header_with_fields.yellow().to_string(),
					"│ ".yellow().to_string(),
					"└────".yellow().to_string(),
				),
				LogLevel::Error => (
					"┌─ ".red().to_string(),
					header_with_fields.red().to_string(),
					"│ ".red().to_string(),
					"└────".red().to_string(),
				),
				LogLevel::Critical => (
					"┌─ "
						.bright_magenta()
						.bold()
						.to_string(),
					header_with_fields
						.bright_magenta()
						.bold()
						.to_string(),
					"│ ".bright_magenta()
						.bold()
						.to_string(),
					"└────"
						.bright_magenta()
						.bold()
						.to_string(),
				),
			};

			// Build output with colored elements
			output.push_str(&top_border);
			output.push_str(&header_colored);
			output.push('\n');

			// Message content with colored left border and wrapping
			if !record.message.is_empty() {
				const MAX_LINE_WIDTH: usize = 120;

				for line in record.message.lines() {
					if line.len() <= MAX_LINE_WIDTH {
						output.push_str(&side_border);
						output.push_str(line);
						output.push('\n');
					} else {
						let mut remaining = line;
						while !remaining.is_empty() {
							let chunk_end = if remaining.len() > MAX_LINE_WIDTH {
                                let mut break_point = MAX_LINE_WIDTH;
                                for (i, ch) in remaining[..MAX_LINE_WIDTH].char_indices().rev() {
                                    if ch == ' ' || ch == ',' || ch == ';' || ch == ':' {
                                        break_point = i + 1;
                                        break;
                                    }
                                }
                                break_point
                            } else {
                                remaining.len()
                            };

							output.push_str(
								&side_border,
							);
							output.push_str(&remaining[..chunk_end]);
							output.push('\n');
							remaining = &remaining
								[chunk_end..]
								.trim_start();
						}
					}
				}
			}

			output.push_str(&bottom_border);
		} else {
			// No color version
			output.push_str("┌─ ");
			output.push_str(&header_with_fields);
			output.push('\n');

			if !record.message.is_empty() {
				const MAX_LINE_WIDTH: usize = 120;

				for line in record.message.lines() {
					if line.len() <= MAX_LINE_WIDTH {
						output.push_str("│ ");
						output.push_str(line);
						output.push('\n');
					} else {
						let mut remaining = line;
						while !remaining.is_empty() {
							let chunk_end = if remaining.len() > MAX_LINE_WIDTH {
                                let mut break_point = MAX_LINE_WIDTH;
                                for (i, ch) in remaining[..MAX_LINE_WIDTH].char_indices().rev() {
                                    if ch == ' ' || ch == ',' || ch == ';' || ch == ':' {
                                        break_point = i + 1;
                                        break;
                                    }
                                }
                                break_point
                            } else {
                                remaining.len()
                            };

							output.push_str("│ ");
							output.push_str(&remaining[..chunk_end]);
							output.push('\n');
							remaining = &remaining
								[chunk_end..]
								.trim_start();
						}
					}
				}
			}

			output.push_str("└────");
		}

		output
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

		match self.format_style {
			FormatStyle::Timeline => {
				// Process all records together for timeline formatting
				let formatted_groups = self.format_timeline_records(records);
				for formatted in formatted_groups {
					// Check if any record in this group should go to stderr
					// For simplicity, we'll send all to stdout for now
					stdout_records.push(formatted);
				}
			}
			FormatStyle::Box => {
				// Original box formatting
				for record in records {
					if record.level == LogLevel::Off {
						continue;
					}

					let formatted =
						format!("{}\n", self.format_record(record));
					if self.stderr_for_errors
						&& record.level >= LogLevel::Error
					{
						stderr_records.push(formatted);
					} else {
						stdout_records.push(formatted);
					}
				}
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
