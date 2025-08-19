// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Console logging backend with colored output

use super::LogBackend;
use crate::record::{LogLevel, LogRecord};
use colored::*;
use parking_lot::Mutex;
use reifydb_core::Result;
use std::io::{self, Write};

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

    fn format_record(&self, record: &LogRecord) -> String {
        let mut output = String::new();
        
        // Create the header content
        let timestamp = record.timestamp.format("%Y-%m-%d %H:%M:%S%.3f");
        let module = self.format_module(&record.module);
        
        // Build the header text
        let header_text = format!(
            "{} [{}] {}",
            timestamp,
            match record.level {
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
            let (top_border, header_colored, side_border, bottom_border) = match record.level {
                LogLevel::Trace => (
                    "┌─ ".bright_black().to_string(),
                    header_with_fields.bright_black().to_string(),
                    "│ ".bright_black().to_string(),
                    "└────".bright_black().to_string(),
                ),
                LogLevel::Debug => (
                    "┌─ ".bright_blue().to_string(),
                    header_with_fields.bright_blue().to_string(),
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
                    "┌─ ".bright_magenta().bold().to_string(),
                    header_with_fields.bright_magenta().bold().to_string(),
                    "│ ".bright_magenta().bold().to_string(),
                    "└────".bright_magenta().bold().to_string(),
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
                            
                            output.push_str(&side_border);
                            output.push_str(&remaining[..chunk_end]);
                            output.push('\n');
                            remaining = &remaining[chunk_end..].trim_start();
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
                            remaining = &remaining[chunk_end..].trim_start();
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
    fn write(&self, record: &LogRecord) -> Result<()> {
        let formatted = format!("{}\n", self.format_record(record));
        
        if self.stderr_for_errors && record.level >= LogLevel::Error {
            let mut stderr = self.stderr_lock.lock();
            // Best effort - ignore errors on console output
            let _ = stderr.write_all(formatted.as_bytes());
            let _ = stderr.flush();
        } else {
            let mut stdout = self.stdout_lock.lock();
            // Best effort - ignore errors on console output
            let _ = stdout.write_all(formatted.as_bytes());
            let _ = stdout.flush();
        }
        
        Ok(())
    }

    fn write_batch(&self, records: &[LogRecord]) -> Result<()> {
        let mut stdout_records = Vec::new();
        let mut stderr_records = Vec::new();

        for record in records {
            let formatted = format!("{}\n", self.format_record(record));
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

    fn name(&self) -> &str {
        "console"
    }
}