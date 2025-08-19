// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Logging backend implementations

use crate::record::LogRecord;
use reifydb_core::Result;
use std::fmt::Debug;

pub mod console;
pub mod database;

pub use console::ConsoleBackend;
pub use database::DatabaseBackend;

/// Trait for logging backends
pub trait LogBackend: Send + Sync + Debug {
	/// Write a single log record
	fn write(&self, record: &LogRecord) -> Result<()>;

	/// Write multiple log records in a batch
	fn write_batch(&self, records: &[LogRecord]) -> Result<()> {
		for record in records {
			self.write(record)?;
		}
		Ok(())
	}

	/// Flush any buffered logs
	fn flush(&self) -> Result<()> {
		Ok(())
	}

	/// Backend name for identification
	fn name(&self) -> &str;
}
