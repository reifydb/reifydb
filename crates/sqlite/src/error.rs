// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::io::Error as IoError;

use rusqlite::Error as SqlError;
use thiserror::Error;

pub type SqliteResult<T> = Result<T, SqliteError>;

#[derive(Debug, Error)]
pub enum SqliteError {
	#[error("failed to connect to SQLite database at {path}: {source}")]
	Connect {
		path: String,
		#[source]
		source: SqlError,
	},

	#[error("failed to create directory for SQLite database at {path}: {source}")]
	CreateDir {
		path: String,
		#[source]
		source: IoError,
	},

	#[error("failed to apply pragma {name}: {source}")]
	Pragma {
		name: String,
		#[source]
		source: SqlError,
	},

	#[error("failed to execute statement {statement}: {source}")]
	Execute {
		statement: String,
		#[source]
		source: SqlError,
	},
}
