// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::diagnostic::Diagnostic;

use crate::diagnostic::database_error;
pub use crate::diagnostic::transaction_failed;

/// Converts a rusqlite error to a diagnostic
pub fn from_rusqlite_error(error: rusqlite::Error) -> Diagnostic {
	match error {
		rusqlite::Error::SqliteFailure(err, msg) => {
			let code = err.code;
			let extended = err.extended_code;

			let message = if let Some(msg) = msg {
				format!("SQLite error (code: {}, extended: {}): {}", code as i32, extended, msg)
			} else {
				format!("SQLite error (code: {}, extended: {})", code as i32, extended)
			};

			database_error(message)
		}
		_ => database_error(error.to_string()),
	}
}
