// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{OwnedFragment, diagnostic::Diagnostic};

/// Creates a diagnostic for general database errors
pub fn database_error(message: impl Into<String>) -> Diagnostic {
	let msg = message.into();
	Diagnostic {
		code: "STORAGE_001".to_string(),
		statement: None,
		message: format!("Database operation failed: {}", msg),
		column: None,
		fragment: OwnedFragment::None,
		label: Some("Database error occurred".to_string()),
		help: Some("Check that:\n\
             • The database file is accessible\n\
             • Sufficient disk space is available\n\
             • The database is not corrupted"
			.to_string()),
		notes: vec![msg],
		cause: None,
	}
}

/// Creates a diagnostic for transaction failures
pub fn transaction_failed(reason: impl Into<String>) -> Diagnostic {
	let reason = reason.into();
	Diagnostic {
		code: "STORAGE_002".to_string(),
		statement: None,
		message: format!("Database transaction failed: {}", reason),
		column: None,
		fragment: OwnedFragment::None,
		label: Some("Transaction could not be completed".to_string()),
		help: Some("Transaction failures can occur due to:\n\
             • Constraint violations\n\
             • Lock timeouts\n\
             • Disk space issues\n\
             • Database corruption\n\n\
             Review the transaction operations and retry if appropriate."
			.to_string()),
		notes: vec![
			reason,
			"The transaction has been rolled back".to_string(),
			"No changes were committed to the database".to_string(),
		],
		cause: None,
	}
}

/// Creates a diagnostic for connection failures
pub fn connection_failed(path: impl Into<String>, error: impl Into<String>) -> Diagnostic {
	let path = path.into();
	let error = error.into();
	Diagnostic {
		code: "STORAGE_003".to_string(),
		statement: None,
		message: format!("Failed to connect to database at '{}': {}", path, error),
		column: None,
		fragment: OwnedFragment::None,
		label: Some("Database connection failed".to_string()),
		help: Some("Ensure that:\n\
             • The database path is correct\n\
             • The database file exists (or can be created)\n\
             • You have appropriate file permissions\n\
             • The database is not locked by another process"
			.to_string()),
		notes: vec![format!("Path: {}", path), format!("Error: {}", error)],
		cause: None,
	}
}

/// Creates a diagnostic for sequence exhaustion
pub fn sequence_exhausted() -> Diagnostic {
	Diagnostic {
		code: "STORAGE_004".to_string(),
		statement: None,
		message: "Transaction sequence number exhausted (exceeds u16 maximum)".to_string(),
		column: None,
		fragment: OwnedFragment::None,
		label: Some("Too many operations in a single transaction".to_string()),
		help: Some("This transaction contains more than 65,535 operations.\n\
             Consider splitting the transaction into smaller batches."
			.to_string()),
		notes: vec![
			"Maximum operations per transaction: 65,535".to_string(),
			"Large transactions can impact performance".to_string(),
		],
		cause: None,
	}
}

/// Macro to create an internal storage error with automatic source location
#[macro_export]
macro_rules! storage_internal_error {
    ($reason:expr) => {
        reifydb_type::diagnostic::internal_with_context(
            $reason,
            file!(),
            line!(),
            column!(),
            {
                fn f() {}
                fn type_name_of<T>(_: T) -> &'static str {
                    std::any::type_name::<T>()
                }
                let name = type_name_of(f);
                &name[..name.len() - 3]
            },
            module_path!()
        )
    };
    ($fmt:expr, $($arg:tt)*) => {
        reifydb_type::diagnostic::internal_with_context(
            format!($fmt, $($arg)*),
            file!(),
            line!(),
            column!(),
            {
                fn f() {}
                fn type_name_of<T>(_: T) -> &'static str {
                    std::any::type_name::<T>()
                }
                let name = type_name_of(f);
                &name[..name.len() - 3]
            },
            module_path!()
        )
    };
}

#[allow(unused_imports)]
pub use storage_internal_error;
