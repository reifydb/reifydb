// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{OwnedFragment, result::error::diagnostic::Diagnostic};

/// Creates a detailed internal error diagnostic with source location and
/// context
pub fn internal_with_context(
	reason: impl Into<String>,
	file: &str,
	line: u32,
	column: u32,
	function: &str,
	module_path: &str,
) -> Diagnostic {
	let reason = reason.into();

	// Generate a unique error ID based on timestamp and location
	let error_id = format!(
		"ERR-{}-{}:{}",
		chrono::Utc::now().timestamp_millis(),
		file.split('/').last().unwrap_or(file).replace(".rs", ""),
		line
	);

	let detailed_message =
		format!("Internal error [{}]: {}", error_id, reason);

	let location_info = format!(
		"Location: {}:{}:{}\nFunction: {}\nModule: {}",
		file, line, column, function, module_path
	);

	let help_message = format!(
		"This is an internal error that should never occur in normal operation.\n\n\
         Please file a bug report at: https://github.com/reifydb/reifydb/issues\n\n\
         Include the following information:\n\
         ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\
         Error ID: {}\n\
         {}\n\
         Version: {}\n\
         Build: {} ({})\n\
         Platform: {} {}\n\
         ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
		error_id,
		location_info,
		env!("CARGO_PKG_VERSION"),
		option_env!("GIT_HASH").unwrap_or("unknown"),
		option_env!("BUILD_DATE").unwrap_or("unknown"),
		std::env::consts::OS,
		std::env::consts::ARCH
	);

	Diagnostic {
        code: "INTERNAL_ERROR".to_string(),
        statement: None,
        message: detailed_message,
        column: None,
        fragment: OwnedFragment::None,
        label: Some(format!("Internal invariant violated at {}:{}:{}", file, line, column)),
        help: Some(help_message),
        notes: vec![
            format!("Error occurred in function: {}", function),
            "This error indicates a critical internal inconsistency.".to_string(),
            "Your database may be in an inconsistent state.".to_string(),
            "Consider creating a backup before continuing operations.".to_string(),
            format!("Error tracking ID: {}", error_id),
        ],
        cause: None,
    }
}

/// Simplified internal error without detailed context
pub fn internal(reason: impl Into<String>) -> Diagnostic {
	internal_with_context(reason, "unknown", 0, 0, "unknown", "unknown")
}

/// Macro to create an internal error with automatic source location capture
#[macro_export]
macro_rules! internal_error {
    ($reason:expr) => {
        $crate::result::error::diagnostic::internal_with_context(
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
        $crate::result::error::diagnostic::internal_with_context(
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

/// Macro to create an internal error result with automatic source location
/// capture
#[macro_export]
macro_rules! internal_err {
    ($reason:expr) => {
        Err($crate::error::Error($crate::internal_error!($reason)))
    };
    ($fmt:expr, $($arg:tt)*) => {
        Err($crate::error::Error($crate::internal_error!($fmt, $($arg)*)))
    };
}

/// Macro to return an internal error with automatic source location capture
/// This combines return_error! and internal_error! for convenience
#[macro_export]
macro_rules! return_internal_error {
    ($reason:expr) => {
        return Err($crate::error::Error($crate::internal_error!($reason)))
    };
    ($fmt:expr, $($arg:tt)*) => {
        return Err($crate::error::Error($crate::internal_error!($fmt, $($arg)*)))
    };
}

#[cfg(test)]
mod tests {
	use super::*;

	#[derive(Debug)]
	#[allow(dead_code)]
	struct TestStruct {
		value: i32,
		name: String,
	}

	#[test]
	fn test_internal_error_literal_string() {
		let diagnostic = internal_error!("simple error message");

		assert_eq!(diagnostic.code, "INTERNAL_ERROR");
		assert!(diagnostic.message.contains("simple error message"));
		assert!(diagnostic.help.is_some());
		assert!(diagnostic
			.help
			.as_ref()
			.unwrap()
			.contains("bug report"));
		assert!(diagnostic.notes.len() > 0);
	}

	#[test]
	fn test_internal_error_with_format() {
		let value = 42;
		let name = "test";
		let diagnostic = internal_error!(
			"Error with value: {} and name: {}",
			value,
			name
		);

		assert_eq!(diagnostic.code, "INTERNAL_ERROR");
		assert!(diagnostic
			.message
			.contains("Error with value: 42 and name: test"));
		assert!(diagnostic.label.is_some());
		assert!(diagnostic
			.label
			.as_ref()
			.unwrap()
			.contains("Internal invariant violated"));
	}

	#[test]
	fn test_internal_error_inline_variable_syntax() {
		let test_struct = TestStruct {
			value: 42,
			name: "test".to_string(),
		};

		let diagnostic =
			internal_error!("Test struct: {:?}", test_struct);

		assert_eq!(diagnostic.code, "INTERNAL_ERROR");
		assert!(diagnostic.message.contains("TestStruct"));
		assert!(diagnostic.message.contains("value: 42"));
		assert!(diagnostic.message.contains("name: \"test\""));
	}

	#[test]
	fn test_internal_err_literal_string() {
		let result: Result<(), crate::Error> =
			internal_err!("test error");

		assert!(result.is_err());
		let error = result.unwrap_err();
		assert_eq!(error.0.code, "INTERNAL_ERROR");
		assert!(error.0.message.contains("test error"));
	}

	#[test]
	fn test_internal_err_with_format() {
		let code = "ERR_123";
		let line = 456;
		let result: Result<(), crate::Error> =
			internal_err!("Error code: {} at line {}", code, line);

		assert!(result.is_err());
		let error = result.unwrap_err();
		assert_eq!(error.0.code, "INTERNAL_ERROR");
		assert!(error
			.0
			.message
			.contains("Error code: ERR_123 at line 456"));
	}

	#[test]
	fn test_internal_function() {
		let diagnostic = internal("basic internal error");

		assert_eq!(diagnostic.code, "INTERNAL_ERROR");
		assert!(diagnostic.message.contains("basic internal error"));
		assert!(diagnostic.label.is_some());
		assert!(diagnostic
			.label
			.as_ref()
			.unwrap()
			.contains("unknown:0:0"));
	}

	#[test]
	fn test_internal_with_context_function() {
		let diagnostic = internal_with_context(
			"context error",
			"test.rs",
			100,
			20,
			"test_function",
			"test::module",
		);

		assert_eq!(diagnostic.code, "INTERNAL_ERROR");
		assert!(diagnostic.message.contains("context error"));
		assert!(diagnostic.label.is_some());
		assert!(diagnostic
			.label
			.as_ref()
			.unwrap()
			.contains("test.rs:100:20"));
		assert!(diagnostic
			.notes
			.iter()
			.any(|n| n.contains("test_function")));
		assert!(diagnostic.help.is_some());
		let help = diagnostic.help.as_ref().unwrap();
		assert!(help.contains("test.rs:100:20"));
		assert!(help.contains("test::module"));
	}

	#[test]
	fn test_return_internal_error_in_function() {
		fn test_function_literal() -> Result<(), crate::Error> {
			return_internal_error!("function error");
		}

		let result = test_function_literal();
		assert!(result.is_err());
		let error = result.unwrap_err();
		assert_eq!(error.0.code, "INTERNAL_ERROR");
		assert!(error.0.message.contains("function error"));
	}

	#[test]
	fn test_return_internal_error_with_format() {
		fn test_function_format(val: u32) -> Result<(), crate::Error> {
			return_internal_error!("Invalid value: {:#04x}", val);
		}

		let result = test_function_format(255);
		assert!(result.is_err());
		let error = result.unwrap_err();
		assert_eq!(error.0.code, "INTERNAL_ERROR");
		assert!(error.0.message.contains("Invalid value: 0xff"));
	}

	#[test]
	fn test_error_id_generation() {
		let diagnostic1 = internal_with_context(
			"error 1", "file1.rs", 10, 5, "func1", "mod1",
		);

		// Small delay to ensure different timestamps
		std::thread::sleep(std::time::Duration::from_millis(2));

		let diagnostic2 = internal_with_context(
			"error 2", "file2.rs", 20, 10, "func2", "mod2",
		);

		// Extract error IDs from messages
		let id1 = diagnostic1
			.message
			.split('[')
			.nth(1)
			.unwrap()
			.split(']')
			.nth(0)
			.unwrap();
		let id2 = diagnostic2
			.message
			.split('[')
			.nth(1)
			.unwrap()
			.split(']')
			.nth(0)
			.unwrap();

		// Error IDs should be unique
		assert_ne!(id1, id2);
		assert!(id1.starts_with("ERR-"));
		assert!(id2.starts_with("ERR-"));
	}
}
