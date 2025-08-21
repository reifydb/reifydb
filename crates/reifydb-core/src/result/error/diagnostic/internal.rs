// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{OwnedFragment, diagnostic::Diagnostic};

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
        Err($crate::Error::Diagnostic($crate::internal_error!($reason)))
    };
    ($fmt:expr, $($arg:tt)*) => {
        Err($crate::Error::Diagnostic($crate::internal_error!($fmt, $($arg)*)))
    };
}

/// Macro to assert an invariant that should always be true
#[macro_export]
macro_rules! invariant {
    ($cond:expr, $reason:expr) => {
        if !$cond {
            return $crate::internal_err!($reason);
        }
    };
    ($cond:expr, $fmt:expr, $($arg:tt)*) => {
        if !$cond {
            return $crate::internal_err!($fmt, $($arg)*);
        }
    };
}

/// Macro to mark code that should be unreachable
#[macro_export]
macro_rules! unreachable_internal {
    () => {
        $crate::internal_err!("Reached unreachable code")
    };
    ($reason:expr) => {
        $crate::internal_err!(concat!("Reached unreachable code: ", $reason))
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::internal_err!(concat!("Reached unreachable code: ", $fmt), $($arg)*)
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
