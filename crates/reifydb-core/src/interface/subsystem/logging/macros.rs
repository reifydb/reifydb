// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Logging macros for convenient usage

/// Main logging macro with support for structured fields
#[macro_export]
macro_rules! log {
    // Simple message
    ($level:expr, $msg:expr) => {{
        let record = $crate::interface::subsystem::logging::Record::new(
            $level,
            module_path!(),
            $msg,
        )
        .with_location(file!(), line!());
        $crate::interface::subsystem::logging::log(record);
    }};

    // Format string with arguments
    ($level:expr, $fmt:expr, $($arg:tt)*) => {{
        let message = format!($fmt, $($arg)*);
        let record = $crate::interface::subsystem::logging::Record::new(
            $level,
            module_path!(),
            message,
        )
        .with_location(file!(), line!());
        $crate::interface::subsystem::logging::log(record);
    }};
}

/// Trace level logging
#[macro_export]
macro_rules! log_trace {
    // Simple message
    ($msg:expr) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Trace, $msg)
    };

    // Message with fields: log_trace!("message", { key: value, ... })
    ($msg:expr, { $($key:expr => $value:expr),+ $(,)? }) => {{
        let mut record = $crate::interface::subsystem::logging::Record::new(
            $crate::interface::subsystem::logging::LogLevel::Trace,
            module_path!(),
            $msg,
        )
        .with_location(file!(), line!());
        $(
            record = record.with_field($key, $value);
        )+
        $crate::interface::subsystem::logging::log(record);
    }};

    // Format string with arguments
    ($fmt:expr, $($arg:tt)*) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Trace, $fmt, $($arg)*)
    };
}

/// Debug level logging
#[macro_export]
macro_rules! log_debug {
    // Simple message
    ($msg:expr) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Debug, $msg)
    };

    // Message with fields: log_debug!("message", { key: value, ... })
    ($msg:expr, { $($key:expr => $value:expr),+ $(,)? }) => {{
        let mut record = $crate::interface::subsystem::logging::Record::new(
            $crate::interface::subsystem::logging::LogLevel::Debug,
            module_path!(),
            $msg,
        )
        .with_location(file!(), line!());
        $(
            record = record.with_field($key, $value);
        )+
        $crate::interface::subsystem::logging::log(record);
    }};

    // Format string with arguments
    ($fmt:expr, $($arg:tt)*) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Debug, $fmt, $($arg)*)
    };
}

/// Info level logging
#[macro_export]
macro_rules! log_info {
    // Simple message
    ($msg:expr) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Info, $msg)
    };

    // Message with fields: log_info!("message", { key: value, ... })
    ($msg:expr, { $($key:expr => $value:expr),+ $(,)? }) => {{
        let mut record = $crate::interface::subsystem::logging::Record::new(
            $crate::interface::subsystem::logging::LogLevel::Info,
            module_path!(),
            $msg,
        )
        .with_location(file!(), line!());
        $(
            record = record.with_field($key, $value);
        )+
        $crate::interface::subsystem::logging::log(record);
    }};

    // Format string with arguments
    ($fmt:expr, $($arg:tt)*) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Info, $fmt, $($arg)*)
    };
}

/// Warning level logging
#[macro_export]
macro_rules! log_warn {
    // Simple message
    ($msg:expr) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Warn, $msg)
    };

    // Message with fields: log_warn!("message", { key: value, ... })
    ($msg:expr, { $($key:expr => $value:expr),+ $(,)? }) => {{
        let mut record = $crate::interface::subsystem::logging::Record::new(
            $crate::interface::subsystem::logging::LogLevel::Warn,
            module_path!(),
            $msg,
        )
        .with_location(file!(), line!());
        $(
            record = record.with_field($key, $value);
        )+
        $crate::interface::subsystem::logging::log(record);
    }};

    // Format string with arguments
    ($fmt:expr, $($arg:tt)*) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Warn, $fmt, $($arg)*)
    };
}

/// Error level logging
#[macro_export]
macro_rules! log_error {
    // Simple message
    ($msg:expr) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Error, $msg)
    };

    // Message with fields: log_error!("message", { key: value, ... })
    ($msg:expr, { $($key:expr => $value:expr),+ $(,)? }) => {{
        let mut record = $crate::interface::subsystem::logging::Record::new(
            $crate::interface::subsystem::logging::LogLevel::Error,
            module_path!(),
            $msg,
        )
        .with_location(file!(), line!());
        $(
            record = record.with_field($key, $value);
        )+
        $crate::interface::subsystem::logging::log(record);
    }};

    // Format string with arguments
    ($fmt:expr, $($arg:tt)*) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Error, $fmt, $($arg)*)
    };
}

/// Critical level logging with guaranteed synchronous delivery
#[macro_export]
macro_rules! log_critical {
    // Simple message
    ($msg:expr) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Critical, $msg)
    };

    // Message with fields: log_critical!("message", { key: value, ... })
    ($msg:expr, { $($key:expr => $value:expr),+ $(,)? }) => {{
        let mut record = $crate::interface::subsystem::logging::Record::new(
            $crate::interface::subsystem::logging::LogLevel::Critical,
            module_path!(),
            $msg,
        )
        .with_location(file!(), line!());
        $(
            record = record.with_field($key, $value);
        )+
        $crate::interface::subsystem::logging::log(record);
    }};

    // Format string with arguments
    ($fmt:expr, $($arg:tt)*) => {
        $crate::log!($crate::interface::subsystem::logging::LogLevel::Critical, $fmt, $($arg)*)
    };
}

#[test]
fn test_logging_macros_compile() {
	// These should compile without errors
	// Note: they won't actually log anything since no logger is initialized

	// Basic logging macros
	log_trace!("This is a trace message");
	log_debug!("This is a debug message");
	log_info!("This is an info message");
	log_warn!("This is a warning message");
	log_error!("This is an error message");
	log_critical!("This is a critical message");

	// With format arguments
	let value = 42;
	log_info!("Value is: {}", value);
	log_debug!("Debug value: {} and string: {}", value, "test");

	// With structured fields using ReifyDB Values
	// Both keys and values are converted via IntoValue trait
	log_info!("User logged in", {
	    "user_id" => 123i32,
	    "username" => "alice",
	    "active" => true
	});

	log_debug!("Processing request", {
	    "request_id" => "abc-123",
	    "method" => "GET",
	    "path" => "/api/users",
	    "status_code" => 200u16
	});

	log_error!("Database connection failed", {
	    "error_code" => 500i32,
	    "retry_count" => 3u8,
	    "message" => "Connection timeout"
	});

	// Test with various ReifyDB value types
	log_warn!("Performance warning", {
	    "duration_ms" => 1500i64,
	    "threshold_ms" => 1000i64,
	    "exceeded_by" => 500i64
	});

	// Test with numeric keys (they become Values too)
	log_debug!("Numeric keys test", {
	    1 => "first",
	    2 => "second",
	    3 => "third"
	});

	// Test with boolean values
	log_info!("Feature flags", {
	    "feature_a" => true,
	    "feature_b" => false,
	    "feature_c" => true
	});

	// Test with optional values
	let optional_value: Option<i32> = Some(42);
	let none_value: Option<&str> = None;
	log_debug!("Optional values", {
	    "some_value" => optional_value,
	    "none_value" => none_value
	});
}

#[test]
fn test_logging_with_different_types() {
	// Test that various Rust types can be used as both keys and values

	// Integer types
	log_info!("Integer types", {
	    "i8" => 127i8,
	    "i16" => 32767i16,
	    "i32" => 2147483647i32,
	    "i64" => 9223372036854775807i64,
	    "u8" => 255u8,
	    "u16" => 65535u16,
	    "u32" => 4294967295u32,
	    "u64" => 18446744073709551615u64
	});

	// String types
	let owned_string = String::from("owned");
	log_debug!("String types", {
	    "literal" => "string literal",
	    "owned" => owned_string.clone(),
	    "reference" => owned_string.as_str()
	});

	// Mixed types as keys
	log_trace!("Mixed key types", {
	    "string_key" => "value1",
	    123 => "numeric_key",
	    true => "boolean_key"
	});
}

#[test]
fn test_format_with_fields() {
	let user = "alice";
	let attempts = 3;

	// Format message with fields - this should work but requires careful macro design
	// For now, we can use simple message with fields
	log_error!("Login failed", {
	    "user" => user,
	    "attempts" => attempts,
	    "max_attempts" => 5
	});
}
