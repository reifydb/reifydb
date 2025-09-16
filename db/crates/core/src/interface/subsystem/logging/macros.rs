// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Logging macros for convenient usage

/// Main logging macro with support for structured fields
#[macro_export]
macro_rules! log {
    // Simple message (no formatting)
    ($level:expr, $msg:expr) => {{
        // Check if it's a format string by trying to format it
        // This allows both literal strings and format strings with inline variables
        let message = format!($msg);
        let record = $crate::interface::subsystem::logging::Record::new(
            $level,
            module_path!(),
            message,
        )
        .with_location(file!(), line!());
        $crate::interface::subsystem::logging::log(record);
    }};

    // Format string with explicit arguments
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

#[cfg(test)]
mod tests {
	use crossbeam_channel::unbounded;

	use super::super::{mock::with_mock_logger, *};
	use crate::{log, log_critical, log_debug, log_error, log_info, log_trace, log_warn};

	#[derive(Debug)]
	#[allow(dead_code)]
	struct TestStruct {
		value: i32,
		name: String,
	}

	#[test]
	fn test_literal_string() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			log_debug!("simple message");
		});

		let record = receiver.try_recv().unwrap();
		assert_eq!(record.message, "simple message");
		assert_eq!(record.level, LogLevel::Debug);
	}

	#[test]
	fn test_inline_variable_syntax() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let change = TestStruct {
				value: 42,
				name: "test".to_string(),
			};

			log_debug!("{change:?}");
		});

		let record = receiver.try_recv().unwrap();
		assert!(record.message.contains("TestStruct"));
		assert!(record.message.contains("value: 42"));
		assert!(record.message.contains("name: \"test\""));
	}

	#[test]
	fn test_traditional_format_syntax() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let change = TestStruct {
				value: 99,
				name: "traditional".to_string(),
			};

			log_debug!("{:?}", change);
		});

		let record = receiver.try_recv().unwrap();
		assert!(record.message.contains("TestStruct"));
		assert!(record.message.contains("value: 99"));
		assert!(record.message.contains("name: \"traditional\""));
	}

	#[test]
	fn test_traditional_with_display() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let x = 123;
			log_info!("Value: {}", x);
		});

		let record = receiver.try_recv().unwrap();
		assert_eq!(record.message, "Value: 123");
		assert_eq!(record.level, LogLevel::Info);
	}

	#[test]
	fn test_multiple_inline_variables() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let name = "Alice";
			let age = 30;
			let city = "New York";

			log_info!("{name} is {age} years old and lives in {city}");
		});

		let record = receiver.try_recv().unwrap();
		assert_eq!(record.message, "Alice is 30 years old and lives in New York");
	}

	#[test]
	fn test_mixed_formatting() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let count = 5;
			let items = vec!["apple", "banana", "orange"];

			log_warn!("Found {count} items: {items:?}");
		});

		let record = receiver.try_recv().unwrap();
		assert!(record.message.contains("Found 5 items"));
		assert!(record.message.contains("[\"apple\", \"banana\", \"orange\"]"));
		assert_eq!(record.level, LogLevel::Warn);
	}

	#[test]
	fn test_all_log_levels_with_inline_syntax() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let value = 42;

			log_trace!("Trace: {value}");
			log_debug!("Debug: {value}");
			log_info!("Info: {value}");
			log_warn!("Warn: {value}");
			log_error!("Error: {value}");
			log_critical!("Critical: {value}");
		});

		// Collect all logs
		let mut logs = Vec::new();
		while let Ok(record) = receiver.try_recv() {
			logs.push(record);
		}

		assert_eq!(logs.len(), 6);

		assert_eq!(logs[0].message, "Trace: 42");
		assert_eq!(logs[0].level, LogLevel::Trace);

		assert_eq!(logs[1].message, "Debug: 42");
		assert_eq!(logs[1].level, LogLevel::Debug);

		assert_eq!(logs[2].message, "Info: 42");
		assert_eq!(logs[2].level, LogLevel::Info);

		assert_eq!(logs[3].message, "Warn: 42");
		assert_eq!(logs[3].level, LogLevel::Warn);

		assert_eq!(logs[4].message, "Error: 42");
		assert_eq!(logs[4].level, LogLevel::Error);

		assert_eq!(logs[5].message, "Critical: 42");
		assert_eq!(logs[5].level, LogLevel::Critical);
	}

	#[test]
	fn test_comptokenize_inline_expressions() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let numbers = vec![1, 2, 3, 4, 5];

			log_info!("Sum: {}", numbers.iter().sum::<i32>());
			log_info!("Sum inline: {sum}", sum = numbers.iter().sum::<i32>());
		});

		let log1 = receiver.try_recv().unwrap();
		let log2 = receiver.try_recv().unwrap();

		assert_eq!(log1.message, "Sum: 15");
		assert_eq!(log2.message, "Sum inline: 15");
	}

	#[test]
	fn test_escaped_braces() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let value = 10;
			log_debug!("The value {{in braces}} is {value}");
		});

		let record = receiver.try_recv().unwrap();
		assert_eq!(record.message, "The value {in braces} is 10");
	}

	#[test]
	fn test_empty_format_string() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			log_info!("");
		});

		let record = receiver.try_recv().unwrap();
		assert_eq!(record.message, "");
	}

	#[test]
	fn test_format_specifiers() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let pi = 3.1456789;
			let hex = 255;

			// Traditional syntax with format specifiers
			log_debug!("{:.2}", pi);
			log_debug!("{:x}", hex);

			// Inline syntax with format specifiers
			log_debug!("{pi:.2}");
			log_debug!("{hex:x}");
		});

		let log1 = receiver.try_recv().unwrap();
		let log2 = receiver.try_recv().unwrap();
		let log3 = receiver.try_recv().unwrap();
		let log4 = receiver.try_recv().unwrap();

		assert_eq!(log1.message, "3.15");
		assert_eq!(log2.message, "ff");
		assert_eq!(log3.message, "3.15");
		assert_eq!(log4.message, "ff");
	}

	#[test]
	fn test_multiline_strings() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let error = "Connection failed";
			log_error!("Error occurred:\n{error}\nPlease retry");
		});

		let record = receiver.try_recv().unwrap();
		assert_eq!(record.message, "Error occurred:\nConnection failed\nPlease retry");
	}

	#[test]
	fn test_raw_log_macro() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let custom_value = "custom";

			// Test the raw log! macro directly
			log!(LogLevel::Info, "Raw log message");
			log!(LogLevel::Warn, "Raw with value: {}", 123);
			log!(LogLevel::Error, "Raw inline: {custom_value}");
		});

		let log1 = receiver.try_recv().unwrap();
		let log2 = receiver.try_recv().unwrap();
		let log3 = receiver.try_recv().unwrap();

		assert_eq!(log1.message, "Raw log message");
		assert_eq!(log1.level, LogLevel::Info);

		assert_eq!(log2.message, "Raw with value: 123");
		assert_eq!(log2.level, LogLevel::Warn);

		assert_eq!(log3.message, "Raw inline: custom");
		assert_eq!(log3.level, LogLevel::Error);
	}

	#[test]
	fn test_logging_macros_compile() {
		// These should compile without errors
		// Note: they won't actually log anything since no logger is
		// initialized

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
		// Test that various Rust types can be used as both keys and
		// values

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

		// Format message with fields - this should work but requires
		// careful macro design For now, we can use simple message
		// with fields
		log_error!("Login failed", {
		    "user" => user,
		    "attempts" => attempts,
		    "max_attempts" => 5
		});
	}
}
