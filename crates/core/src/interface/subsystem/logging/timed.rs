// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Timed logging macros that automatically measure and append execution time

/// Format a duration in nanoseconds for human-readable display
pub fn format_duration_nanos(nanos: u128) -> String {
	if nanos < 1_000 {
		format!("{}ns", nanos)
	} else if nanos < 1_000_000 {
		format!("{:.2}μs", nanos as f64 / 1_000.0)
	} else if nanos < 1_000_000_000 {
		format!("{:.2}ms", nanos as f64 / 1_000_000.0)
	} else {
		format!("{:.2}s", nanos as f64 / 1_000_000_000.0)
	}
}

/// Main timed logging macro with support for all format patterns
#[macro_export]
macro_rules! log_timed {
    // Simple message with code block
    ($level:expr, $msg:expr, $code:block) => {{
        let __start = std::time::Instant::now();
        let __result = $code;
        let __elapsed = __start.elapsed().as_nanos();
        let __duration_str = $crate::interface::subsystem::logging::timed::format_duration_nanos(__elapsed);
        let __message = format!("{} (took {})", format!($msg), __duration_str);

        let __record = $crate::interface::subsystem::logging::Record::new(
            $level,
            module_path!(),
            __message,
        )
        .with_location(file!(), line!());
        $crate::interface::subsystem::logging::log(__record);
        __result
    }};

    // Format string with arguments and code block
    ($level:expr, $fmt:expr, $($arg:tt)*; $code:block) => {{
        let __start = std::time::Instant::now();
        let __result = $code;
        let __elapsed = __start.elapsed().as_nanos();
        let __duration_str = $crate::interface::subsystem::logging::timed::format_duration_nanos(__elapsed);
        let __message = format!("{} (took {})", format!($fmt, $($arg)*), __duration_str);

        let __record = $crate::interface::subsystem::logging::Record::new(
            $level,
            module_path!(),
            __message,
        )
        .with_location(file!(), line!());
        $crate::interface::subsystem::logging::log(__record);
        __result
    }};
}

/// Trace level timed logging
#[macro_export]
macro_rules! log_timed_trace {
    // Simple message
    ($msg:expr, $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Trace, $msg, $code)
    };

    // Format string with arguments - note the semicolon separator before the code block
    ($fmt:expr, $($arg:tt)*; $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Trace, $fmt, $($arg)*; $code)
    };
}

/// Debug level timed logging
#[macro_export]
macro_rules! log_timed_debug {
    // Simple message
    ($msg:expr, $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Debug, $msg, $code)
    };

    // Format string with arguments - note the semicolon separator before the code block
    ($fmt:expr, $($arg:tt)*; $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Debug, $fmt, $($arg)*; $code)
    };
}

/// Info level timed logging
#[macro_export]
macro_rules! log_timed_info {
    // Simple message
    ($msg:expr, $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Info, $msg, $code)
    };

    // Format string with arguments - note the semicolon separator before the code block
    ($fmt:expr, $($arg:tt)*; $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Info, $fmt, $($arg)*; $code)
    };
}

/// Warning level timed logging
#[macro_export]
macro_rules! log_timed_warn {
    // Simple message
    ($msg:expr, $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Warn, $msg, $code)
    };

    // Format string with arguments - note the semicolon separator before the code block
    ($fmt:expr, $($arg:tt)*; $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Warn, $fmt, $($arg)*; $code)
    };
}

/// Error level timed logging
#[macro_export]
macro_rules! log_timed_error {
    // Simple message
    ($msg:expr, $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Error, $msg, $code)
    };

    // Format string with arguments - note the semicolon separator before the code block
    ($fmt:expr, $($arg:tt)*; $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Error, $fmt, $($arg)*; $code)
    };
}

/// Critical level timed logging
#[macro_export]
macro_rules! log_timed_critical {
    // Simple message
    ($msg:expr, $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Critical, $msg, $code)
    };

    // Format string with arguments - note the semicolon separator before the code block
    ($fmt:expr, $($arg:tt)*; $code:block) => {
        $crate::log_timed!($crate::interface::subsystem::logging::LogLevel::Critical, $fmt, $($arg)*; $code)
    };
}

#[cfg(test)]
mod tests {
	use crossbeam_channel::unbounded;

	use super::*;
	use crate::{
		interface::subsystem::logging::{
			LogLevel, mock::with_mock_logger,
		},
		log_timed_debug, log_timed_info, log_timed_trace,
	};

	#[test]
	fn test_format_duration_nanos() {
		assert_eq!(format_duration_nanos(500), "500ns");
		assert_eq!(format_duration_nanos(1_500), "1.50μs");
		assert_eq!(format_duration_nanos(1_500_000), "1.50ms");
		assert_eq!(format_duration_nanos(1_500_000_000), "1.50s");
	}

	#[test]
	fn test_simple_timed_log() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let result =
				log_timed_debug!("Test operation", {
					std::thread::sleep(std::time::Duration::from_millis(10));
					42
				});
			assert_eq!(result, 42);
		});

		let record = receiver.try_recv().unwrap();
		assert_eq!(record.level, LogLevel::Debug);
		assert!(record.message.starts_with("Test operation (took "));
		assert!(record.message.ends_with(")"));
	}

	#[test]
	fn test_timed_log_with_inline_variables() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			let operation = "database init";
			let result =
				log_timed_info!("Performing {operation}", {
					std::thread::sleep(std::time::Duration::from_millis(5));
					"success"
				});
			assert_eq!(result, "success");
		});

		let record = receiver.try_recv().unwrap();
		assert_eq!(record.level, LogLevel::Info);
		assert!(record
			.message
			.starts_with("Performing database init (took "));
	}

	#[test]
	fn test_timed_log_returns_result() {
		let (sender, _receiver) = unbounded();

		with_mock_logger(sender, || {
			// Test that the macro properly returns the result of
			// the code block
			let value = log_timed_debug!("Computing value", {
				100 + 200
			});
			assert_eq!(value, 300);

			// Test with more comptokenize return type
			let vec = log_timed_info!("Creating vector", {
				vec![1, 2, 3, 4, 5]
			});
			assert_eq!(vec.len(), 5);
		});
	}

	#[test]
	fn test_all_timed_log_levels() {
		let (sender, receiver) = unbounded();

		with_mock_logger(sender, || {
			log_timed_trace!("Trace operation", { 1 });
			log_timed_debug!("Debug operation", { 2 });
			log_timed_info!("Info operation", { 3 });
			log_timed_warn!("Warn operation", { 4 });
			log_timed_error!("Error operation", { 5 });
			log_timed_critical!("Critical operation", { 6 });
		});

		let logs: Vec<_> =
			(0..6).map(|_| receiver.try_recv().unwrap()).collect();

		assert_eq!(logs[0].level, LogLevel::Trace);
		assert!(logs[0].message.starts_with("Trace operation (took "));

		assert_eq!(logs[1].level, LogLevel::Debug);
		assert!(logs[1].message.starts_with("Debug operation (took "));

		assert_eq!(logs[2].level, LogLevel::Info);
		assert!(logs[2].message.starts_with("Info operation (took "));

		assert_eq!(logs[3].level, LogLevel::Warn);
		assert!(logs[3].message.starts_with("Warn operation (took "));

		assert_eq!(logs[4].level, LogLevel::Error);
		assert!(logs[4].message.starts_with("Error operation (took "));

		assert_eq!(logs[5].level, LogLevel::Critical);
		assert!(logs[5]
			.message
			.starts_with("Critical operation (took "));
	}
}
