// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Logging macros for convenient usage

/// Main logging macro with support for structured fields
#[macro_export]
macro_rules! log {
    // Format string with arguments
    ($level:expr, $($arg:tt)+) => {{
        let message = format!($($arg)+);
        let record = $crate::LogRecord::new(
            $level,
            module_path!(),
            message,
        )
        .with_location(file!(), line!());
        $crate::log(record);
    }};
}

/// Trace level logging
#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => {
        $crate::log!($crate::LogLevel::Trace, $($arg)*)
    };
}

/// Debug level logging
#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => {
        $crate::log!($crate::LogLevel::Debug, $($arg)*)
    };
}

/// Info level logging
#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => {
        $crate::log!($crate::LogLevel::Info, $($arg)*)
    };
}

/// Warning level logging
#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => {
        $crate::log!($crate::LogLevel::Warn, $($arg)*)
    };
}

/// Error level logging
#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => {
        $crate::log!($crate::LogLevel::Error, $($arg)*)
    };
}

/// Critical level logging with guaranteed synchronous delivery
#[macro_export]
macro_rules! critical {
    ($($arg:tt)*) => {
        $crate::log!($crate::LogLevel::Critical, $($arg)*)
    };
}

/// Structured logging macro with fields
#[macro_export]
macro_rules! log_with_fields {
    ($level:expr, $msg:expr, $($key:expr => $value:expr),+ $(,)?) => {{
        let mut record = $crate::LogRecord::new(
            $level,
            module_path!(),
            $msg,
        )
        .with_location(file!(), line!());
        $(
            record = record.with_field($key, $value);
        )+
        $crate::log(record);
    }};
}

/// Info level logging with structured fields
#[macro_export]
macro_rules! info_with_fields {
    ($msg:expr, $($key:expr => $value:expr),+ $(,)?) => {
        $crate::log_with_fields!($crate::LogLevel::Info, $msg, $($key => $value),+)
    };
}

/// Debug level logging with structured fields
#[macro_export]
macro_rules! debug_with_fields {
    ($msg:expr, $($key:expr => $value:expr),+ $(,)?) => {
        $crate::log_with_fields!($crate::LogLevel::Debug, $msg, $($key => $value),+)
    };
}

/// Error level logging with structured fields
#[macro_export]
macro_rules! error_with_fields {
    ($msg:expr, $($key:expr => $value:expr),+ $(,)?) => {
        $crate::log_with_fields!($crate::LogLevel::Error, $msg, $($key => $value),+)
    };
}