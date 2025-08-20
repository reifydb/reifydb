// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! ReifyDB Logging System
//!
//! High-performance, extensible logging system.
//! Supports multiple backends and structured logging.

mod backend;
mod buffer;
mod builder;
mod factory;
mod metrics;
mod processor;
mod subsystem;

#[cfg(debug_assertions)]
mod test_utils;

pub use backend::{ConsoleBuilder, FormatStyle};
pub use builder::LoggingBuilder;
pub use factory::LoggingSubsystemFactory;
pub use metrics::LoggingMetrics;
pub use subsystem::LoggingSubsystem;

#[cfg(debug_assertions)]
pub use test_utils::{TestLoggerHandle, LoggingBuilderTestExt};
