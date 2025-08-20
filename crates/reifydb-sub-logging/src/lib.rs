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

pub use backend::ConsoleBuilder;
pub use builder::LoggingBuilder;
pub use factory::LoggingSubsystemFactory;
pub use metrics::LoggingMetrics;
pub use subsystem::LoggingSubsystem;
