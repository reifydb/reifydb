// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! ReifyDB Tracing Subsystem
//!
//! High-performance tracing system using tracing_subscriber.
//! Supports per-crate/module filtering, structured logging, and
//! extensible backends via tracing_subscriber's Layer trait.
//!
//! # Example
//!
//! ```ignore
//! use reifydb_sub_tracing::TracingBuilder;
//!
//! let subsystem = TracingBuilder::new()
//!     .with_console(|console| console.color(true))
//!     .with_filter("info,reifydb_engine=debug")
//!     .build();
//! ```

mod backend;
mod builder;
mod factory;
mod subsystem;

pub use backend::ConsoleBuilder;
pub use builder::TracingBuilder;
pub use factory::TracingSubsystemFactory;
pub use reifydb_type::Result;
pub use subsystem::TracingSubsystem;
pub use tracing::instrument;
// Re-export tracing macros for convenience
pub use tracing::{debug, error, info, trace, warn};
