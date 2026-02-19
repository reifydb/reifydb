// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#![cfg_attr(not(debug_assertions), deny(warnings))]

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

pub mod backend;
pub mod builder;
pub mod factory;
pub mod subsystem;
