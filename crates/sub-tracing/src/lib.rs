// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
#![cfg_attr(not(debug_assertions), deny(clippy::disallowed_methods))]
#![cfg_attr(debug_assertions, warn(clippy::disallowed_methods))]
#![cfg_attr(not(debug_assertions), deny(warnings))]
#![allow(clippy::tabs_in_doc_comments)]

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
