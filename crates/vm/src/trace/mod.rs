// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! VM execution tracing for debugging.
//!
//! This module provides full instrumentation of VM execution, capturing:
//! - Each instruction executed with raw bytecode and decoded form
//! - Full state snapshots after each step
//! - Delta information showing what changed
//!
//! Enable with the `trace` feature flag.

pub mod diff;
pub mod entry;
pub mod format;
pub mod snapshot;
pub mod tracer;
