// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Actor model for ReifyDB.
//!
//! This module provides an actor model that provides identical semantics whether
//! running on a single thread (WASM) or multiple OS threads (native).
//!
//! # Execution Model
//!
//! - **Native**: Actors run on threads with shared rayon pool for compute
//! - **WASM**: Messages are processed inline (synchronously) when sent
//!
//! All actor states must be `Send`.

pub mod context;
pub mod mailbox;
pub mod system;
pub mod testing;
pub mod timers;
pub mod traits;
