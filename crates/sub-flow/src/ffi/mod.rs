// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Host runtime for FFI operators
//!
//! This module provides the host-side implementation for FFI operator integration,
//! including type marshalling, memory management, and callback implementations.

pub mod callbacks;
pub mod context;
pub mod loader;
