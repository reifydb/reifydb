// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Marshaling implementations for FFI types
//!
//! Marshal/unmarshal methods are implemented directly on Arena.

mod column;
mod types;
mod util;

pub(crate) mod flow;
