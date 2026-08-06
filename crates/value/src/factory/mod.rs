// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Constructors for test fixtures, shared workspace-wide so a helper is written once rather than
//! per test file. Where the argument is a raw integer the unit lives in the function name, since
//! the type cannot carry it.

pub mod time;

pub use time::{at_millis, at_nanos, millis, secs};
