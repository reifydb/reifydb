// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Shared test harness used by the snapshot test binaries. The actual
//! testscript Runner lives in `runner.rs`.

#![allow(dead_code, unused_imports)]

mod runner;

pub use runner::{Runner, format_cdc, format_change};
