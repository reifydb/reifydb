// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! C ABI definitions for ReifyDB source and sink connectors

pub mod sink;
pub mod source;

/// Magic number to identify valid FFI source connector libraries
pub const SOURCE_MAGIC: u32 = 19661506;

/// Magic number to identify valid FFI sink connector libraries
pub const SINK_MAGIC: u32 = 19681212;
