// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! C ABI shapes for FFI sources and sinks. The two magic constants are the values an extension stamps into its
//! descriptor so the host can confirm at load time that a connector binary actually exports a connector and not
//! some other kind of FFI object - mismatched magic is a hard load failure.

pub mod sink;
pub mod source;

pub const SOURCE_MAGIC: u32 = 19661506;

pub const SINK_MAGIC: u32 = 19681212;
