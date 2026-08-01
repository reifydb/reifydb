// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! C ABI shapes for FFI sources and sinks. The magic constants let the host reject, at load time, a binary that
//! exports some other kind of FFI object; a mismatch is a hard load failure.

pub mod sink;
pub mod source;

pub const SOURCE_MAGIC: u32 = 19661506;

pub const SINK_MAGIC: u32 = 19681212;
