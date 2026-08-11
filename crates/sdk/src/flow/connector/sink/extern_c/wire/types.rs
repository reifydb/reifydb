// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

pub const SINK_MAGIC: u32 = 19681212;

pub type ExternCSinkMagicFn = extern "C" fn() -> u32;

pub type ExternCSinkCreateFn = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;
