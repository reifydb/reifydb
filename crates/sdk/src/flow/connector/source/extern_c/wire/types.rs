// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

pub const SOURCE_MAGIC: u32 = 19661506;

pub type ExternCSourceMagicFn = extern "C" fn() -> u32;

pub type ExternCSourceCreateFn = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;
