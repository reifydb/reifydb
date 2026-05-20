// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

pub const TRANSFORM_MAGIC: u32 = 230424;

pub type TransformMagicFnFFI = extern "C" fn() -> u32;

pub type TransformCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;
