// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

pub const OPERATOR_MAGIC: u32 = 231123;

pub const OPERATOR_ABI_TAG: u32 = 0x2810;

pub type ExternCOperatorMagicFn = extern "C" fn() -> u32;

pub type ExternCOperatorCreateFn = extern "C" fn(config: *const u8, config_len: usize, operator_id: u64) -> *mut c_void;
