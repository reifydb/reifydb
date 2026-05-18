// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

pub const OPERATOR_MAGIC: u32 = 231123;

pub type OperatorMagicFnFFI = extern "C" fn() -> u32;

pub type OperatorCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize, operator_id: u64) -> *mut c_void;
