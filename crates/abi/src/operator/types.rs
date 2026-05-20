// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

pub const OPERATOR_MAGIC: u32 = 231123;

pub type OperatorMagicFnFFI = extern "C" fn() -> u32;

pub type OperatorCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize, operator_id: u64) -> *mut c_void;
