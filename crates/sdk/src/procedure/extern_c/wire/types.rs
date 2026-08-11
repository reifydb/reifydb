// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::ffi::c_void;

pub const PROCEDURE_MAGIC: u32 = 19880803;

pub const PROCEDURE_ABI_TAG: u32 = 0x2820;

pub type ExternCProcedureMagicFn = extern "C" fn() -> u32;

pub type ExternCProcedureCreateFn = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;
