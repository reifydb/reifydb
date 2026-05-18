// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

pub const PROCEDURE_MAGIC: u32 = 19880803;

pub type ProcedureMagicFnFFI = extern "C" fn() -> u32;

pub type ProcedureCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;
