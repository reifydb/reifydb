// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

/// Magic number to identify valid FFI procedure libraries
///
/// Libraries must export a `ffi_procedure_magic` symbol that returns this value
/// to be recognized as valid FFI procedures.
pub const PROCEDURE_MAGIC: u32 = 19880803;

/// Function signature for the magic number export
///
/// FFI procedure libraries must export this function to be recognized as valid procedures.
pub type ProcedureMagicFnFFI = extern "C" fn() -> u32;

/// Factory function type for creating procedure instances
pub type ProcedureCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;
