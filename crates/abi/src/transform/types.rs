// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

/// Magic number to identify valid FFI transform libraries
///
/// Libraries must export a `ffi_transform_magic` symbol that returns this value
/// to be recognized as valid FFI transforms.
pub const TRANSFORM_MAGIC: u32 = 230424;

/// Function signature for the magic number export
///
/// FFI transform libraries must export this function to be recognized as valid transforms.
pub type TransformMagicFnFFI = extern "C" fn() -> u32;

/// Factory function type for creating transform instances
pub type TransformCreateFnFFI = extern "C" fn(config: *const u8, config_len: usize) -> *mut c_void;
