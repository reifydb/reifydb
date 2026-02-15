// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Constants and version information for the FFI operator API

/// Current API version
///
/// This version must be incremented when making breaking changes to the FFI interface.
/// Operators compiled against different API versions will be rejected.
pub const CURRENT_API: u32 = 1;

/// Magic number to identify valid FFI operator libraries
///
/// Libraries must export a `ffi_operator_magic` symbol that returns this value
/// to be recognized as valid FFI operators.
pub const OPERATOR_MAGIC: u32 = 231123;

/// Function signature for the magic number export
///
/// FFI operator libraries must export this function to be recognized as valid operators.
pub type FFIOperatorMagicFn = extern "C" fn() -> u32;

// =============================
// FFI Return Codes
// =============================

/// FFI return code: Operation succeeded, value found, or iterator has next item
pub const FFI_OK: i32 = 0;

/// FFI return code: Query succeeded but entity doesn't exist
pub const FFI_NOT_FOUND: i32 = 1;

/// FFI return code: Iterator has no more items (alias for FFI_NOT_FOUND)
pub const FFI_END_OF_ITERATION: i32 = 1;

/// FFI error code: Null pointer passed as parameter
pub const FFI_ERROR_NULL_PTR: i32 = -1;

/// FFI error code: Internal error during operation (transaction error, etc.)
pub const FFI_ERROR_INTERNAL: i32 = -2;

/// FFI error code: Memory allocation failed
pub const FFI_ERROR_ALLOC: i32 = -3;

/// FFI error code: Invalid UTF-8 in string parameter
pub const FFI_ERROR_INVALID_UTF8: i32 = -4;

/// FFI error code: Failed to marshal Rust type to FFI struct
pub const FFI_ERROR_MARSHAL: i32 = -5;

/// Magic number to identify valid FFI transform libraries
///
/// Libraries must export a `ffi_transform_magic` symbol that returns this value
/// to be recognized as valid FFI transforms.
pub const TRANSFORM_MAGIC: u32 = 230424;

/// Function signature for the transform magic number export
///
/// FFI transform libraries must export this function to be recognized as valid transforms.
pub type FFITransformMagicFn = extern "C" fn() -> u32;
