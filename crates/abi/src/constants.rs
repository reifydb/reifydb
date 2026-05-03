// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Current API version
///
/// This version must be incremented when making breaking changes to the FFI interface.
/// Operators compiled against different API versions will be rejected.
pub const CURRENT_API: u32 = 1;

pub const OPERATOR_MAGIC: u32 = 231123;

pub type FFIOperatorMagicFn = extern "C" fn() -> u32;

pub const FFI_OK: i32 = 0;

pub const FFI_NOT_FOUND: i32 = 1;

pub const FFI_END_OF_ITERATION: i32 = 1;

pub const FFI_ERROR_NULL_PTR: i32 = -1;

pub const FFI_ERROR_INTERNAL: i32 = -2;

pub const FFI_ERROR_ALLOC: i32 = -3;

pub const FFI_ERROR_INVALID_UTF8: i32 = -4;

pub const FFI_ERROR_MARSHAL: i32 = -5;

pub const TRANSFORM_MAGIC: u32 = 230424;

pub type FFITransformMagicFn = extern "C" fn() -> u32;

pub const PROCEDURE_MAGIC: u32 = 19880803;

pub type FFIProcedureMagicFn = extern "C" fn() -> u32;
