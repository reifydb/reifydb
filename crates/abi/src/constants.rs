// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub const CURRENT_API: u32 = 2;

pub const OPERATOR_MAGIC: u32 = 231123;

pub const OPERATOR_ABI_TAG: u32 = 0x2810;

pub type ExternCOperatorMagicFn = extern "C" fn() -> u32;

pub const EXTERN_C_OK: i32 = 0;

pub const EXTERN_C_NOT_FOUND: i32 = 1;

pub const EXTERN_C_SAMPLE_NO_DATA: i32 = 2;

pub const EXTERN_C_END_OF_ITERATION: i32 = 1;

pub const EXTERN_C_ERROR_NULL_PTR: i32 = -1;

pub const EXTERN_C_ERROR_INTERNAL: i32 = -2;

pub const EXTERN_C_ERROR_ALLOC: i32 = -3;

pub const EXTERN_C_ERROR_INVALID_UTF8: i32 = -4;

pub const EXTERN_C_ERROR_MARSHAL: i32 = -5;

pub const GROUP_ABSENT: u64 = 0;

pub const TRANSFORM_MAGIC: u32 = 230424;

pub type ExternCTransformMagicFn = extern "C" fn() -> u32;

pub const PROCEDURE_MAGIC: u32 = 19880803;

pub type ExternCProcedureMagicFn = extern "C" fn() -> u32;
