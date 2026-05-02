// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::ffi::c_void;

use super::ttl::TtlFFI;

/// Magic number to identify valid FFI operator libraries
///
/// Libraries must export a `ffi_operator_magic` symbol that returns this value
/// to be recognized as valid FFI operators.
pub const OPERATOR_MAGIC: u32 = 231123;

/// Function signature for the magic number export
///
/// FFI operator libraries must export this function to be recognized as valid operators.
pub type OperatorMagicFnFFI = extern "C" fn() -> u32;

/// Factory function type for creating operator instances.
///
/// `ttl` is `null` when DDL had no `WITH { ttl: ... }` clause; otherwise
/// points to a host-allocated `TtlFFI` valid for the duration of the call.
/// The guest must not retain the pointer past return.
pub type OperatorCreateFnFFI =
	extern "C" fn(config: *const u8, config_len: usize, operator_id: u64, ttl: *const TtlFFI) -> *mut c_void;
