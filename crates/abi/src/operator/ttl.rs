// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// FFI projection of operator-state TTL configuration.
///
/// Passed to `OperatorCreateFnFFI` as `*const TtlFFI` alongside config bytes.
/// Null pointer means no TTL (the absent-clause default; state grows unbounded).
///
/// `cleanup_mode` is intentionally absent from the FFI surface: operator-state
/// cleanup is always silent drop. `mode: delete` is rejected at compile time
/// before reaching this struct.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TtlFFI {
	/// Duration in nanoseconds after which entries expire.
	pub duration_nanos: u64,
	/// Which timestamp to measure from.
	/// 0 = `created_at` (matches `TtlAnchor::Created`)
	/// 1 = `updated_at` (matches `TtlAnchor::Updated`)
	pub anchor: u8,
}

/// Anchor discriminator for `TtlFFI::anchor`.
pub const TTL_ANCHOR_CREATED: u8 = 0;
pub const TTL_ANCHOR_UPDATED: u8 = 1;
