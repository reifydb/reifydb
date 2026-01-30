// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

use super::diff::DiffFFI;

/// FFI-safe representation of change origin
///
/// Encodes both Internal and External origins:
/// - origin: 0 = Internal, 1 = External.Table, 2 = External.View, 3 = External.TableVirtual, 4 = External.RingBuffer
/// - id: For Internal, this is the FlowNodeId. For External, this is the source ID (TableId, ViewId, etc.)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OriginFFI {
	pub origin: u8,
	pub id: u64,
}

/// FFI-safe change containing multiple diffs
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ChangeFFI {
	/// Origin of this change
	pub origin: OriginFFI,
	/// Number of diffs in the change
	pub diff_count: usize,
	/// Pointer to array of diffs
	pub diffs: *const DiffFFI,
	/// Version number for this change
	pub version: u64,
}

impl ChangeFFI {
	/// Create an empty change with Internal origin 0
	pub const fn empty() -> Self {
		Self {
			origin: OriginFFI {
				origin: 0,
				id: 0,
			},
			diff_count: 0,
			diffs: core::ptr::null(),
			version: 0,
		}
	}
}
