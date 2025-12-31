// SPDX-License-Identifier: MIT
// Copyright (c) 2025 ReifyDB

/// Capability: Operator can process inserts
pub const CAPABILITY_INSERT: u32 = 1 << 0; // 0x01

/// Capability: Operator can process updates
pub const CAPABILITY_UPDATE: u32 = 1 << 1; // 0x02

/// Capability: Operator can process deletes
pub const CAPABILITY_DELETE: u32 = 1 << 2; // 0x04

/// Capability: Operator supports pull(), which is required for join and window flows
pub const CAPABILITY_PULL: u32 = 1 << 3; // 0x08

/// Capability: Operator can drop data without cascading change
pub const CAPABILITY_DROP: u32 = 1 << 4; // 0x10

/// Capability: Operator wants periodic tick() callbacks
pub const CAPABILITY_TICK: u32 = 1 << 5; // 0x20

/// All standard capabilities (Insert + Update + Delete + Pull)
pub const CAPABILITY_ALL_STANDARD: u32 = CAPABILITY_INSERT | CAPABILITY_UPDATE | CAPABILITY_DELETE | CAPABILITY_PULL;

/// Helper to check if a capability is set
///
/// # Example
/// ```
/// use reifydb_abi::*;
///
/// let caps = CAPABILITY_INSERT | CAPABILITY_UPDATE;
/// assert!(has_capability(caps, CAPABILITY_INSERT));
/// assert!(has_capability(caps, CAPABILITY_UPDATE));
/// assert!(!has_capability(caps, CAPABILITY_DELETE));
/// ```
#[inline]
pub const fn has_capability(capabilities: u32, capability: u32) -> bool {
	(capabilities & capability) != 0
}
