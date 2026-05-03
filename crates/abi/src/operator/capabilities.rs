// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

/// Capability: Operator can process inserts
pub const CAPABILITY_INSERT: u32 = 1 << 0;

pub const CAPABILITY_UPDATE: u32 = 1 << 1;

pub const CAPABILITY_DELETE: u32 = 1 << 2;

pub const CAPABILITY_PULL: u32 = 1 << 3;

pub const CAPABILITY_DROP: u32 = 1 << 4;

pub const CAPABILITY_TICK: u32 = 1 << 5;

pub const CAPABILITY_ALL_STANDARD: u32 = CAPABILITY_INSERT | CAPABILITY_UPDATE | CAPABILITY_DELETE | CAPABILITY_PULL;

#[inline]
pub const fn has_capability(capabilities: u32, capability: u32) -> bool {
	(capabilities & capability) != 0
}
