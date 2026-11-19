// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod advancer;
#[allow(clippy::module_inception)]
pub mod done_until;
pub mod state;

pub const MAX_WAITERS: usize = 10000;
pub const OLD_VERSION_THRESHOLD: u64 = 1000;
pub const MAX_INLINE_ADVANCE: usize = 64;
pub const ADVANCER_CHUNK: usize = 256;
