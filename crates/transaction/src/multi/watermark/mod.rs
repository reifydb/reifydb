// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

pub mod actor;
#[allow(clippy::module_inception)]
pub mod watermark;

pub const MAX_WAITERS: usize = 10000;
pub const MAX_PENDING: usize = 100000;
pub const OLD_VERSION_THRESHOLD: u64 = 1000;
pub const PENDING_CLEANUP_THRESHOLD: u64 = 1000;
pub const WATERMARK_CHANNEL_SIZE: usize = 1000;
