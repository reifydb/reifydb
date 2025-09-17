// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use closer::Closer;
pub use watermark::WaterMark;

mod closer;
mod process;
mod watermark;

// Configuration constants for watermark processing
pub const MAX_WAITERS: usize = 10000;
pub const MAX_PENDING: usize = 100000;
pub const OLD_VERSION_THRESHOLD: u64 = 10; // Versions older than this are considered irrelevant
pub const PENDING_CLEANUP_THRESHOLD: u64 = 1000; // Clean up pending entries older than this
pub const WATERMARK_CHANNEL_SIZE: usize = 1000; // Channel buffer size for watermark messages
