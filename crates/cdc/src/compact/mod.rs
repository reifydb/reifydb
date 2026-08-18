// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(not(target_arch = "wasm32"))]
pub mod actor;
pub mod cache;

use reifydb_core::common::CommitVersion;

#[derive(Debug, Clone)]
pub struct CompactBlockSummary {
	pub min_version: CommitVersion,
	pub max_version: CommitVersion,
	pub num_entries: usize,
	pub compressed_bytes: usize,
}
