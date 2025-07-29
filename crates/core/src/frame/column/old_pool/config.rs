// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

/// Configuration for buffer pools.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of buffers to keep per size bucket.
    pub max_buffers_per_bucket: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self { max_buffers_per_bucket: 64 }
    }
}
