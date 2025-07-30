// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

/// Statistics about pool usage
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub available: usize,
    pub total_acquired: usize,
    pub total_released: usize,
}

impl Default for PoolStats {
    fn default() -> Self {
        Self { available: 0, total_acquired: 0, total_released: 0 }
    }
}
