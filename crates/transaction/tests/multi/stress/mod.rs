// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::runtime::ComputePool;
use reifydb_transaction::multi::TransactionMulti;

pub fn test_compute_pool() -> ComputePool {
	ComputePool::new(2, 8)
}

pub fn test_multi() -> TransactionMulti {
	TransactionMulti::testing(test_compute_pool())
}

#[cfg(test)]
mod oracle_stress;
