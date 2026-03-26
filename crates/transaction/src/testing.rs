// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::Result;

use crate::transaction::TestTransaction;

/// Processes transactional flows inline within the current test transaction,
/// materialising view rows that would normally be computed during pre-commit.
pub trait TestFlowProcessor: Send + Sync {
	fn process(&self, txn: &mut TestTransaction<'_>) -> Result<()>;
}
