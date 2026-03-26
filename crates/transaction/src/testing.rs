// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::Result;

use crate::transaction::TestTransaction;

/// Processes all flows (transactional + deferred) inline within the current
/// test transaction. Used by `testing::views::changed()` to materialise view
/// rows on demand rather than waiting for commit or async CDC processing.
pub trait TestFlowProcessor: Send + Sync {
	fn process(&self, txn: &mut TestTransaction<'_>) -> Result<()>;
}
