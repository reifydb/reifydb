// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::Result;

use crate::transaction::admin::AdminTransaction;

/// Optional subsystem hook that can materialize pending view mutations into
/// the active test context without committing the transaction.
pub trait TestingViewMutationCaptor: Send + Sync {
	fn capture(&self, txn: &mut AdminTransaction) -> Result<()>;
}
