// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::Result;

use crate::transaction::admin::AdminTransaction;

pub trait TestingViewMutationCaptor: Send + Sync {
	fn capture(&self, txn: &mut AdminTransaction) -> Result<()>;
}
