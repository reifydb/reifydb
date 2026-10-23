// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::flow::dag::FlowDag;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::Result;

pub trait FlowRegistrationCheck: Send + Sync {
	fn check(&self, txn: &mut AdminTransaction, dag: &FlowDag) -> Result<()>;
}

pub type FlowRegistrationCheckRef = Arc<dyn FlowRegistrationCheck>;
