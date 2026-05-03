// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_rql::flow::flow::FlowDag;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;

pub trait SubscriptionService: Send + Sync {
	fn next_id(&self) -> SubscriptionId;

	fn register_subscription(
		&self,
		id: SubscriptionId,
		flow_dag: FlowDag,
		column_names: Vec<String>,
		txn: &mut Transaction<'_>,
	) -> Result<()>;

	fn unregister_subscription(&self, id: &SubscriptionId) -> Result<()>;
}

pub type SubscriptionServiceRef = Arc<dyn SubscriptionService>;
