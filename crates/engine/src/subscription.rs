// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_core::interface::catalog::id::SubscriptionId;
use reifydb_rql::flow::flow::FlowDag;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::Result;

/// Service interface for managing ephemeral subscriptions.
///
/// Implemented by `reifydb-sub-subscription` and registered in IoC as
/// `Arc<dyn SubscriptionService>`. The engine's DDL code resolves this
/// to create and drop subscriptions.
pub trait SubscriptionService: Send + Sync {
	/// Generate a new unique subscription ID.
	fn next_id(&self) -> SubscriptionId;

	/// Register a subscription with a pre-compiled flow DAG.
	fn register_subscription(
		&self,
		id: SubscriptionId,
		flow_dag: FlowDag,
		column_names: Vec<String>,
		txn: &mut Transaction<'_>,
	) -> Result<()>;

	/// Unregister a subscription.
	fn unregister_subscription(&self, id: &SubscriptionId) -> Result<()>;
}

/// Convenience type alias for IoC registration.
pub type SubscriptionServiceRef = Arc<dyn SubscriptionService>;
