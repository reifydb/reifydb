// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion, interface::catalog::id::SubscriptionId, metric::ExecutionMetrics,
	value::column::columns::Columns,
};
use reifydb_engine::{
	engine::StandardEngine,
	subscription::{HydrateError, SubscriptionServiceRef},
};
use reifydb_transaction::multi::lease::VersionLeaseGuard;
use reifydb_type::value::identity::IdentityId;
use tokio::task::spawn_blocking;

pub async fn run_hydrate(
	service: SubscriptionServiceRef,
	engine: StandardEngine,
	subscription_id: SubscriptionId,
	identity: IdentityId,
	lease: VersionLeaseGuard,
	max_rows: u64,
) -> Result<(CommitVersion, Option<(Columns, ExecutionMetrics)>), HydrateError> {
	let outcome = spawn_blocking(move || service.hydrate(subscription_id, &engine, identity, lease, max_rows))
		.await
		.map_err(|e| HydrateError::Internal(e.to_string()))??;

	let version = outcome.version;
	let metrics = outcome.metrics;
	let merged = Columns::concat(outcome.batches)?;
	Ok((version, merged.map(|cols| (cols, metrics))))
}
