// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt;

use reifydb_catalog::{drop_flow_by_name, drop_subscription};
use reifydb_core::{
	error::diagnostic::internal::internal,
	interface::catalog::{
		id::SubscriptionId,
		subscription::{subscription_flow_name, subscription_flow_namespace},
	},
};
use reifydb_engine::engine::StandardEngine;
use reifydb_type::{
	Result as TypeResult,
	error::Error,
	params::Params,
	value::{Value, identity::IdentityId},
};
use tracing::{debug, error};

use crate::{
	execute::{ExecuteError, execute_subscription},
	state::AppState,
};

/// Error type for subscription creation.
pub enum CreateSubscriptionError {
	Execute(ExecuteError),
	ExtractionFailed,
}

impl fmt::Display for CreateSubscriptionError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CreateSubscriptionError::Execute(e) => write!(f, "{}", e),
			CreateSubscriptionError::ExtractionFailed => write!(f, "Failed to extract subscription ID"),
		}
	}
}

impl fmt::Debug for CreateSubscriptionError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			CreateSubscriptionError::Execute(e) => f.debug_tuple("Execute").field(e).finish(),
			CreateSubscriptionError::ExtractionFailed => write!(f, "ExtractionFailed"),
		}
	}
}

impl From<ExecuteError> for CreateSubscriptionError {
	fn from(err: ExecuteError) -> Self {
		CreateSubscriptionError::Execute(err)
	}
}

/// Execute `CREATE SUBSCRIPTION AS { query }` and extract the subscription ID from the result.
pub async fn create_subscription(
	state: &AppState,
	identity: IdentityId,
	query: &str,
) -> Result<SubscriptionId, CreateSubscriptionError> {
	let statement = format!("CREATE SUBSCRIPTION AS {{ {} }}", query);
	debug!("Subscription statement: {}", statement);

	let frames = execute_subscription(
		state.actor_system(),
		state.engine_clone(),
		statement,
		identity,
		Params::None,
		state.query_timeout(),
	)
	.await?;

	frames.first()
		.and_then(|frame| frame.columns.iter().find(|c| c.name == "subscription_id"))
		.and_then(|col| {
			if col.data.len() > 0 {
				Some(col.data.get_value(0))
			} else {
				None
			}
		})
		.and_then(|value| match value {
			Value::Uint8(id) => Some(SubscriptionId(id)),
			other => {
				error!("subscription_id column has wrong type: {:?}", other);
				None
			}
		})
		.ok_or(CreateSubscriptionError::ExtractionFailed)
}

/// Synchronous cleanup: begin subscription txn, drop flow, drop subscription, commit.
pub fn cleanup_subscription_sync(engine: &StandardEngine, subscription_id: SubscriptionId) -> TypeResult<()> {
	let mut txn = engine.begin_subscription()?;
	let flow_name = subscription_flow_name(subscription_id);
	let namespace_id = subscription_flow_namespace();
	drop_flow_by_name(txn.as_admin_mut(), namespace_id, &flow_name)?;
	drop_subscription(txn.as_admin_mut(), subscription_id)?;
	txn.commit()?;
	Ok(())
}

/// Async cleanup via the compute pool.
pub async fn cleanup_subscription(state: &AppState, subscription_id: SubscriptionId) -> TypeResult<()> {
	let engine = state.engine_clone();
	let system = state.actor_system();

	system.compute(move || cleanup_subscription_sync(&engine, subscription_id))
		.await
		.map_err(|e| Error(internal(format!("Compute pool error: {:?}", e))))?
}
