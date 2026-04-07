// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::fmt;

use reifydb_core::{error::diagnostic::internal::internal, interface::catalog::id::SubscriptionId};
use reifydb_engine::engine::StandardEngine;
use reifydb_type::{
	Result as TypeResult,
	error::Error,
	params::Params,
	value::{Value, identity::IdentityId},
};
use tokio::task::spawn_blocking;
use tracing::{debug, error};

use crate::{
	execute::{ExecuteError, execute},
	interceptor::{Operation, RequestContext, RequestMetadata},
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

/// Result of creating a subscription: either local or remote.
pub enum CreateSubscriptionResult {
	Local(SubscriptionId),
	Remote {
		address: String,
		query: String,
	},
}

/// Execute `CREATE SUBSCRIPTION AS { query }` and extract the subscription ID from the result.
pub async fn create_subscription(
	state: &AppState,
	identity: IdentityId,
	query: &str,
	metadata: RequestMetadata,
) -> Result<CreateSubscriptionResult, CreateSubscriptionError> {
	let statement = format!("CREATE SUBSCRIPTION AS {{ {} }}", query);
	debug!("Subscription statement: {}", statement);

	let ctx = RequestContext {
		identity,
		operation: Operation::Subscribe,
		statements: vec![statement],
		params: Params::None,
		metadata,
	};

	let (frames, _duration) =
		execute(state.request_interceptors(), state.engine_clone(), ctx, state.query_timeout(), state.clock())
			.await?;

	let frame = frames.first().ok_or(CreateSubscriptionError::ExtractionFailed)?;

	// Check if result indicates a remote source
	if let Some(addr_col) = frame.columns.iter().find(|c| c.name == "remote_address") {
		let address = if !addr_col.data.is_empty() {
			match addr_col.data.get_value(0) {
				Value::Utf8(s) => s,
				_ => return Err(CreateSubscriptionError::ExtractionFailed),
			}
		} else {
			return Err(CreateSubscriptionError::ExtractionFailed);
		};

		let rql = frame
			.columns
			.iter()
			.find(|c| c.name == "remote_rql")
			.and_then(|col| {
				if !col.data.is_empty() {
					match col.data.get_value(0) {
						Value::Utf8(s) => Some(s),
						_ => None,
					}
				} else {
					None
				}
			})
			.ok_or(CreateSubscriptionError::ExtractionFailed)?;

		return Ok(CreateSubscriptionResult::Remote {
			address,
			query: rql,
		});
	}

	// Normal local path: extract subscription_id
	frame.columns
		.iter()
		.find(|c| c.name == "subscription_id")
		.and_then(|col| {
			if !col.data.is_empty() {
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
		.map(CreateSubscriptionResult::Local)
		.ok_or(CreateSubscriptionError::ExtractionFailed)
}

/// Synchronous cleanup: drop subscription via DDL.
pub fn cleanup_subscription_sync(engine: &StandardEngine, subscription_id: SubscriptionId) -> TypeResult<()> {
	let rql = format!("drop subscription if exists subscription_{};", subscription_id.0);
	engine.admin_as(IdentityId::system(), &rql, Params::None).map(|r| r.frames)?;
	Ok(())
}

/// Async cleanup via a blocking task.
pub async fn cleanup_subscription(state: &AppState, subscription_id: SubscriptionId) -> TypeResult<()> {
	let engine = state.engine_clone();

	spawn_blocking(move || cleanup_subscription_sync(&engine, subscription_id))
		.await
		.map_err(|e| Error(Box::new(internal(format!("Blocking task error: {:?}", e)))))?
}
