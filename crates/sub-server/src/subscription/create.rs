// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{id::SubscriptionId, subscription::HydrationConfig};
use reifydb_value::value::duration::Duration;
#[cfg(not(reifydb_single_threaded))]
use reifydb_value::value::frame::{column::FrameColumn, frame::Frame};
#[cfg(not(reifydb_single_threaded))]
use reifydb_value::{
	params::Params,
	value::{Value, identity::IdentityId},
};
#[cfg(not(reifydb_single_threaded))]
use tracing::{debug, error};

pub enum CreateSubscriptionResult {
	Local {
		id: SubscriptionId,
		hydration: HydrationConfig,
		throttle: Option<Duration>,
		linger: Option<Duration>,
	},
	Remote {
		address: String,
		body: String,
		token: Option<String>,
		hydration: HydrationConfig,
		throttle: Option<Duration>,
		linger: Option<Duration>,
	},
}

#[cfg(not(reifydb_single_threaded))]
use reifydb_core::actors::server::Operation;

#[cfg(not(reifydb_single_threaded))]
use crate::{
	dispatch::dispatch_subscribe,
	interceptor::{RequestContext, RequestMetadata},
	state::AppState,
	subscription::errors::CreateSubscriptionError,
};

#[cfg(not(reifydb_single_threaded))]
pub async fn create_subscription(
	state: &AppState,
	identity: IdentityId,
	rql: &str,
	params: Params,
	metadata: RequestMetadata,
) -> Result<CreateSubscriptionResult, CreateSubscriptionError> {
	debug!("Subscription rql: {}", rql);

	let ctx = RequestContext {
		identity,
		operation: Operation::Subscribe,
		rql: rql.to_string(),
		params,
		metadata,
	};
	let (frames, _metrics) = dispatch_subscribe(state, ctx).await?;
	let frame = frames.first().ok_or(CreateSubscriptionError::ExtractionFailed)?;

	if let Some(remote) = extract_remote_result(frame)? {
		return Ok(remote);
	}
	extract_local_result(frame)
}

#[cfg(not(reifydb_single_threaded))]
fn extract_remote_result(frame: &Frame) -> Result<Option<CreateSubscriptionResult>, CreateSubscriptionError> {
	let Some(addr_col) = frame.columns.iter().find(|c| c.name == "remote_address") else {
		return Ok(None);
	};
	let address = match first_utf8_value(addr_col) {
		Some(s) => s,
		None => return Err(CreateSubscriptionError::ExtractionFailed),
	};
	let body = frame
		.columns
		.iter()
		.find(|c| c.name == "remote_body")
		.and_then(first_utf8_value)
		.ok_or(CreateSubscriptionError::ExtractionFailed)?;
	let token = frame.columns.iter().find(|c| c.name == "remote_token").and_then(first_utf8_value);
	let enabled = first_bool_value(frame, "remote_hydration_enabled").unwrap_or(true);
	let max_rows = first_uint8_value(frame, "remote_hydration_max_rows");
	let throttle = first_uint8_value(frame, "remote_throttle_ms")
		.map(|ms| Duration::from_milliseconds(ms as i64).unwrap());
	let linger =
		first_uint8_value(frame, "remote_linger_ms").map(|ms| Duration::from_milliseconds(ms as i64).unwrap());
	Ok(Some(CreateSubscriptionResult::Remote {
		address,
		body,
		token,
		hydration: HydrationConfig {
			enabled,
			max_rows,
		},
		throttle,
		linger,
	}))
}

#[cfg(not(reifydb_single_threaded))]
fn extract_local_result(frame: &Frame) -> Result<CreateSubscriptionResult, CreateSubscriptionError> {
	let id = frame
		.columns
		.iter()
		.find(|c| c.name == "subscription_id")
		.and_then(|col| {
			if col.data.is_empty() {
				None
			} else {
				Some(col.data.get_value(0))
			}
		})
		.and_then(|value| match value {
			Value::Uint8(id) => Some(SubscriptionId(id)),
			other => {
				error!("subscription_id column has wrong type: {:?}", other);
				None
			}
		})
		.ok_or(CreateSubscriptionError::ExtractionFailed)?;

	let enabled = first_bool_value(frame, "hydration_enabled").unwrap_or(true);
	let max_rows = first_uint8_value(frame, "hydration_max_rows");
	let throttle =
		first_uint8_value(frame, "throttle_ms").map(|ms| Duration::from_milliseconds(ms as i64).unwrap());
	let linger = first_uint8_value(frame, "linger_ms").map(|ms| Duration::from_milliseconds(ms as i64).unwrap());

	Ok(CreateSubscriptionResult::Local {
		id,
		hydration: HydrationConfig {
			enabled,
			max_rows,
		},
		throttle,
		linger,
	})
}

#[cfg(not(reifydb_single_threaded))]
#[inline]
fn first_utf8_value(col: &FrameColumn) -> Option<String> {
	if col.data.is_empty() {
		return None;
	}
	match col.data.get_value(0) {
		Value::Utf8(s) => Some(s),
		_ => None,
	}
}

#[cfg(not(reifydb_single_threaded))]
#[inline]
fn first_bool_value(frame: &Frame, name: &str) -> Option<bool> {
	let col = frame.columns.iter().find(|c| c.name == name)?;
	if col.data.is_empty() {
		return None;
	}
	match col.data.get_value(0) {
		Value::Boolean(b) => Some(b),
		_ => None,
	}
}

#[cfg(not(reifydb_single_threaded))]
#[inline]
fn first_uint8_value(frame: &Frame, name: &str) -> Option<u64> {
	let col = frame.columns.iter().find(|c| c.name == name)?;
	if col.data.is_empty() {
		return None;
	}
	match col.data.get_value(0) {
		Value::Uint8(n) => Some(n),
		_ => None,
	}
}
