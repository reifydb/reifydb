// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{
	id::SubscriptionId,
	subscription::{HydrationConfig, SubscribeOptions, SubscribeOutcome},
};
use reifydb_value::{
	params::Params,
	value::{duration::Duration, identity::IdentityId},
};
use tracing::debug;

use crate::{errors::CreateSubscriptionError, host::SubscribeHost};

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

pub async fn create_subscription<H: SubscribeHost>(
	host: &H,
	identity: IdentityId,
	query: &str,
	params: Params,
	options: SubscribeOptions,
) -> Result<CreateSubscriptionResult, CreateSubscriptionError<H::Error>> {
	debug!("Subscription query: {}", query);

	let outcome = host
		.execute_subscribe(identity, query.to_string(), params, options.clone())
		.await
		.map_err(CreateSubscriptionError::Execute)?;

	Ok(match outcome {
		SubscribeOutcome::Local {
			id,
		} => CreateSubscriptionResult::Local {
			id,
			hydration: options.hydration,
			throttle: options.throttle,
			linger: options.linger,
		},
		SubscribeOutcome::Remote {
			address,
			body,
			token,
		} => CreateSubscriptionResult::Remote {
			address,
			body,
			token,
			hydration: options.hydration,
			throttle: options.throttle,
			linger: options.linger,
		},
	})
}
