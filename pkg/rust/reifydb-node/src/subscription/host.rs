// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	Database, Error, IdentityId, Params, Result as ReifyResult,
	core::interface::catalog::subscription::{SubscribeOptions, SubscribeOutcome},
	engine::engine::StandardEngine,
	runtime::context::rng::Rng,
	sub_core::host::{SubscribeContext, SubscribeHost},
	value::value::duration::Duration,
};

/// Matches the server defaults so a subscription that is refused here would be refused there too.
const MAX_HYDRATION_ROWS: u64 = 10_000;

/// Runs subscribe statements straight against the engine.
///
/// The server host routes the statement through the interceptor chain and the server actor because
/// a request arrived over a socket and carries metadata worth intercepting. Nothing arrives over a
/// socket here, so there is no chain to run and no timeout to enforce: an in-process caller that
/// wants to stop waiting simply stops calling.
pub struct NodeSubscribeHost {
	context: SubscribeContext,
}

impl NodeSubscribeHost {
	pub fn new(db: &Database) -> ReifyResult<Self> {
		let rng = db.engine().ioc().resolve::<Rng>()?;
		Ok(Self {
			context: SubscribeContext::new(
				db.engine().clone(),
				db.clock().clone(),
				rng,
				MAX_HYDRATION_ROWS,
				Duration::from_milliseconds(50).unwrap(),
				Duration::zero(),
			),
		})
	}

	#[inline]
	pub fn engine(&self) -> &StandardEngine {
		self.context.engine()
	}
}

impl SubscribeHost for NodeSubscribeHost {
	type Error = Error;

	fn context(&self) -> &SubscribeContext {
		&self.context
	}

	async fn execute_subscribe(
		&self,
		identity: IdentityId,
		query: String,
		params: Params,
		options: SubscribeOptions,
	) -> Result<SubscribeOutcome, Self::Error> {
		self.engine().subscribe_as(identity, &query, params, options)
	}
}
