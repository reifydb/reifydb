// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	Database, Error, Frame, IdentityId, Params,
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
	pub fn new(db: &Database) -> reifydb::Result<Self> {
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
		rql: String,
		params: Params,
	) -> Result<Vec<Frame>, Self::Error> {
		// `rql` is the whole `CREATE SUBSCRIPTION ... AS { .. }` statement, as it is on the
		// socket: the client builds it, and the subscribe endpoint rejects anything else. Wrapping
		// a bare body here instead would accept statements the server refuses.
		let result = self.engine().subscribe_as(identity, &rql, params);
		match result.error {
			Some(e) => Err(e),
			None => Ok(result.frames),
		}
	}
}
