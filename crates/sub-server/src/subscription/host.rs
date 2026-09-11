// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::actors::server::Operation;
use reifydb_sub_core::host::{SubscribeContext, SubscribeHost};
use reifydb_value::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};

use crate::{
	dispatch::dispatch_subscribe,
	execute::ExecuteError,
	interceptor::{RequestContext, RequestMetadata},
	state::AppState,
};

pub struct ServerSubscribeHost<'a> {
	state: &'a AppState,
	metadata: RequestMetadata,
	context: SubscribeContext,
}

impl<'a> ServerSubscribeHost<'a> {
	pub fn new(state: &'a AppState, metadata: RequestMetadata) -> Self {
		let context = SubscribeContext::new(
			state.engine_clone(),
			state.clock().clone(),
			state.rng().clone(),
			state.subscribe_max_hydration_rows(),
			state.subscribe_min_throttle(),
			state.subscribe_min_linger(),
		);
		Self {
			state,
			metadata,
			context,
		}
	}
}

impl SubscribeHost for ServerSubscribeHost<'_> {
	type Error = ExecuteError;

	fn context(&self) -> &SubscribeContext {
		&self.context
	}

	async fn execute_subscribe(
		&self,
		identity: IdentityId,
		rql: String,
		params: Params,
	) -> Result<Vec<Frame>, Self::Error> {
		let ctx = RequestContext {
			identity,
			operation: Operation::Subscribe,
			rql,
			params,
			metadata: self.metadata.clone(),
		};
		let (frames, _metrics) = dispatch_subscribe(self.state, ctx).await?;
		Ok(frames)
	}
}

impl AppState {
	pub fn subscribe_host(&self, metadata: RequestMetadata) -> ServerSubscribeHost<'_> {
		ServerSubscribeHost::new(self, metadata)
	}
}
