// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::ops::Deref;

use reifydb_core::actors::server::ServerMessage;
use reifydb_runtime::actor::{mailbox::ActorRef, system::ActorHandle};
use reifydb_sub_server::state::AppState;

#[derive(Clone)]
pub struct GrpcServerState {
	state: AppState,
}

impl GrpcServerState {
	pub fn new(state: AppState) -> Self {
		Self {
			state,
		}
	}

	pub fn state(&self) -> &AppState {
		&self.state
	}

	pub fn spawn_actor(&self) -> (ActorRef<ServerMessage>, ActorHandle<ServerMessage>) {
		self.state.spawn_server_actor()
	}
}

impl Deref for GrpcServerState {
	type Target = AppState;

	fn deref(&self) -> &Self::Target {
		&self.state
	}
}
