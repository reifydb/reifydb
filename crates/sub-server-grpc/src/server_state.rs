// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Server state wrapper combining shared AppState with per-request actor spawning.

use std::ops::Deref;

use reifydb_core::actors::server::ServerMessage;
use reifydb_runtime::actor::{mailbox::ActorRef, system::ActorHandle};
use reifydb_sub_server::state::AppState;

/// gRPC server state wrapping the shared `AppState`.
///
/// Spawns a fresh actor per request to avoid serializing all gRPC requests
/// through a single actor mailbox.
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

	/// Spawn a short-lived actor for one request and return its ref + handle.
	///
	/// The caller must keep the `ActorHandle` alive until the reply is received;
	/// dropping it shuts down the actor.
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
