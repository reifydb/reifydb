// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb_core::actors::admin::AdminMessage;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		mailbox::ActorRef,
		system::{ActorHandle, ActorSystem},
	},
	context::clock::Clock,
};

use crate::actor::AdminServerActor;

/// Shared application state for admin handler.
///
/// This struct is cloneable and cheap to clone since `StandardEngine` uses
/// `Arc` internally.
#[derive(Clone)]
pub struct AdminState {
	engine: StandardEngine,
	max_connections: usize,
	request_timeout: Duration,
	auth_required: bool,
	auth_token: Option<String>,
	clock: Clock,
	actor_system: ActorSystem,
}

impl AdminState {
	/// Create a new AdminState.
	pub fn new(
		engine: StandardEngine,
		max_connections: usize,
		request_timeout: Duration,
		auth_required: bool,
		auth_token: Option<String>,
		clock: Clock,
		actor_system: ActorSystem,
	) -> Self {
		Self {
			engine,
			max_connections,
			request_timeout,
			auth_required,
			auth_token,
			clock,
			actor_system,
		}
	}

	/// Spawn a short-lived actor for one request and return its ref + handle.
	///
	/// The caller must keep the `ActorHandle` alive until the reply is received;
	/// dropping it shuts down the actor.
	pub fn spawn_actor(&self) -> (ActorRef<AdminMessage>, ActorHandle<AdminMessage>) {
		let actor = AdminServerActor::new(
			self.engine.clone(),
			self.auth_required,
			self.auth_token.clone(),
			self.clock.clone(),
		);
		let handle = self.actor_system.spawn_query("admin-req", actor);
		let actor_ref = handle.actor_ref().clone();
		(actor_ref, handle)
	}

	/// Get a reference to the database engine.
	#[inline]
	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	/// Get a clone of the database engine.
	#[inline]
	pub fn engine_clone(&self) -> StandardEngine {
		self.engine.clone()
	}

	/// Get the maximum connections.
	#[inline]
	pub fn max_connections(&self) -> usize {
		self.max_connections
	}

	/// Get the request timeout.
	#[inline]
	pub fn request_timeout(&self) -> Duration {
		self.request_timeout
	}

	/// Check if authentication is required.
	#[inline]
	pub fn auth_required(&self) -> bool {
		self.auth_required
	}

	/// Get the auth token (if set).
	#[inline]
	pub fn auth_token(&self) -> Option<&str> {
		self.auth_token.as_deref()
	}
}
