// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::time::Duration;

use reifydb_core::actors::admin::AdminMessage;
use reifydb_engine::engine::StandardEngine;
use reifydb_runtime::{
	actor::{
		mailbox::ActorRef,
		system::{ActorHandle, ActorSpawner},
	},
	context::clock::Clock,
};

use crate::actor::AdminServerActor;

#[derive(Clone)]
pub struct AdminState {
	engine: StandardEngine,
	max_connections: usize,
	request_timeout: Duration,
	auth_required: bool,
	auth_token: Option<String>,
	clock: Clock,
	spawner: ActorSpawner,
}

impl AdminState {
	pub fn new(
		engine: StandardEngine,
		max_connections: usize,
		request_timeout: Duration,
		auth_required: bool,
		auth_token: Option<String>,
		clock: Clock,
		spawner: ActorSpawner,
	) -> Self {
		Self {
			engine,
			max_connections,
			request_timeout,
			auth_required,
			auth_token,
			clock,
			spawner,
		}
	}

	pub fn spawn_actor(&self) -> (ActorRef<AdminMessage>, ActorHandle<AdminMessage>) {
		let actor = AdminServerActor::new(
			self.engine.clone(),
			self.auth_required,
			self.auth_token.clone(),
			self.clock.clone(),
		);
		let handle = self.spawner.spawn_query("admin-req", actor);
		let actor_ref = handle.actor_ref().clone();
		(actor_ref, handle)
	}

	#[inline]
	pub fn engine(&self) -> &StandardEngine {
		&self.engine
	}

	#[inline]
	pub fn engine_clone(&self) -> StandardEngine {
		self.engine.clone()
	}

	#[inline]
	pub fn max_connections(&self) -> usize {
		self.max_connections
	}

	#[inline]
	pub fn request_timeout(&self) -> Duration {
		self.request_timeout
	}

	#[inline]
	pub fn auth_required(&self) -> bool {
		self.auth_required
	}

	#[inline]
	pub fn auth_token(&self) -> Option<&str> {
		self.auth_token.as_deref()
	}
}
