// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native actor runtime implementation.
//!
//! Spawns each actor on its own OS thread.

use super::ActorRuntime;
use crate::actor::{
	context::Context,
	mailbox::{ActorRef, create_mailbox},
	runner::ActorRunner,
	traits::Actor,
};

impl ActorRuntime {
	/// Spawn an actor on its own OS thread.
	///
	/// Returns a handle with the ActorRef and join capability.
	pub(super) fn spawn_inner<A: Actor>(&self, name: &str, actor: A) -> ActorHandleInner<A::Message> {
		let config = actor.config();
		let (actor_ref, mailbox) = create_mailbox(config.mailbox_capacity);

		let ctx = Context::new(actor_ref.clone(), self.clone(), self.inner.cancel.clone());

		let runner = ActorRunner::new(actor, mailbox, ctx);

		let thread_name = name.to_string();
		let handle = std::thread::Builder::new()
			.name(thread_name.clone())
			.spawn(move || {
				tracing::debug!(actor = %thread_name, "Actor thread starting");
				runner.run();
				tracing::debug!(actor = %thread_name, "Actor thread stopped");
			})
			.expect("Failed to spawn actor thread");

		ActorHandleInner {
			actor_ref,
			join_handle: Some(handle),
		}
	}
}

/// Native handle to a spawned actor.
pub struct ActorHandleInner<M> {
	/// Reference to send messages to the actor.
	pub actor_ref: ActorRef<M>,
	pub(super) join_handle: Option<std::thread::JoinHandle<()>>,
}

impl<M> ActorHandleInner<M> {
	/// Get the actor reference.
	pub fn actor_ref(&self) -> &ActorRef<M> {
		&self.actor_ref
	}

	/// Wait for the actor to complete (joins the thread).
	pub fn join(&mut self) -> Result<(), JoinErrorInner> {
		if let Some(handle) = self.join_handle.take() {
			handle.join().map_err(|e| JoinErrorInner::Panicked(format!("{:?}", e)))
		} else {
			Ok(())
		}
	}
}

/// Native error returned when joining an actor fails.
#[derive(Debug)]
pub enum JoinErrorInner {
	/// The actor thread panicked.
	Panicked(String),
}

impl std::fmt::Display for JoinErrorInner {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			JoinErrorInner::Panicked(msg) => write!(f, "actor thread panicked: {}", msg),
		}
	}
}

impl std::error::Error for JoinErrorInner {}
