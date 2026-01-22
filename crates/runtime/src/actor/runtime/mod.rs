// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Actor runtime for spawning and managing actors.
//!
//! The runtime abstracts over the underlying execution model:
//! - **Native**: Each actor runs on its own OS thread (using `std::thread::spawn`)
//! - **WASM**: Messages are processed inline (synchronously) when sent

use std::sync::Arc;

use crate::actor::{context::CancellationToken, mailbox::ActorRef, traits::Actor};

#[cfg(reifydb_target = "native")]
pub(crate) mod native;
#[cfg(reifydb_target = "wasm")]
pub(crate) mod wasm;

// =============================================================================
// ActorRuntime (shared)
// =============================================================================

/// Runtime for spawning and managing actors.
///
/// The runtime abstracts over the underlying execution model:
/// - Native: spawns OS threads (one per actor)
/// - WASM: sets up inline message processing
#[derive(Clone)]
pub struct ActorRuntime {
	inner: Arc<ActorRuntimeInner>,
}

struct ActorRuntimeInner {
	cancel: CancellationToken,
}

impl ActorRuntime {
	pub fn new() -> Self {
		Self {
			inner: Arc::new(ActorRuntimeInner {
				cancel: CancellationToken::new(),
			}),
		}
	}

	pub fn shutdown(&self) {
		self.inner.cancel.cancel();
	}

	pub fn is_shutdown(&self) -> bool {
		self.inner.cancel.is_cancelled()
	}

	pub fn cancellation_token(&self) -> CancellationToken {
		self.inner.cancel.clone()
	}
}

impl Default for ActorRuntime {
	fn default() -> Self {
		Self::new()
	}
}

// Spawn methods are implemented in native.rs and wasm.rs
impl ActorRuntime {
	/// Spawn an actor.
	///
	/// Returns a handle with the ActorRef and join capability.
	#[cfg(reifydb_target = "native")]
	pub fn spawn<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message> {
		let inner = self.spawn_inner(name, actor);
		ActorHandle {
			actor_ref: inner.actor_ref,
			join_handle: inner.join_handle,
		}
	}

	/// Spawn an actor.
	///
	/// Returns a handle with the ActorRef and join capability.
	#[cfg(reifydb_target = "wasm")]
	pub fn spawn<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message> {
		let inner = self.spawn_inner(name, actor);
		ActorHandle {
			actor_ref: inner.actor_ref,
		}
	}

	/// Spawn and return just the ActorRef (fire-and-forget).
	pub fn spawn_ref<A: Actor>(&self, name: &str, actor: A) -> ActorRef<A::Message> {
		self.spawn_inner(name, actor).actor_ref
	}
}

// =============================================================================
// ActorHandle (wrapper)
// =============================================================================

/// Handle to a spawned actor.
pub struct ActorHandle<M> {
	/// Reference to send messages to the actor.
	pub actor_ref: ActorRef<M>,
	#[cfg(reifydb_target = "native")]
	join_handle: Option<std::thread::JoinHandle<()>>,
}

impl<M> ActorHandle<M> {
	/// Wait for the actor to complete.
	#[cfg(reifydb_target = "native")]
	pub fn join(mut self) -> Result<(), JoinError> {
		if let Some(handle) = self.join_handle.take() {
			handle.join().map_err(|e| JoinError::new(format!("{:?}", e)))
		} else {
			Ok(())
		}
	}

	/// Wait for the actor to complete.
	///
	/// In WASM, this is a no-op since messages are processed inline.
	#[cfg(reifydb_target = "wasm")]
	pub fn join(self) -> Result<(), JoinError> {
		Ok(())
	}
}

// =============================================================================
// JoinError (shared)
// =============================================================================

/// Error returned when joining an actor fails.
#[derive(Debug)]
pub struct JoinError {
	message: String,
}

impl JoinError {
	/// Create a new JoinError with a message.
	pub fn new(message: impl Into<String>) -> Self {
		Self {
			message: message.into(),
		}
	}
}

impl std::fmt::Display for JoinError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "actor thread panicked: {}", self.message)
	}
}

impl std::error::Error for JoinError {}
