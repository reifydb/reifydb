// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Actor runtime for spawning and managing actors.
//!
//! The runtime abstracts over the underlying execution model:
//! - **Native**: Each actor runs on its own OS thread (using `std::thread::spawn`)
//! - **WASM**: Messages are processed inline (synchronously) when sent

use std::sync::Arc;

use crate::actor::context::{CancellationToken, Context};
use crate::actor::mailbox::ActorRef;
use crate::actor::traits::Actor;

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
	/// Create a new actor runtime.
	///
	/// No tokio handle needed - uses OS threads on native.
	pub fn new() -> Self {
		Self {
			inner: Arc::new(ActorRuntimeInner {
				cancel: CancellationToken::new(),
			}),
		}
	}

	/// Request graceful shutdown of all actors.
	pub fn shutdown(&self) {
		self.inner.cancel.cancel();
	}

	/// Check if shutdown has been requested.
	pub fn is_shutdown(&self) -> bool {
		self.inner.cancel.is_cancelled()
	}

	/// Get the cancellation token.
	pub fn cancellation_token(&self) -> CancellationToken {
		self.inner.cancel.clone()
	}
}

impl Default for ActorRuntime {
	fn default() -> Self {
		Self::new()
	}
}

// =============================================================================
// Native: Thread-per-actor implementation
// =============================================================================

#[cfg(feature = "native")]
mod native {
	use super::*;
	use crate::actor::mailbox::create_mailbox;
	use crate::actor::runner::ActorRunner;

	impl ActorRuntime {
		/// Spawn an actor on its own OS thread.
		///
		/// Returns a handle with the ActorRef and join capability.
		pub fn spawn<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message> {
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

			ActorHandle {
				actor_ref,
				join_handle: Some(handle),
			}
		}

		/// Spawn and return just the ActorRef (fire-and-forget).
		pub fn spawn_ref<A: Actor>(&self, name: &str, actor: A) -> ActorRef<A::Message> {
			self.spawn(name, actor).actor_ref
		}
	}

	/// Handle to a spawned actor.
	pub struct ActorHandle<M> {
		/// Reference to send messages to the actor.
		pub actor_ref: ActorRef<M>,
		join_handle: Option<std::thread::JoinHandle<()>>,
	}

	impl<M> ActorHandle<M> {
		/// Get the actor reference.
		pub fn actor_ref(&self) -> &ActorRef<M> {
			&self.actor_ref
		}

		/// Wait for the actor to complete (joins the thread).
		pub fn join(mut self) -> Result<(), JoinError> {
			if let Some(handle) = self.join_handle.take() {
				handle.join().map_err(|e| JoinError::Panicked(format!("{:?}", e)))
			} else {
				Ok(())
			}
		}
	}

	/// Error returned when joining an actor fails.
	#[derive(Debug)]
	pub enum JoinError {
		/// The actor thread panicked.
		Panicked(String),
	}

	impl std::fmt::Display for JoinError {
		fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			match self {
				JoinError::Panicked(msg) => write!(f, "actor thread panicked: {}", msg),
			}
		}
	}

	impl std::error::Error for JoinError {}
}

// =============================================================================
// WASM: Inline processing implementation
// =============================================================================

#[cfg(feature = "wasm")]
mod wasm {
	use super::*;
	use crate::actor::mailbox::create_actor_ref;
	use crate::actor::traits::Flow;
	use std::cell::RefCell;

	impl ActorRuntime {
		/// Spawn an actor for WASM (sets up inline message processing).
		///
		/// In WASM, messages are processed synchronously when sent,
		/// so no separate thread or task is created.
		pub fn spawn<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message> {
			let actor_ref = create_actor_ref::<A::Message>();

			// Create a shared state cell that will be initialized lazily
			let state: RefCell<Option<A::State>> = RefCell::new(None);
			let ctx = Context::new(actor_ref.clone(), self.clone(), self.inner.cancel.clone());

			let _name = name.to_string();
			let actor_ref_for_closure = actor_ref.clone();
			let cancel = self.inner.cancel.clone();

			// Create the processor that handles messages inline
			let processor = move |msg: A::Message| {
				// Check cancellation
				if cancel.is_cancelled() {
					tracing::debug!(actor = %_name, "Actor cancelled, ignoring message");
					actor_ref_for_closure.mark_stopped();
					return;
				}

				let mut state_ref = state.borrow_mut();

				// Initialize state lazily on first message
				if state_ref.is_none() {
					tracing::debug!(actor = %_name, "Actor initializing");
					let mut initial_state = actor.init(&ctx);
					actor.pre_start(&mut initial_state, &ctx);
					*state_ref = Some(initial_state);
				}

				// Handle the message
				if let Some(ref mut s) = *state_ref {
					match actor.handle(s, msg, &ctx) {
						Flow::Stop => {
							tracing::debug!(actor = %_name, "Actor returned Flow::Stop");
							actor.post_stop(s);
							actor_ref_for_closure.mark_stopped();
						}
						// Continue, Yield, Park are all no-ops in WASM
						Flow::Continue | Flow::Yield | Flow::Park => {}
					}
				}
			};

			// Install the processor
			{
				let mut processor_ref = actor_ref.processor.borrow_mut();
				*processor_ref = Some(Box::new(processor));
			}

			ActorHandle { actor_ref }
		}

		/// Spawn and return just the ActorRef (fire-and-forget).
		pub fn spawn_ref<A: Actor>(&self, name: &str, actor: A) -> ActorRef<A::Message> {
			self.spawn(name, actor).actor_ref
		}
	}

	/// Handle to a spawned actor.
	pub struct ActorHandle<M> {
		/// Reference to send messages to the actor.
		pub actor_ref: ActorRef<M>,
	}

	impl<M> ActorHandle<M> {
		/// Get the actor reference.
		pub fn actor_ref(&self) -> &ActorRef<M> {
			&self.actor_ref
		}

		/// Wait for the actor to complete.
		///
		/// In WASM, this is a no-op since messages are processed inline.
		pub fn join(self) -> Result<(), JoinError> {
			Ok(())
		}
	}

	/// Error returned when joining an actor fails.
	#[derive(Debug)]
	pub enum JoinError {
		/// Placeholder - WASM join never fails.
		Never,
	}

	impl std::fmt::Display for JoinError {
		fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
			write!(f, "actor join failed")
		}
	}

	impl std::error::Error for JoinError {}
}

// Re-export platform-specific types
#[cfg(feature = "native")]
pub use native::{ActorHandle, JoinError};

#[cfg(feature = "wasm")]
pub use wasm::{ActorHandle, JoinError};
