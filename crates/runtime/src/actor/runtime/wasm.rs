// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM actor runtime implementation.
//!
//! Sets up inline message processing for actors (no threads).

use std::cell::RefCell;
use std::rc::Rc;

use crate::actor::context::Context;
use crate::actor::mailbox::{create_actor_ref, ActorRef};
use crate::actor::traits::{Actor, Flow};

use super::ActorRuntime;

impl ActorRuntime {
	/// Spawn an actor for WASM (sets up inline message processing).
	///
	/// In WASM, messages are processed synchronously when sent,
	/// so no separate thread or task is created.
	///
	/// The actor is initialized eagerly (matching native behavior) so that
	/// `pre_start()` is called immediately. This is required for actors like
	/// `PollActor` that rely on `pre_start()` to initiate their work.
	///
	/// Messages sent during `pre_start()` are queued and processed after
	/// initialization completes to avoid RefCell borrow conflicts.
	pub(super) fn spawn_inner<A: Actor>(&self, name: &str, actor: A) -> ActorHandleInner<A::Message> {
		let actor_ref = create_actor_ref::<A::Message>();

		// Wrap actor and state in Rc for sharing between processor and eager init
		let actor = Rc::new(actor);
		let actor_for_processor = actor.clone();

		let state: Rc<RefCell<Option<A::State>>> = Rc::new(RefCell::new(None));
		let state_for_processor = state.clone();

		let ctx = Context::new(actor_ref.clone(), self.clone(), self.inner.cancel.clone());
		let ctx_for_init = ctx.clone();
		let ctx_for_processor = ctx.clone();

		let _name = name.to_string();
		let _name_for_drain = _name.clone();
		let actor_ref_for_closure = actor_ref.clone();
		let actor_ref_for_drain = actor_ref.clone();
		let cancel = self.inner.cancel.clone();

		// Queue for messages sent during initialization
		// Some(vec) = initializing (queue messages), None = ready (process normally)
		let init_queue: Rc<RefCell<Option<Vec<A::Message>>>> =
			Rc::new(RefCell::new(Some(Vec::new())));
		let init_queue_for_processor = init_queue.clone();

		// Create the processor that handles messages inline
		let processor = move |msg: A::Message| {
			// If still initializing, queue the message for later
			{
				let mut queue_ref = init_queue_for_processor.borrow_mut();
				if let Some(ref mut queue) = *queue_ref {
					tracing::debug!(actor = %_name, "Queueing message during initialization");
					queue.push(msg);
					return;
				}
			}

			// Check cancellation
			if cancel.is_cancelled() {
				tracing::debug!(actor = %_name, "Actor cancelled, ignoring message");
				actor_ref_for_closure.mark_stopped();
				return;
			}

			let mut state_ref = state_for_processor.borrow_mut();

			// State should already be initialized from eager init
			if state_ref.is_none() {
				tracing::warn!(actor = %_name, "Actor state unexpectedly not initialized");
				return;
			}

			// Handle the message
			if let Some(ref mut s) = *state_ref {
				match actor_for_processor.handle(s, msg, &ctx_for_processor) {
					Flow::Stop => {
						tracing::debug!(actor = %_name, "Actor returned Flow::Stop");
						actor_for_processor.post_stop(s);
						actor_ref_for_closure.mark_stopped();
					}
					// Continue, Yield, Park are all no-ops in WASM
					Flow::Continue | Flow::Yield | Flow::Park => {}
				}
			}
		};

		// Install the processor FIRST (so pre_start can send messages - they'll be queued)
		{
			let mut processor_ref = actor_ref.processor().borrow_mut();
			*processor_ref = Some(Box::new(processor));
		}

		// EAGERLY initialize actor and call pre_start (matches native behavior)
		// This must happen AFTER processor is installed so messages can be sent
		{
			let mut state_ref = state.borrow_mut();
			let mut initial_state = actor.init(&ctx_for_init);
			actor.pre_start(&mut initial_state, &ctx_for_init);
			*state_ref = Some(initial_state);
		}

		// Mark initialization complete and drain queued messages
		let queued_messages = init_queue.borrow_mut().take().unwrap_or_default();
		if !queued_messages.is_empty() {
			tracing::debug!(
				actor = %_name_for_drain,
				count = queued_messages.len(),
				"Draining queued messages after init"
			);
		}
		for msg in queued_messages {
			let _ = actor_ref_for_drain.send(msg);
		}

		ActorHandleInner { actor_ref }
	}
}

/// WASM handle to a spawned actor.
pub struct ActorHandleInner<M> {
	/// Reference to send messages to the actor.
	pub actor_ref: ActorRef<M>,
}

impl<M> ActorHandleInner<M> {
	/// Get the actor reference.
	pub fn actor_ref(&self) -> &ActorRef<M> {
		&self.actor_ref
	}

	/// Wait for the actor to complete.
	///
	/// In WASM, this is a no-op since messages are processed inline.
	pub fn join(&mut self) -> Result<(), JoinErrorInner> {
		Ok(())
	}
}

/// WASM error returned when joining an actor fails.
#[derive(Debug)]
pub enum JoinErrorInner {
	/// Placeholder - WASM join never fails.
	Never,
}

impl std::fmt::Display for JoinErrorInner {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "actor join failed")
	}
}

impl std::error::Error for JoinErrorInner {}
