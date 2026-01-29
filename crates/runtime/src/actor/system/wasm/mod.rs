// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM actor system implementation.
//!
//! All operations execute inline (synchronously) since WASM doesn't support threads.

use std::{
	cell::{Cell, RefCell},
	rc::Rc,
	sync::{Arc, atomic::AtomicBool},
};

use crate::actor::{
	context::{CancellationToken, Context},
	mailbox::ActorRef,
	traits::{Actor, Flow},
};

/// Configuration for the actor system (mostly ignored in WASM).
#[derive(Debug, Clone, Default)]
pub struct ActorSystemConfig {
	/// Number of worker threads (ignored in WASM).
	pub pool_threads: usize,
	/// Maximum concurrent compute tasks (ignored in WASM).
	pub max_in_flight: usize,
}

impl ActorSystemConfig {
	/// Set the number of pool threads (ignored in WASM).
	pub fn pool_threads(mut self, threads: usize) -> Self {
		self.pool_threads = threads;
		self
	}

	/// Set the maximum number of in-flight compute tasks (ignored in WASM).
	pub fn max_in_flight(mut self, max: usize) -> Self {
		self.max_in_flight = max;
		self
	}
}

/// Inner shared state for the actor system.
struct ActorSystemInner {
	cancel: CancellationToken,
}

/// Unified system for all concurrent work (WASM version).
///
/// In WASM, all operations execute inline (synchronously).
/// Threading model configuration is ignored.
#[derive(Clone)]
pub struct ActorSystem {
	inner: Arc<ActorSystemInner>,
}

impl ActorSystem {
	/// Create a new actor system.
	///
	/// Configuration parameters are ignored in WASM.
	pub fn new(_config: ActorSystemConfig) -> Self {
		Self {
			inner: Arc::new(ActorSystemInner {
				cancel: CancellationToken::new(),
			}),
		}
	}

	/// Get the cancellation token for this system.
	pub fn cancellation_token(&self) -> CancellationToken {
		self.inner.cancel.clone()
	}

	/// Check if the system has been cancelled.
	pub fn is_cancelled(&self) -> bool {
		self.inner.cancel.is_cancelled()
	}

	/// Signal shutdown to all actors.
	pub fn shutdown(&self) {
		self.inner.cancel.cancel();
	}

	/// Spawn an actor (processes messages inline in WASM).
	///
	/// Threading model is ignored - all actors process messages synchronously.
	pub fn spawn<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message> {
		self.spawn_inner(name, actor)
	}

	/// Spawn an actor that requires a dedicated thread (same as spawn in WASM).
	pub fn spawn_dedicated<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message> {
		self.spawn_inner(name, actor)
	}

	/// Internal spawn implementation.
	fn spawn_inner<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message> {
		let actor_ref = create_actor_ref::<A::Message>();

		// Wrap actor and state in Rc for sharing between processor and eager init
		let actor = Rc::new(actor);
		let actor_for_processor = actor.clone();

		let state: Rc<RefCell<Option<A::State>>> = Rc::new(RefCell::new(None));
		let state_for_processor = state.clone();

		let ctx = Context::new(actor_ref.clone(), self.clone(), self.cancellation_token());
		let ctx_for_init = ctx.clone();
		let ctx_for_processor = ctx.clone();

		let _name = name.to_string();
		let _name_for_drain = _name.clone();
		let actor_ref_for_closure = actor_ref.clone();
		let actor_ref_for_drain = actor_ref.clone();
		let cancel = self.cancellation_token();

		// Queue for messages sent during initialization
		// Some(vec) = initializing (queue messages), None = ready (process normally)
		let init_queue: Rc<RefCell<Option<Vec<A::Message>>>> = Rc::new(RefCell::new(Some(Vec::new())));
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

		ActorHandle {
			actor_ref,
		}
	}

	/// Executes a closure immediately (sequential execution).
	///
	/// In WASM, there's no thread pool, so this executes synchronously.
	pub fn install<R, F>(&self, f: F) -> R
	where
		R: Send,
		F: FnOnce() -> R + Send,
	{
		f()
	}

	/// Runs a CPU-bound function immediately (sequential execution).
	///
	/// In WASM, there's no thread pool or admission control, so this
	/// executes synchronously and returns immediately.
	pub async fn compute<R, F>(&self, f: F) -> Result<R, WasmJoinError>
	where
		R: Send + 'static,
		F: FnOnce() -> R + Send + 'static,
	{
		Ok(f())
	}
}

impl std::fmt::Debug for ActorSystem {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("ActorSystem").field("cancelled", &self.is_cancelled()).finish_non_exhaustive()
	}
}

/// Handle to a spawned actor.
pub struct ActorHandle<M> {
	pub actor_ref: ActorRef<M>,
}

impl<M> ActorHandle<M> {
	/// Get the actor reference for sending messages.
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
		write!(f, "actor join failed: {}", self.message)
	}
}

impl std::error::Error for JoinError {}

/// WASM join error for compute operations.
#[derive(Debug)]
pub struct WasmJoinError;

impl std::fmt::Display for WasmJoinError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "WASM task failed")
	}
}

impl std::error::Error for WasmJoinError {}

// =============================================================================
// WASM ActorRef internals
// =============================================================================

/// WASM implementation of ActorRef inner.
struct ActorRefInner<M> {
	processor: Rc<RefCell<Option<Box<dyn FnMut(M)>>>>,
	alive: Arc<AtomicBool>,
	queue: Rc<RefCell<Vec<M>>>,
	processing: Rc<Cell<bool>>,
}

impl<M> Clone for ActorRefInner<M> {
	fn clone(&self) -> Self {
		Self {
			processor: self.processor.clone(),
			alive: self.alive.clone(),
			queue: self.queue.clone(),
			processing: self.processing.clone(),
		}
	}
}

/// Create an ActorRef for WASM.
fn create_actor_ref<M>() -> ActorRef<M> {
	let inner = ActorRefInner {
		processor: Rc::new(RefCell::new(None)),
		alive: Arc::new(AtomicBool::new(true)),
		queue: Rc::new(RefCell::new(Vec::new())),
		processing: Rc::new(Cell::new(false)),
	};

	ActorRef::from_wasm_inner(inner.processor, inner.alive, inner.queue, inner.processing)
}
