// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Testing utilities for actors.
//!
//! This module provides a [`TestHarness`] for synchronous actor testing
//! without spawning actual tasks or threads.
//!
//! # Example
//!
//! ```ignore
//! #[test]
//! fn test_counter() {
//!     struct Counter;
//!
//!     impl Actor for Counter {
//!         type State = i64;
//!         type Message = i64;
//!
//!         fn init(&self, _ctx: &Context<Self::Message>) -> Self::State { 0 }
//!
//!         fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Flow {
//!             *state += msg;
//!             Flow::Continue
//!         }
//!     }
//!
//!     let mut harness = TestHarness::new(Counter);
//!     harness.send(5);
//!     harness.send(3);
//!     harness.process_all();
//!
//!     assert_eq!(*harness.state(), 8);
//! }
//! ```

use std::collections::VecDeque;

use crate::actor::{
	context::{CancellationToken, Context},
	mailbox::ActorRef,
	runtime::ActorRuntime,
	traits::{Actor, Flow},
};

/// Test harness for synchronous actor testing.
///
/// This harness allows testing actors without spawning tasks:
/// - Messages are queued in a local VecDeque
/// - Processing is explicit via `process_one()` or `process_all()`
/// - State is directly accessible for assertions
pub struct TestHarness<A: Actor> {
	actor: A,
	state: A::State,
	mailbox: VecDeque<A::Message>,
	ctx: TestContext<A::Message>,
}

impl<A: Actor> TestHarness<A> {
	/// Create a new test harness for the given actor.
	pub fn new(actor: A) -> Self {
		let ctx = TestContext::new();
		let state = actor.init(&ctx.to_context());

		Self {
			actor,
			state,
			mailbox: VecDeque::new(),
			ctx,
		}
	}

	/// Create a new test harness with a pre-initialized state.
	///
	/// This is useful when you want to test specific state transitions
	/// without going through the init process.
	pub fn with_state(actor: A, state: A::State) -> Self {
		let ctx = TestContext::new();

		Self {
			actor,
			state,
			mailbox: VecDeque::new(),
			ctx,
		}
	}

	/// Send a message to the actor's mailbox.
	///
	/// The message will be queued and processed when `process_one()`
	/// or `process_all()` is called.
	pub fn send(&mut self, msg: A::Message) {
		self.mailbox.push_back(msg);
	}

	/// Process a single message from the mailbox.
	///
	/// Returns `Some(flow)` if a message was processed,
	/// or `None` if the mailbox was empty.
	pub fn process_one(&mut self) -> Option<Flow> {
		let msg = self.mailbox.pop_front()?;
		let flow = self.actor.handle(&mut self.state, msg, &self.ctx.to_context());
		Some(flow)
	}

	/// Process all messages in the mailbox.
	///
	/// Returns a Vec of all Flow values returned by handle().
	/// Processing stops early if any handler returns `Flow::Stop`.
	pub fn process_all(&mut self) -> Vec<Flow> {
		let mut flows = Vec::new();

		while let Some(flow) = self.process_one() {
			flows.push(flow);
			if flow == Flow::Stop {
				break;
			}
		}

		flows
	}

	/// Process messages until the mailbox is empty or a condition is met.
	///
	/// Returns the flows from all processed messages.
	pub fn process_until<F>(&mut self, mut condition: F) -> Vec<Flow>
	where
		F: FnMut(&A::State) -> bool,
	{
		let mut flows = Vec::new();

		while !self.mailbox.is_empty() {
			if condition(&self.state) {
				break;
			}

			if let Some(flow) = self.process_one() {
				flows.push(flow);
				if flow == Flow::Stop {
					break;
				}
			}
		}

		flows
	}

	/// Call the actor's idle hook.
	///
	/// This is useful for testing background work behavior.
	pub fn idle(&mut self) -> Flow {
		self.actor.idle(&mut self.state, &self.ctx.to_context())
	}

	/// Call the actor's pre_start hook.
	pub fn pre_start(&mut self) {
		self.actor.pre_start(&mut self.state, &self.ctx.to_context());
	}

	/// Call the actor's post_stop hook.
	pub fn post_stop(&mut self) {
		self.actor.post_stop(&mut self.state);
	}

	/// Get a reference to the actor's state.
	pub fn state(&self) -> &A::State {
		&self.state
	}

	/// Get a mutable reference to the actor's state.
	pub fn state_mut(&mut self) -> &mut A::State {
		&mut self.state
	}

	/// Check if the mailbox is empty.
	pub fn is_empty(&self) -> bool {
		self.mailbox.is_empty()
	}

	/// Get the number of messages in the mailbox.
	pub fn mailbox_len(&self) -> usize {
		self.mailbox.len()
	}

	/// Signal cancellation.
	pub fn cancel(&mut self) {
		self.ctx.cancel();
	}

	/// Check if cancelled.
	pub fn is_cancelled(&self) -> bool {
		self.ctx.is_cancelled()
	}
}

/// Test context that doesn't require a real runtime.
struct TestContext<M> {
	cancel: CancellationToken,
	_marker: std::marker::PhantomData<M>,
}

impl<M: Send + 'static> TestContext<M> {
	fn new() -> Self {
		Self {
			cancel: CancellationToken::new(),
			_marker: std::marker::PhantomData,
		}
	}

	fn cancel(&self) {
		self.cancel.cancel();
	}

	fn is_cancelled(&self) -> bool {
		self.cancel.is_cancelled()
	}

	/// Convert to a Context.
	///
	/// Note: The ActorRef in this context is not usable for sending
	/// messages in tests. Use `harness.send()` instead.
	fn to_context(&self) -> Context<M> {
		// Create a dummy actor ref using platform-specific implementation
		#[cfg(reifydb_target = "native")]
		let actor_ref = {
			let (tx, _rx) = crossbeam_channel::unbounded();
			ActorRef::new(tx)
		};

		#[cfg(reifydb_target = "wasm")]
		let actor_ref = crate::actor::mailbox::create_actor_ref();

		// Create a runtime (no tokio handle needed anymore)
		let runtime = ActorRuntime::new();

		Context::new(actor_ref, runtime, self.cancel.clone())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	struct CounterActor;

	impl Actor for CounterActor {
		type State = i64;
		type Message = CounterMsg;

		fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
			0
		}

		fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Flow {
			match msg {
				CounterMsg::Inc => *state += 1,
				CounterMsg::Dec => *state -= 1,
				CounterMsg::Set(v) => *state = v,
				CounterMsg::Stop => return Flow::Stop,
			}
			Flow::Continue
		}

		fn idle(&self, _state: &mut Self::State, _ctx: &Context<Self::Message>) -> Flow {
			Flow::Park
		}
	}

	#[derive(Debug)]
	enum CounterMsg {
		Inc,
		Dec,
		Set(i64),
		Stop,
	}

	#[test]
	fn test_harness_basic() {
		let mut harness = TestHarness::new(CounterActor);

		harness.send(CounterMsg::Inc);
		harness.send(CounterMsg::Inc);
		harness.send(CounterMsg::Inc);

		assert_eq!(harness.mailbox_len(), 3);

		let flows = harness.process_all();

		assert_eq!(flows.len(), 3);
		assert!(flows.iter().all(|f| *f == Flow::Continue));
		assert_eq!(*harness.state(), 3);
	}

	#[test]
	fn test_harness_stops_on_stop() {
		let mut harness = TestHarness::new(CounterActor);

		harness.send(CounterMsg::Inc);
		harness.send(CounterMsg::Stop);
		harness.send(CounterMsg::Inc); // Should not be processed

		let flows = harness.process_all();

		assert_eq!(flows.len(), 2);
		assert_eq!(flows[1], Flow::Stop);
		assert_eq!(*harness.state(), 1);
		assert_eq!(harness.mailbox_len(), 1); // One message left
	}

	#[test]
	fn test_harness_process_one() {
		let mut harness = TestHarness::new(CounterActor);

		harness.send(CounterMsg::Set(42));
		harness.send(CounterMsg::Inc);

		assert_eq!(harness.process_one(), Some(Flow::Continue));
		assert_eq!(*harness.state(), 42);

		assert_eq!(harness.process_one(), Some(Flow::Continue));
		assert_eq!(*harness.state(), 43);

		assert_eq!(harness.process_one(), None);
	}

	#[test]
	fn test_harness_idle() {
		let mut harness = TestHarness::new(CounterActor);

		let flow = harness.idle();
		assert_eq!(flow, Flow::Park);
	}
}
