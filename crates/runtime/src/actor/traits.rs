// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Core actor trait and associated types.
//!
//! This module defines the fundamental abstractions for the actor model:
//! - [`Actor`]: The trait that all actors must implement
//! - [`Flow`]: Control flow for actor scheduling
//! - [`ActorConfig`]: Configuration for actor behavior (re-exported from system)

use crate::actor::context::Context;

// Re-export config types from system module
pub use crate::actor::system::config::ActorConfig;

/// What the actor wants to do after handling a message.
///
/// This enum controls actor behavior after processing each message.
///
/// - **Native**: `Yield` and `Park` are no-ops since actors have their own threads
/// - **WASM**: Messages are processed inline (synchronously), so `Yield` and `Park` are no-ops
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flow {
	/// Keep processing messages immediately.
	Continue,

	/// Yield to other actors (no-op in current implementation).
	Yield,

	/// Block waiting for message (no-op in current implementation).
	Park,

	/// Stop this actor permanently.
	///
	/// The actor's `post_stop` hook will be called, and the actor
	/// will be removed from the system.
	Stop,
}

/// The core actor abstraction.
///
/// Actors are isolated units of computation that:
/// - Own their state exclusively (no shared mutable state)
/// - Process messages one at a time (no internal concurrency)
/// - Communicate with other actors only via message passing
/// - Yield cooperatively to allow fair scheduling
///
/// # Lifecycle
///
/// 1. `init()` - Create initial state
/// 2. `pre_start()` - Called before processing begins
/// 3. Loop: `handle()` messages, `idle()` when empty
/// 4. `post_stop()` - Cleanup after actor stops
///
/// # Example
///
/// ```ignore
/// struct Counter {
///     name: String,
/// }
///
/// enum CounterMsg {
///     Increment,
///     Decrement,
///     Get { reply: oneshot::Sender<i64> },
/// }
///
/// impl Actor for Counter {
///     type State = i64;
///     type Message = CounterMsg;
///
///     fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
///         0
///     }
///
///     fn handle(
///         &self,
///         state: &mut Self::State,
///         msg: Self::Message,
///         _ctx: &Context<Self::Message>,
///     ) -> Flow {
///         match msg {
///             CounterMsg::Increment => *state += 1,
///             CounterMsg::Decrement => *state -= 1,
///             CounterMsg::Get { reply } => { let _ = reply.send(*state); }
///         }
///         Flow::Continue
///     }
/// }
/// ```
pub trait Actor: Send + 'static {
	/// The actor's internal state (owned, not shared).
	type State: Send + 'static;

	/// Messages this actor can receive.
	type Message: Send + 'static;

	/// Create initial state. Called on start and restart.
	fn init(&self, ctx: &Context<Self::Message>) -> Self::State;

	/// Handle a single message. This is the core of the actor.
	///
	/// Return `Flow` to control scheduling:
	/// - `Continue`: Process next message immediately
	/// - `Yield`: Give other actors a chance to run
	/// - `Park`: Sleep until a message arrives
	/// - `Stop`: Terminate this actor
	fn handle(&self, state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Flow;

	/// Called when the mailbox is empty.
	///
	/// Use for:
	/// - Background/periodic work
	/// - Polling external state
	/// - Cleanup tasks
	///
	/// Default: Park (sleep until message arrives)
	#[allow(unused_variables)]
	fn idle(&self, state: &mut Self::State, ctx: &Context<Self::Message>) -> Flow {
		Flow::Park
	}

	/// Called once before message processing begins.
	#[allow(unused_variables)]
	fn pre_start(&self, state: &mut Self::State, ctx: &Context<Self::Message>) {}

	/// Called once after actor stops (always called, even on panic).
	#[allow(unused_variables)]
	fn post_stop(&self, state: &mut Self::State) {}

	/// Actor configuration. Override for custom settings.
	fn config(&self) -> ActorConfig {
		ActorConfig::default()
	}
}
