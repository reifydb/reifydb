// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Actor execution context.
//!
//! The context provides actors with access to:
//! - Self reference for receiving messages
//! - Actor system for spawning child actors
//! - Cancellation status for graceful shutdown
//! - Timer scheduling (when enabled)

use std::sync::{
	Arc,
	atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use crate::actor::mailbox::ActorRef;
use crate::actor::timers::TimerHandle;
use crate::actor::system::ActorSystem;

/// A cancellation token for signaling shutdown.
///
/// This is a simple atomic boolean that can be shared across actors.
#[derive(Clone)]
pub struct CancellationToken {
	cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
	/// Create a new cancellation token.
	pub fn new() -> Self {
		Self {
			cancelled: Arc::new(AtomicBool::new(false)),
		}
	}

	/// Signal cancellation.
	pub fn cancel(&self) {
		self.cancelled.store(true, Ordering::SeqCst);
	}

	/// Check if cancellation was requested.
	pub fn is_cancelled(&self) -> bool {
		self.cancelled.load(Ordering::SeqCst)
	}
}

impl Default for CancellationToken {
	fn default() -> Self {
		Self::new()
	}
}

/// Context provided to actors during execution.
///
/// Provides access to:
/// - Self reference (to give to other actors)
/// - Actor system (to spawn child actors and run compute)
/// - Cancellation (for graceful shutdown)
pub struct Context<M> {
	self_ref: ActorRef<M>,
	system: ActorSystem,
	cancel: CancellationToken,
}

impl<M: Send + 'static> Context<M> {
	/// Create a new context.
	pub(crate) fn new(self_ref: ActorRef<M>, system: ActorSystem, cancel: CancellationToken) -> Self {
		Self {
			self_ref,
			system,
			cancel,
		}
	}

	/// Get a reference to send messages to self.
	pub fn self_ref(&self) -> ActorRef<M> {
		self.self_ref.clone()
	}

	/// Get the actor system (for spawning child actors).
	pub fn system(&self) -> &ActorSystem {
		&self.system
	}

	/// Check if shutdown was requested.
	pub fn is_cancelled(&self) -> bool {
		self.cancel.is_cancelled()
	}

	/// Get the cancellation token.
	pub fn cancellation_token(&self) -> CancellationToken {
		self.cancel.clone()
	}
}

impl<M: Send + Clone + 'static> Context<M> {
	/// Schedule a message to be sent to this actor after a delay.
	///
	/// Returns a handle that can be used to cancel the timer.
	#[cfg(reifydb_target = "native")]
	pub fn schedule_once(&self, delay: Duration, msg: M) -> TimerHandle {
		let actor_ref = self.self_ref.clone();
		self.system.scheduler().schedule_once(delay, move || {
			let _ = actor_ref.send(msg);
		})
	}

	/// Schedule a message to be sent to this actor after a delay.
	///
	/// Returns a handle that can be used to cancel the timer.
	#[cfg(reifydb_target = "wasm")]
	pub fn schedule_once(&self, delay: Duration, msg: M) -> TimerHandle {
		crate::actor::timers::wasm::schedule_once(self.self_ref.clone(), delay, msg)
	}
}

impl<M: Send + Sync + Clone + 'static> Context<M> {
	/// Schedule a message to be sent to this actor repeatedly at an interval.
	///
	/// The timer continues until cancelled or the actor is dropped.
	/// Returns a handle that can be used to cancel the timer.
	#[cfg(reifydb_target = "native")]
	pub fn schedule_repeat(&self, interval: Duration, msg: M) -> TimerHandle {
		let actor_ref = self.self_ref.clone();
		self.system.scheduler().schedule_repeat(interval, move || {
			actor_ref.send(msg.clone()).is_ok()
		})
	}

	/// Schedule a message to be sent to this actor repeatedly at an interval.
	///
	/// The timer continues until cancelled or the actor is dropped.
	/// Returns a handle that can be used to cancel the timer.
	#[cfg(reifydb_target = "wasm")]
	pub fn schedule_repeat(&self, interval: Duration, msg: M) -> TimerHandle {
		crate::actor::timers::wasm::schedule_repeat(self.self_ref.clone(), interval, msg)
	}
}

impl<M> Clone for Context<M> {
	fn clone(&self) -> Self {
		Self {
			self_ref: self.self_ref.clone(),
			system: self.system.clone(),
			cancel: self.cancel.clone(),
		}
	}
}
