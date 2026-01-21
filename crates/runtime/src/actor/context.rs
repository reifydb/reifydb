// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Actor execution context.
//!
//! The context provides actors with access to:
//! - Self reference for receiving messages
//! - Runtime for spawning child actors
//! - Cancellation status for graceful shutdown
//! - Timer scheduling (when enabled)

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::actor::mailbox::ActorRef;
use crate::actor::runtime::ActorRuntime;

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
/// - Runtime (to spawn child actors)
/// - Cancellation (for graceful shutdown)
pub struct Context<M> {
	self_ref: ActorRef<M>,
	runtime: ActorRuntime,
	cancel: CancellationToken,
}

impl<M: Send + 'static> Context<M> {
	/// Create a new context.
	pub(crate) fn new(self_ref: ActorRef<M>, runtime: ActorRuntime, cancel: CancellationToken) -> Self {
		Self {
			self_ref,
			runtime,
			cancel,
		}
	}

	/// Get a reference to send messages to self.
	pub fn self_ref(&self) -> ActorRef<M> {
		self.self_ref.clone()
	}

	/// Get the runtime (for spawning child actors).
	pub fn runtime(&self) -> &ActorRuntime {
		&self.runtime
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

impl<M> Clone for Context<M> {
	fn clone(&self) -> Self {
		Self {
			self_ref: self.self_ref.clone(),
			runtime: self.runtime.clone(),
			cancel: self.cancel.clone(),
		}
	}
}
