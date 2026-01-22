// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native actor runner implementation.
//!
//! Drives actor execution on a dedicated OS thread with blocking message receives.

use std::time::Duration;

use crate::actor::{
	context::Context,
	mailbox::{Mailbox, RecvTimeoutError},
	traits::{Actor, Flow},
};

/// Interval for checking cancellation during blocked recv.
const SHUTDOWN_CHECK_INTERVAL: Duration = Duration::from_millis(10);

/// Error returned when an actor fails.
#[derive(Debug)]
pub enum ActorError {
	/// Actor panicked during execution.
	Panicked(String),
	/// Mailbox was unexpectedly closed.
	MailboxClosed,
}

impl std::fmt::Display for ActorError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ActorError::Panicked(msg) => write!(f, "actor panicked: {}", msg),
			ActorError::MailboxClosed => write!(f, "actor mailbox closed unexpectedly"),
		}
	}
}

impl std::error::Error for ActorError {}

/// Internal runner that drives an actor's execution on a dedicated thread.
///
/// The runner implements a simple blocking run loop:
/// 1. Initialize actor state
/// 2. Call pre_start hook
/// 3. Loop: block on channel recv, handle message
/// 4. Call post_stop hook on termination
pub(crate) struct ActorRunner<A: Actor> {
	actor: A,
	mailbox: Mailbox<A::Message>,
	ctx: Context<A::Message>,
}

impl<A: Actor> ActorRunner<A> {
	/// Create a new actor runner.
	pub fn new(actor: A, mailbox: Mailbox<A::Message>, ctx: Context<A::Message>) -> Self {
		Self {
			actor,
			mailbox,
			ctx,
		}
	}

	/// Run the actor to completion.
	///
	/// This is the main entry point for actor execution.
	/// Runs on a dedicated OS thread with blocking receives.
	pub fn run(mut self) {
		// Initialize state
		let mut state = self.actor.init(&self.ctx);

		// Pre-start hook
		self.actor.pre_start(&mut state, &self.ctx);

		// Run the main loop
		self.run_loop(&mut state);

		// Post-stop hook (always called)
		self.actor.post_stop(&mut state);
	}

	/// The main run loop - uses recv_timeout to allow periodic cancellation checks.
	fn run_loop(&mut self, state: &mut A::State) {
		loop {
			// Check for cancellation
			if self.ctx.is_cancelled() {
				tracing::debug!("Actor cancelled, stopping");
				return;
			}

			// Use timeout to allow periodic cancellation checks
			match self.mailbox.recv_timeout(SHUTDOWN_CHECK_INTERVAL) {
				Ok(msg) => {
					match self.actor.handle(state, msg, &self.ctx) {
						Flow::Stop => {
							tracing::debug!("Actor returned Flow::Stop");
							return;
						}
						// Continue, Yield, Park all continue the loop
						// (Yield and Park are no-ops since we have our own thread)
						Flow::Continue | Flow::Yield | Flow::Park => continue,
					}
				}
				Err(RecvTimeoutError::Timeout) => {
					// Timeout elapsed, check cancellation on next iteration
					continue;
				}
				Err(RecvTimeoutError::Closed) => {
					// Channel closed (all senders dropped)
					tracing::debug!("Actor mailbox closed, stopping");
					return;
				}
			}
		}
	}
}
