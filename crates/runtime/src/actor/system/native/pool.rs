// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pool-based scheduler for actors with Send-compatible state.
//!
//! Actors run on the shared rayon thread pool with work-stealing.

use std::{thread, time::Duration};

use crossbeam_channel::Receiver;

use crate::actor::{
	context::{CancellationToken, Context},
	mailbox::{ActorRef, create_mailbox},
	traits::{Actor, Flow},
};

use super::{ActorSystem, JoinError};

/// Interval for checking cancellation during blocked recv.
const SHUTDOWN_CHECK_INTERVAL: Duration = Duration::from_millis(10);

/// Maximum messages to process in one batch before yielding.
const BATCH_SIZE: usize = 64;

/// Handle to an actor running on the shared pool.
pub struct PoolActorHandle<M> {
	pub actor_ref: ActorRef<M>,
	/// Join handle for the worker thread (pool actors still use a thread for their run loop)
	join_handle: Option<thread::JoinHandle<()>>,
}

impl<M> PoolActorHandle<M> {
	/// Get the actor reference.
	pub fn actor_ref(&self) -> &ActorRef<M> {
		&self.actor_ref
	}

	/// Wait for the actor to complete.
	pub fn join(mut self) -> Result<(), JoinError> {
		if let Some(handle) = self.join_handle.take() {
			handle.join().map_err(|e| JoinError::new(format!("{:?}", e)))
		} else {
			Ok(())
		}
	}
}

/// Spawn an actor on the shared pool.
///
/// The actor runs its own message loop on a dedicated thread, but message
/// processing can use the shared rayon pool for parallel work via `install()`.
pub(super) fn spawn_on_pool<A: Actor>(system: &ActorSystem, name: &str, actor: A) -> PoolActorHandle<A::Message>
where
	A::State: Send,
{
	let config = actor.config();
	let (actor_ref, mailbox) = create_mailbox(config.mailbox_capacity);

	let ctx = Context::new(actor_ref.clone(), system.clone(), system.cancellation_token());

	let thread_name = name.to_string();
	let cancel = system.cancellation_token();
	let rx = mailbox.rx;

	let handle = thread::Builder::new()
		.name(thread_name.clone())
		.spawn(move || {
			tracing::debug!(actor = %thread_name, "Pool actor thread starting");
			run_actor_loop(actor, rx, ctx, cancel);
			tracing::debug!(actor = %thread_name, "Pool actor thread stopped");
		})
		.expect("Failed to spawn actor thread");

	PoolActorHandle {
		actor_ref,
		join_handle: Some(handle),
	}
}

/// Run the actor's message loop.
fn run_actor_loop<A: Actor>(actor: A, rx: Receiver<A::Message>, ctx: Context<A::Message>, cancel: CancellationToken)
where
	A::State: Send,
{
	// Initialize state
	let mut state = actor.init(&ctx);

	// Pre-start hook
	actor.pre_start(&mut state, &ctx);

	// Run the main loop
	loop {
		// Check for cancellation
		if cancel.is_cancelled() {
			tracing::debug!("Pool actor cancelled, stopping");
			break;
		}

		// Use timeout to allow periodic cancellation checks
		match rx.recv_timeout(SHUTDOWN_CHECK_INTERVAL) {
			Ok(msg) => {
				match actor.handle(&mut state, msg, &ctx) {
					Flow::Stop => {
						tracing::debug!("Pool actor returned Flow::Stop");
						break;
					}
					// Continue, Yield, Park all continue the loop
					Flow::Continue | Flow::Yield | Flow::Park => continue,
				}
			}
			Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
				// Timeout elapsed, check cancellation on next iteration
				continue;
			}
			Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
				// Channel closed (all senders dropped)
				tracing::debug!("Pool actor mailbox closed, stopping");
				break;
			}
		}
	}

	// Post-stop hook (always called)
	actor.post_stop(&mut state);
}
