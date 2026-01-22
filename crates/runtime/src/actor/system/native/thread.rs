// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Thread-based scheduler for actors with non-Send state.
//!
//! Each actor runs on its own dedicated OS thread.

use std::{thread, time::Duration};

use crossbeam_channel::{Receiver, RecvTimeoutError};
use tracing::debug;
use crate::actor::{
	context::{CancellationToken, Context},
	mailbox::{ActorRef, create_mailbox},
	traits::{Actor, Flow},
};

use super::{ActorSystem, JoinError};

/// Interval for checking cancellation during blocked recv.
const SHUTDOWN_CHECK_INTERVAL: Duration = Duration::from_millis(10);

/// Handle to an actor running on a dedicated thread.
pub struct ThreadActorHandle<M> {
	pub actor_ref: ActorRef<M>,
	join_handle: Option<thread::JoinHandle<()>>,
}

impl<M> ThreadActorHandle<M> {
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

/// Spawn an actor on a dedicated OS thread.
///
/// This allows actors with non-Send state (like Rc, RefCell) to work correctly.
pub(super) fn spawn_on_thread<A: Actor>(system: &ActorSystem, name: &str, actor: A) -> ThreadActorHandle<A::Message> {
	let config = actor.config();
	let (actor_ref, mailbox) = create_mailbox(config.mailbox_capacity);

	let ctx = Context::new(actor_ref.clone(), system.clone(), system.cancellation_token());

	let thread_name = name.to_string();
	let cancel = system.cancellation_token();
	let rx = mailbox.rx;

	let handle = thread::Builder::new()
		.name(thread_name.clone())
		.spawn(move || {
			debug!(actor = %thread_name, "Dedicated thread actor starting");
			run_actor_loop(actor, rx, ctx, cancel);
			debug!(actor = %thread_name, "Dedicated thread actor stopped");
		})
		.expect("Failed to spawn actor thread");

	ThreadActorHandle {
		actor_ref,
		join_handle: Some(handle),
	}
}

/// Run the actor's message loop on its dedicated thread.
fn run_actor_loop<A: Actor>(actor: A, rx: Receiver<A::Message>, ctx: Context<A::Message>, cancel: CancellationToken) {
	let mut state = actor.init(&ctx);

	actor.pre_start(&mut state, &ctx);

	loop {
		if cancel.is_cancelled() {
			debug!("Dedicated thread actor cancelled, stopping");
			break;
		}

		// Use timeout to allow periodic cancellation checks
		match rx.recv_timeout(SHUTDOWN_CHECK_INTERVAL) {
			Ok(msg) => {
				match actor.handle(&mut state, msg, &ctx) {
					Flow::Stop => {
						debug!("Dedicated thread actor returned Flow::Stop");
						break;
					}
					Flow::Continue | Flow::Yield | Flow::Park => continue,
				}
			}
			Err(RecvTimeoutError::Timeout) => {
				// Timeout elapsed, check cancellation on next iteration
				continue;
			}
			Err(RecvTimeoutError::Disconnected) => {
				// Channel closed (all senders dropped)
				debug!("Dedicated thread actor mailbox closed, stopping");
				break;
			}
		}
	}

	actor.post_stop(&mut state);
}
