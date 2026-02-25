// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Pool-based scheduler for actors with Send-compatible state.
//!
//! Each actor is a self-scheduling state machine on the shared rayon pool.
//! No dedicated OS thread per actor — idle actors consume zero pool resources.

use std::sync::{
	Arc, Mutex,
	atomic::{AtomicU8, Ordering},
};

use crossbeam_channel::Receiver;
use rayon::ThreadPool;
use tracing::debug;

use super::{ActorSystem, JoinError};
use crate::actor::{
	context::{CancellationToken, Context},
	mailbox::{ActorRef, create_mailbox},
	traits::{Actor, Directive},
};

/// Maximum messages to process in one batch before yielding.
const BATCH_SIZE: usize = 64;

// Schedule states
const IDLE: u8 = 0;
const SCHEDULED: u8 = 1;
const NOTIFIED: u8 = 2;

/// Per-actor cell holding all state needed for pool-based scheduling.
struct ActorCell<A: Actor> {
	actor: A,
	state: Mutex<Option<A::State>>,
	rx: Receiver<A::Message>,
	ctx: Context<A::Message>,
	cancel: CancellationToken,
	schedule_state: AtomicU8,
	completion_tx: crossbeam_channel::Sender<()>,
	done_tx: crossbeam_channel::Sender<()>,
	pool: Arc<ThreadPool>,
}

/// Transition the actor to SCHEDULED and submit a task to the pool if it was IDLE.
fn notify<A: Actor>(cell: &Arc<ActorCell<A>>)
where
	A::State: Send,
{
	// IDLE → SCHEDULED: we must submit. SCHEDULED → NOTIFIED: already queued. NOTIFIED → NOTIFIED: no-op.
	let prev = cell.schedule_state.fetch_max(SCHEDULED, Ordering::AcqRel);
	if prev == IDLE {
		let cell = Arc::clone(cell);
		let pool = Arc::clone(&cell.pool);
		pool.spawn(move || run_batch(cell));
	}
}

/// Process up to BATCH_SIZE messages, then decide whether to reschedule or go idle.
fn run_batch<A: Actor>(cell: Arc<ActorCell<A>>)
where
	A::State: Send,
{
	let mut guard = cell.state.lock().unwrap();
	let state = match guard.as_mut() {
		Some(s) => s,
		None => {
			// Actor was already stopped (shouldn't happen, but be safe)
			cell.schedule_state.store(IDLE, Ordering::Release);
			return;
		}
	};

	let mut processed = 0;
	let mut flow = Directive::Continue;

	while processed < BATCH_SIZE {
		// Check cancellation
		if cell.cancel.is_cancelled() {
			flow = Directive::Stop;
			break;
		}

		match cell.rx.try_recv() {
			Ok(msg) => {
				processed += 1;
				flow = cell.actor.handle(state, msg, &cell.ctx);
				match flow {
					Directive::Continue => continue,
					Directive::Yield | Directive::Park | Directive::Stop => break,
				}
			}
			Err(crossbeam_channel::TryRecvError::Empty) => {
				// No messages — run idle handler
				flow = cell.actor.idle(&cell.ctx);
				break;
			}
			Err(crossbeam_channel::TryRecvError::Disconnected) => {
				// All senders dropped
				debug!("Pool actor mailbox closed, stopping");
				flow = Directive::Stop;
				break;
			}
		}
	}

	match flow {
		Directive::Stop => {
			cell.actor.post_stop();
			*guard = None;
			cell.schedule_state.store(IDLE, Ordering::Release);
			let _ = cell.completion_tx.send(());
			let _ = cell.done_tx.send(());
		}
		Directive::Park => {
			// Go idle — consume zero pool resources until next send()
			cell.schedule_state.store(IDLE, Ordering::Release);
			drop(guard);

			// Check if messages arrived between try_recv and storing IDLE.
			// If so, re-notify to avoid lost wakeup.
			let has_msgs = !cell.rx.is_empty();
			let cancelled = cell.cancel.is_cancelled();
			if has_msgs || cancelled {
				notify(&cell);
			}
		}
		Directive::Yield | Directive::Continue => {
			// End of batch or explicit yield — check if we should reschedule
			drop(guard);

			// Try NOTIFIED → SCHEDULED (new messages arrived during batch)
			// or SCHEDULED → IDLE (no new messages, go idle)
			let prev = cell.schedule_state.compare_exchange(
				NOTIFIED,
				SCHEDULED,
				Ordering::AcqRel,
				Ordering::Acquire,
			);

			match prev {
				Ok(_) => {
					// Was NOTIFIED, resubmit
					let cell2 = Arc::clone(&cell);
					cell.pool.spawn(move || run_batch(cell2));
				}
				Err(SCHEDULED) => {
					// No new notifications — check if there are still messages
					if !cell.rx.is_empty() || cell.cancel.is_cancelled() {
						// Messages still pending, resubmit
						let cell2 = Arc::clone(&cell);
						cell.pool.spawn(move || run_batch(cell2));
					} else {
						// Go idle
						cell.schedule_state.store(IDLE, Ordering::Release);
						// Double check for lost wakeup
						if !cell.rx.is_empty() || cell.cancel.is_cancelled() {
							notify(&cell);
						}
					}
				}
				Err(_) => {
					// IDLE — shouldn't happen during a running batch, but be safe
				}
			}
		}
	}
}

/// Handle to an actor running on the shared pool.
pub struct PoolActorHandle<M> {
	pub actor_ref: ActorRef<M>,
	completion_rx: crossbeam_channel::Receiver<()>,
}

impl<M> PoolActorHandle<M> {
	/// Get the actor reference.
	pub fn actor_ref(&self) -> &ActorRef<M> {
		&self.actor_ref
	}

	/// Wait for the actor to complete.
	pub fn join(self) -> Result<(), JoinError> {
		self.completion_rx.recv().map_err(|_| JoinError::new("actor completion channel disconnected"))
	}
}

/// Spawn an actor on the shared pool.
pub(super) fn spawn_on_pool<A: Actor>(system: &ActorSystem, name: &str, actor: A) -> PoolActorHandle<A::Message>
where
	A::State: Send,
{
	let config = actor.config();
	let (actor_ref, mailbox) = create_mailbox(config.mailbox_capacity);

	let ctx = Context::new(actor_ref.clone(), system.clone(), system.cancellation_token());

	let cancel = system.cancellation_token();
	let rx = mailbox.rx;
	let pool = Arc::clone(system.pool());

	let (completion_tx, completion_rx) = crossbeam_channel::bounded(1);
	let (done_tx, done_rx) = crossbeam_channel::bounded(1);
	system.register_done_rx(done_rx);

	let cell = Arc::new(ActorCell {
		actor,
		state: Mutex::new(None),
		rx,
		ctx,
		cancel,
		schedule_state: AtomicU8::new(SCHEDULED),
		completion_tx,
		done_tx,
		pool,
	});

	// Notify closure uses Weak to avoid ActorCell ↔ notify self-referential cycle.
	let cell_weak = Arc::downgrade(&cell);
	let notify_fn: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
		if let Some(cell) = cell_weak.upgrade() {
			notify(&cell);
		}
	});
	actor_ref.set_notify(notify_fn.clone());
	system.register_waker(notify_fn);

	// The Weak-based notify closure doesn't keep the cell alive, so register a
	// strong ref that the system drops on shutdown.
	system.register_keepalive(Box::new(Arc::clone(&cell)));

	// Spawn init + first batch on the pool
	let actor_name = name.to_string();
	let cell_for_init = Arc::clone(&cell);
	cell.pool.spawn(move || {
		debug!(actor = %actor_name, "Pool actor starting");

		{
			let mut guard = cell_for_init.state.lock().unwrap();
			let state_val = cell_for_init.actor.init(&cell_for_init.ctx);
			*guard = Some(state_val);
		}

		run_batch(cell_for_init);
	});

	PoolActorHandle {
		actor_ref,
		completion_rx,
	}
}
