// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{
	Arc, Mutex,
	atomic::{AtomicU8, Ordering},
};

use crossbeam_channel::{Receiver, Sender, TryRecvError as CcTryRecvError, bounded};
use rayon::ThreadPool;
use tracing::debug;

use super::{ActorSystem, JoinError};
use crate::actor::{
	context::{CancellationToken, Context},
	mailbox::{ActorRef, create_mailbox},
	traits::{Actor, Directive},
};

const BATCH_SIZE: usize = 64;

const IDLE: u8 = 0;
const SCHEDULED: u8 = 1;
const NOTIFIED: u8 = 2;

struct ActorCell<A: Actor> {
	actor: A,
	state: Mutex<Option<A::State>>,
	rx: Receiver<A::Message>,
	ctx: Context<A::Message>,
	cancel: CancellationToken,
	schedule_state: AtomicU8,
	completion_tx: Sender<()>,
	done_tx: Sender<()>,
	pool: Arc<ThreadPool>,
}

fn notify<A: Actor>(cell: &Arc<ActorCell<A>>)
where
	A::State: Send,
{
	let prev = cell.schedule_state.fetch_max(SCHEDULED, Ordering::AcqRel);
	if prev == IDLE {
		let pool = Arc::clone(&cell.pool);
		let cell = Arc::clone(cell);
		pool.spawn(move || run_batch(cell));
	}
}

fn run_batch<A: Actor>(cell: Arc<ActorCell<A>>)
where
	A::State: Send,
{
	let mut guard = cell.state.lock().unwrap();
	let state = match guard.as_mut() {
		Some(s) => s,
		None => {
			cell.schedule_state.store(IDLE, Ordering::Release);
			return;
		}
	};

	let mut processed = 0;
	let mut flow = Directive::Continue;

	while processed < BATCH_SIZE {
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
			Err(CcTryRecvError::Empty) => {
				flow = cell.actor.idle(&cell.ctx);
				break;
			}
			Err(CcTryRecvError::Disconnected) => {
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
			cell.schedule_state.store(IDLE, Ordering::Release);
			drop(guard);

			let has_msgs = !cell.rx.is_empty();
			let cancelled = cell.cancel.is_cancelled();
			if has_msgs || cancelled {
				notify(&cell);
			}
		}
		Directive::Yield | Directive::Continue => {
			drop(guard);

			let prev = cell.schedule_state.compare_exchange(
				NOTIFIED,
				SCHEDULED,
				Ordering::AcqRel,
				Ordering::Acquire,
			);

			match prev {
				Ok(_) => {
					let pool = Arc::clone(&cell.pool);
					let cell2 = Arc::clone(&cell);
					pool.spawn(move || run_batch(cell2));
				}
				Err(SCHEDULED) => {
					if !cell.rx.is_empty() || cell.cancel.is_cancelled() {
						let pool = Arc::clone(&cell.pool);
						let cell2 = Arc::clone(&cell);
						pool.spawn(move || run_batch(cell2));
					} else {
						cell.schedule_state.store(IDLE, Ordering::Release);

						if !cell.rx.is_empty() || cell.cancel.is_cancelled() {
							notify(&cell);
						}
					}
				}
				Err(_) => {}
			}
		}
	}
}

pub struct PoolActorHandle<M> {
	pub actor_ref: ActorRef<M>,
	completion_rx: Receiver<()>,
}

impl<M> PoolActorHandle<M> {
	pub fn actor_ref(&self) -> &ActorRef<M> {
		&self.actor_ref
	}

	pub fn join(self) -> Result<(), JoinError> {
		self.completion_rx.recv().map_err(|_| JoinError::new("actor completion channel disconnected"))
	}
}

pub(super) fn spawn_on_pool<A: Actor>(
	system: &ActorSystem,
	name: &str,
	actor: A,
	pool: &Arc<ThreadPool>,
) -> PoolActorHandle<A::Message>
where
	A::State: Send,
{
	let config = actor.config();
	let (actor_ref, mailbox) = create_mailbox(config.mailbox_capacity);

	let ctx = Context::new(actor_ref.clone(), system.clone(), system.cancellation_token());

	let cancel = system.cancellation_token();
	let rx = mailbox.rx;

	let (completion_tx, completion_rx) = bounded(1);
	let (done_tx, done_rx) = bounded(1);
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
		pool: Arc::clone(pool),
	});

	let cell_weak = Arc::downgrade(&cell);
	let notify_fn: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
		if let Some(cell) = cell_weak.upgrade() {
			notify(&cell);
		}
	});
	actor_ref.set_notify(notify_fn.clone());
	system.register_waker(notify_fn);

	system.register_keepalive(Box::new(Arc::clone(&cell)));

	let actor_name = name.to_string();
	let cell_for_init = Arc::clone(&cell);
	let pool_for_init = Arc::clone(pool);
	pool_for_init.spawn(move || {
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
