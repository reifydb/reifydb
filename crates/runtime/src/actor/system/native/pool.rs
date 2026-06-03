// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

use std::sync::{
	Arc,
	atomic::{AtomicU8, Ordering},
};

use crossbeam_channel::{Receiver, Sender, TryRecvError as CcTryRecvError, bounded};
use rayon::ThreadPool;
use reifydb_value::reifydb_assertions;
use tracing::debug;

use super::{ActorSystem, JoinError};
use crate::{
	actor::{
		context::{CancellationToken, Context},
		mailbox::{ActorRef, create_mailbox},
		traits::{Actor, Directive},
	},
	sync::mutex::{Mutex, MutexGuard},
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
	reifydb_assertions! {
		let s = cell.schedule_state.load(Ordering::Acquire);
		assert!(
			s == SCHEDULED || s == NOTIFIED,
			"actor run_batch entered while schedule_state={s} (expected SCHEDULED={SCHEDULED} or NOTIFIED={NOTIFIED}); this is a spurious wakeup that bypassed the notify guard"
		);
	}

	let mut guard = match lock_state_or_bail(&cell) {
		Some(guard) => guard,
		None => return,
	};

	reifydb_assertions! {
		assert!(
			guard.is_some(),
			"lock_state_or_bail returned a guard whose state is None; the batch loop would then run actor.handle against a dropped actor state"
		);
	}
	let flow = process_message_batch(&cell, guard.as_mut().unwrap());

	dispatch_directive(&cell, guard, flow);
}

#[inline]
fn lock_state_or_bail<A: Actor>(cell: &Arc<ActorCell<A>>) -> Option<MutexGuard<'_, Option<A::State>>>
where
	A::State: Send,
{
	let guard = cell.state.lock();
	if guard.is_none() {
		cell.schedule_state.store(IDLE, Ordering::Release);
		return None;
	}
	Some(guard)
}

#[inline]
fn process_message_batch<A: Actor>(cell: &Arc<ActorCell<A>>, state: &mut A::State) -> Directive
where
	A::State: Send,
{
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

	flow
}

#[inline]
fn dispatch_directive<A: Actor>(cell: &Arc<ActorCell<A>>, mut guard: MutexGuard<'_, Option<A::State>>, flow: Directive)
where
	A::State: Send,
{
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
				notify(cell);
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
					let cell2 = Arc::clone(cell);
					pool.spawn(move || run_batch(cell2));
				}
				Err(SCHEDULED) => {
					if !cell.rx.is_empty() || cell.cancel.is_cancelled() {
						let pool = Arc::clone(&cell.pool);
						let cell2 = Arc::clone(cell);
						pool.spawn(move || run_batch(cell2));
					} else {
						cell.schedule_state.store(IDLE, Ordering::Release);

						if !cell.rx.is_empty() || cell.cancel.is_cancelled() {
							notify(cell);
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

	let (completion_tx, completion_rx) = bounded(1);
	let (done_tx, done_rx) = bounded(1);
	system.register_done_rx(done_rx);

	let cell = build_actor_cell(actor, ctx, cancel, mailbox.rx, completion_tx, done_tx, pool);

	register_actor_hooks(&cell, &actor_ref, system);
	spawn_init_task(Arc::clone(&cell), pool, name);

	PoolActorHandle {
		actor_ref,
		completion_rx,
	}
}

#[inline]
fn build_actor_cell<A: Actor>(
	actor: A,
	ctx: Context<A::Message>,
	cancel: CancellationToken,
	rx: Receiver<A::Message>,
	completion_tx: Sender<()>,
	done_tx: Sender<()>,
	pool: &Arc<ThreadPool>,
) -> Arc<ActorCell<A>>
where
	A::State: Send,
{
	Arc::new(ActorCell {
		actor,
		state: Mutex::new(None),
		rx,
		ctx,
		cancel,
		schedule_state: AtomicU8::new(SCHEDULED),
		completion_tx,
		done_tx,
		pool: Arc::clone(pool),
	})
}

#[inline]
fn register_actor_hooks<A: Actor>(cell: &Arc<ActorCell<A>>, actor_ref: &ActorRef<A::Message>, system: &ActorSystem)
where
	A::State: Send,
{
	let cell_weak = Arc::downgrade(cell);
	let notify_fn: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
		if let Some(cell) = cell_weak.upgrade() {
			notify(&cell);
		}
	});
	actor_ref.set_notify(notify_fn.clone());
	system.register_waker(notify_fn);

	system.register_keepalive(Box::new(Arc::clone(cell)));
}

#[inline]
fn spawn_init_task<A: Actor>(cell: Arc<ActorCell<A>>, pool: &Arc<ThreadPool>, name: &str)
where
	A::State: Send,
{
	let actor_name = name.to_string();
	let pool_for_init = Arc::clone(pool);
	pool_for_init.spawn(move || {
		debug!(actor = %actor_name, "Pool actor starting");

		{
			let mut guard = cell.state.lock();
			let state_val = cell.actor.init(&cell.ctx);
			*guard = Some(state_val);
		}

		run_batch(cell);
	});
}
