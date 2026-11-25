// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	mem,
	sync::{
		Arc,
		atomic::{AtomicU8, Ordering, fence},
	},
	thread,
};

use crossbeam_channel::{Receiver, Sender, TryRecvError as CcTryRecvError, bounded};
use reifydb_value::reifydb_assertions;
use tracing::{debug, trace_span};

use super::{ActorSystem, JoinError};
use crate::{
	actor::{
		context::{CancellationToken, Context},
		mailbox::{ActorRef, create_mailbox},
		traits::{Actor, Directive},
	},
	context::clock::{Clock, Instant},
	pool::actor_pool::{Runnable, Schedule},
	sync::mutex::{Mutex, MutexGuard},
};

const IDLE: u8 = 0;
const SCHEDULED: u8 = 1;
const NOTIFIED: u8 = 2;

enum CellState<S, M> {
	Uninit(Receiver<(Instant, M)>),
	Running(S, Receiver<(Instant, M)>),
	Stopped,
}

struct ActorCell<A: Actor> {
	actor: A,
	name: String,
	state: Mutex<CellState<A::State, A::Message>>,
	tx: Sender<(Instant, A::Message)>,
	clock: Clock,
	ctx: Context<A::Message>,
	cancel: CancellationToken,
	schedule_state: AtomicU8,
	completion_tx: Sender<()>,
	done_tx: Sender<Result<(), String>>,
	schedule: Schedule,
	batch_size: usize,
}

impl<A: Actor> Runnable for ActorCell<A>
where
	A::State: Send,
{
	fn run(self: Arc<Self>) {
		run_batch(self)
	}
}

fn notify<A: Actor>(cell: &Arc<ActorCell<A>>)
where
	A::State: Send,
{
	let prev = cell.schedule_state.fetch_max(SCHEDULED, Ordering::AcqRel);
	if prev == IDLE {
		cell.schedule.enqueue(Arc::clone(cell) as Arc<dyn Runnable>);
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

	let _crash = CrashSignal {
		done_tx: &cell.done_tx,
		name: &cell.name,
	};
	let mut guard = match lock_state_or_bail(&cell) {
		Some(guard) => guard,
		None => return,
	};

	let (state, rx) = match &mut *guard {
		CellState::Running(state, rx) => (state, rx),
		CellState::Uninit(_) | CellState::Stopped => {
			unreachable!("lock_state_or_bail returned a non-running state")
		}
	};
	let flow = process_message_batch(&cell, state, rx);

	dispatch_directive(&cell, guard, flow);
}

struct CrashSignal<'a> {
	done_tx: &'a Sender<Result<(), String>>,
	name: &'a str,
}

impl Drop for CrashSignal<'_> {
	fn drop(&mut self) {
		if thread::panicking() {
			let _ = self.done_tx.try_send(Err(self.name.to_string()));
		}
	}
}

#[inline]
fn lock_state_or_bail<A: Actor>(cell: &Arc<ActorCell<A>>) -> Option<MutexGuard<'_, CellState<A::State, A::Message>>>
where
	A::State: Send,
{
	let mut guard = cell.state.lock();
	match &*guard {
		CellState::Running(..) => Some(guard),
		CellState::Stopped => {
			cell.schedule_state.store(IDLE, Ordering::Release);
			None
		}
		CellState::Uninit(_) => {
			debug!(actor = %cell.name, "Pool actor starting");
			let state = cell.actor.init(&cell.ctx);
			let CellState::Uninit(rx) = mem::replace(&mut *guard, CellState::Stopped) else {
				unreachable!("the cell left Uninit while its state lock was held")
			};
			*guard = CellState::Running(state, rx);
			Some(guard)
		}
	}
}

fn report_mailbox_wait(actor: &str, clock: &Clock, sent_at: &Instant) {
	let waited_us = u64::try_from(clock.instant().duration_since(sent_at).as_micros()).unwrap_or(u64::MAX);
	drop(trace_span!("actor::mailbox::wait", actor, waited_us));
}

#[inline]
fn process_message_batch<A: Actor>(
	cell: &Arc<ActorCell<A>>,
	state: &mut A::State,
	rx: &Receiver<(Instant, A::Message)>,
) -> Directive
where
	A::State: Send,
{
	let mut processed = 0;
	let mut flow = Directive::Continue;

	while processed < cell.batch_size {
		if cell.cancel.is_cancelled() {
			flow = Directive::Stop;
			break;
		}

		match rx.try_recv() {
			Ok((sent_at, msg)) => {
				report_mailbox_wait(&cell.name, &cell.clock, &sent_at);
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
fn dispatch_directive<A: Actor>(
	cell: &Arc<ActorCell<A>>,
	mut guard: MutexGuard<'_, CellState<A::State, A::Message>>,
	flow: Directive,
) where
	A::State: Send,
{
	match flow {
		Directive::Stop => {
			cell.actor.post_stop();
			*guard = CellState::Stopped;
			cell.schedule_state.store(IDLE, Ordering::Release);
			let _ = cell.completion_tx.send(());
			let _ = cell.done_tx.send(Ok(()));
		}
		Directive::Park => {
			cell.schedule_state.store(IDLE, Ordering::Release);
			drop(guard);
			fence(Ordering::SeqCst);

			let has_msgs = !cell.tx.is_empty();
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
					cell.schedule.enqueue(Arc::clone(cell) as Arc<dyn Runnable>);
				}
				Err(SCHEDULED) => {
					if !cell.tx.is_empty() || cell.cancel.is_cancelled() {
						cell.schedule.enqueue(Arc::clone(cell) as Arc<dyn Runnable>);
					} else {
						cell.schedule_state.store(IDLE, Ordering::Release);
						fence(Ordering::SeqCst);

						if !cell.tx.is_empty() || cell.cancel.is_cancelled() {
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

pub(super) fn spawn_on_schedule<A: Actor>(
	system: &ActorSystem,
	name: &str,
	actor: A,
	schedule: Schedule,
	batch_size: usize,
) -> PoolActorHandle<A::Message>
where
	A::State: Send,
{
	let config = actor.config();
	let batch_size = config.batch_size.unwrap_or(batch_size);
	let (actor_ref, mailbox) = create_mailbox(config.mailbox_capacity, system.clock().clone());
	let ctx = Context::new(actor_ref.clone(), system.clone(), system.cancellation_token());
	let cancel = system.cancellation_token();

	let (completion_tx, completion_rx) = bounded(1);
	let (done_tx, done_rx) = bounded(1);
	system.register_done_rx(done_rx);

	let cell = Arc::new(ActorCell {
		actor,
		name: name.to_string(),
		state: Mutex::new(CellState::Uninit(mailbox.rx)),
		tx: mailbox.tx,
		clock: system.clock().clone(),
		ctx,
		cancel,
		schedule_state: AtomicU8::new(SCHEDULED),
		completion_tx,
		done_tx,
		schedule,
		batch_size,
	});

	register_actor_hooks(&cell, &actor_ref, system);
	cell.schedule.enqueue(Arc::clone(&cell) as Arc<dyn Runnable>);

	PoolActorHandle {
		actor_ref,
		completion_rx,
	}
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

#[cfg(test)]
mod tests {
	use std::{
		fmt::Debug,
		sync::{Arc, Mutex},
	};

	use tracing::{
		Event, Metadata, Subscriber,
		field::{Field, Visit},
		span::{Attributes, Id, Record},
		subscriber::with_default,
	};

	use super::*;
	use crate::context::clock::MockClock;

	#[derive(Default)]
	struct Wait {
		actor: Option<String>,
		waited_us: Option<u64>,
	}

	impl Visit for Wait {
		fn record_str(&mut self, field: &Field, value: &str) {
			if field.name() == "actor" {
				self.actor = Some(value.to_string());
			}
		}

		fn record_u64(&mut self, field: &Field, value: u64) {
			if field.name() == "waited_us" {
				self.waited_us = Some(value);
			}
		}

		fn record_debug(&mut self, _field: &Field, _value: &dyn Debug) {}
	}

	#[derive(Clone, Default)]
	struct Opened {
		spans: Arc<Mutex<Vec<(&'static str, Option<String>, Option<u64>)>>>,
	}

	impl Subscriber for Opened {
		fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
			true
		}

		fn new_span(&self, attrs: &Attributes<'_>) -> Id {
			let mut wait = Wait::default();
			attrs.record(&mut wait);
			self.spans.lock().unwrap().push((attrs.metadata().name(), wait.actor, wait.waited_us));
			Id::from_u64(1)
		}

		fn record(&self, _span: &Id, _values: &Record<'_>) {}

		fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

		fn event(&self, _event: &Event<'_>) {}

		fn enter(&self, _span: &Id) {}

		fn exit(&self, _span: &Id) {}
	}

	#[test]
	fn a_received_message_reports_the_time_since_its_send_under_the_actor_name() {
		// The wait must run from the send stamp to the receive, otherwise a stalled actor reports no wait.
		let mock = MockClock::new(1_000);
		let clock = Clock::Mock(mock.clone());
		let sent_at = clock.instant();
		mock.advance_micros(1_200_000);

		let opened = Opened::default();
		with_default(opened.clone(), || report_mailbox_wait("flow-7", &clock, &sent_at));

		assert_eq!(
			*opened.spans.lock().unwrap(),
			vec![("actor::mailbox::wait", Some("flow-7".to_string()), Some(1_200_000))]
		);
	}
}
