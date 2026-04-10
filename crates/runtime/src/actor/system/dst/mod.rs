// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Deterministic Software Testing (DST) actor system.
//!
//! Provides tick-by-tick, externally-driven, fully deterministic actor execution.
//! All message processing is controlled via `step()` and timers via `advance_time()`.
//!
//! # Key properties
//!
//! - **Strict global FIFO**: Every `send()` assigns a global logical timestamp. Messages are processed in timestamp
//!   order regardless of which actor they target.
//! - **Deterministic timers**: Timer deadlines use mock-clock nanos, not OS time. `advance_time()` fires timers in
//!   deadline order and enqueues their messages.
//! - **Panic capture**: `step()` catches panics and returns `StepResult::Panicked` so the test runner can report the
//!   seed and actor for reproducibility.

use std::{
	any::Any,
	cell::{Cell, RefCell},
	cmp::Ordering as CmpOrdering,
	collections::{BinaryHeap, VecDeque},
	error, fmt,
	panic::{AssertUnwindSafe, catch_unwind},
	rc::Rc,
	time::Duration,
};

use crate::{
	actor::{
		context::{CancellationToken, Context},
		mailbox::{ActorRef, create_dst_mailbox},
		timers::dst::{DstTimerHeap, fire_due_timers, new_timer_heap},
		traits::{Actor, Directive},
	},
	context::clock::{Clock, MockClock},
	pool::Pools,
};

struct ReadyEntry {
	logical_ts: u64,
	actor_id: usize,
}

impl PartialEq for ReadyEntry {
	fn eq(&self, other: &Self) -> bool {
		self.logical_ts == other.logical_ts && self.actor_id == other.actor_id
	}
}

impl Eq for ReadyEntry {}

impl PartialOrd for ReadyEntry {
	fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
		Some(self.cmp(other))
	}
}

impl Ord for ReadyEntry {
	fn cmp(&self, other: &Self) -> CmpOrdering {
		// Reverse for min-heap: smaller logical_ts = higher priority
		other.logical_ts.cmp(&self.logical_ts).then_with(|| other.actor_id.cmp(&self.actor_id))
	}
}

trait DstProcessable {
	fn process_one(&self) -> Option<Directive>;
	fn has_pending(&self) -> bool;
	fn is_alive(&self) -> bool;
	fn mark_dead(&self);
	fn post_stop(&self);
}

struct DstActorCell<A: Actor> {
	actor: A,
	state: RefCell<Option<A::State>>,
	queue: Rc<RefCell<VecDeque<A::Message>>>,
	ctx: Context<A::Message>,
	actor_ref: ActorRef<A::Message>,
}

impl<A: Actor> DstProcessable for DstActorCell<A> {
	fn process_one(&self) -> Option<Directive> {
		let msg = self.queue.borrow_mut().pop_front()?;
		let mut state_ref = self.state.borrow_mut();
		if let Some(ref mut state) = *state_ref {
			Some(self.actor.handle(state, msg, &self.ctx))
		} else {
			None
		}
	}

	fn has_pending(&self) -> bool {
		!self.queue.borrow().is_empty()
	}

	fn is_alive(&self) -> bool {
		self.actor_ref.is_alive()
	}

	fn mark_dead(&self) {
		self.actor_ref.mark_stopped();
	}

	fn post_stop(&self) {
		self.actor.post_stop();
	}
}

/// Result of a single `step()` call.
pub enum StepResult {
	/// A message was processed by the given actor.
	Processed {
		actor_id: usize,
	},
	/// An actor panicked during message handling.
	Panicked {
		actor_id: usize,
		payload: Box<dyn Any + Send>,
	},
	/// An actor returned `Directive::Stop`.
	Stopped {
		actor_id: usize,
	},
	/// No actors had pending messages.
	Idle,
}

struct DstActorSystemInner {
	cancel: CancellationToken,
	clock: Clock,
	mock_clock: MockClock,

	/// Global FIFO ready queue (min-heap by logical timestamp).
	ready_queue: RefCell<BinaryHeap<ReadyEntry>>,
	/// Monotonically increasing logical clock, incremented on each send().
	logical_clock: Cell<u64>,

	/// Timer heap (min-heap by deadline_nanos).
	timer_heap: DstTimerHeap,

	/// Type-erased actor cells. Dead actors have their slot set to None.
	actors: RefCell<Vec<Option<Rc<dyn DstProcessable>>>>,

	/// Number of currently alive actors.
	alive_count: Cell<usize>,

	/// Panics captured during init() — drained by step() as StepResult::Panicked.
	init_panics: RefCell<Vec<(usize, Box<dyn Any + Send>)>>,

	/// Panics captured during post_stop() — drained by step() as StepResult::Panicked.
	post_stop_panics: RefCell<Vec<(usize, Box<dyn Any + Send>)>>,

	/// Child scopes for hierarchical shutdown and clock propagation.
	children: RefCell<Vec<ActorSystem>>,
}

/// Deterministic actor system for testing.
///
/// Uses `Rc` internally (single-threaded). Cheap to clone.
pub struct ActorSystem {
	inner: Rc<DstActorSystemInner>,
}

impl Clone for ActorSystem {
	fn clone(&self) -> Self {
		Self {
			inner: self.inner.clone(),
		}
	}
}

// SAFETY: DST is single-threaded. These impls are required because the Actor
// trait bounds require Send + Sync + 'static and Context holds an ActorSystem.
unsafe impl Send for ActorSystem {}
unsafe impl Sync for ActorSystem {}

impl ActorSystem {
	/// Create a new DST actor system.
	///
	/// Pools are ignored in DST (single-threaded execution).
	///
	/// # Panics
	///
	/// Panics if `clock` is `Clock::Real`. DST requires a `MockClock`.
	pub fn new(_pools: Pools, clock: Clock) -> Self {
		let mock_clock = match &clock {
			Clock::Mock(mc) => mc.clone(),
			Clock::Real => panic!("DST actor system requires a MockClock, not Clock::Real"),
		};

		Self {
			inner: Rc::new(DstActorSystemInner {
				cancel: CancellationToken::new(),
				clock,
				mock_clock,
				ready_queue: RefCell::new(BinaryHeap::new()),
				logical_clock: Cell::new(0),
				timer_heap: new_timer_heap(),
				actors: RefCell::new(Vec::new()),
				alive_count: Cell::new(0),
				init_panics: RefCell::new(Vec::new()),
				post_stop_panics: RefCell::new(Vec::new()),
				children: RefCell::new(Vec::new()),
			}),
		}
	}

	/// Create a scoped child system sharing timers but with own cancel/actors/clock.
	///
	/// The child's cancellation token is a child of the parent's, so parent
	/// shutdown propagates downward. The child gets its own `MockClock`
	/// initialized to the parent's current time; parent `advance_time()`
	/// propagates to children but not vice versa.
	pub fn scope(&self) -> Self {
		let child_mock_clock = MockClock::new(self.inner.mock_clock.now_nanos());
		let child = Self {
			inner: Rc::new(DstActorSystemInner {
				cancel: self.inner.cancel.child_token(),
				clock: Clock::Mock(child_mock_clock.clone()),
				mock_clock: child_mock_clock,
				ready_queue: RefCell::new(BinaryHeap::new()),
				logical_clock: Cell::new(0),
				timer_heap: self.inner.timer_heap.clone(),
				actors: RefCell::new(Vec::new()),
				alive_count: Cell::new(0),
				init_panics: RefCell::new(Vec::new()),
				post_stop_panics: RefCell::new(Vec::new()),
				children: RefCell::new(Vec::new()),
			}),
		};
		self.inner.children.borrow_mut().push(child.clone());
		child
	}

	/// Get the pools for this system.
	///
	/// In DST, returns a zero-size marker (no real thread pools).
	pub fn pools(&self) -> Pools {
		Pools::default()
	}

	/// Get the cancellation token for this system.
	pub fn cancellation_token(&self) -> CancellationToken {
		self.inner.cancel.clone()
	}

	/// Check if the system has been cancelled.
	pub fn is_cancelled(&self) -> bool {
		self.inner.cancel.is_cancelled()
	}

	/// Signal shutdown to all actors and child scopes.
	pub fn shutdown(&self) {
		self.inner.cancel.cancel();

		// Propagate shutdown to child scopes.
		for child in self.inner.children.borrow().iter() {
			child.shutdown();
		}

		let actors = self.inner.actors.borrow();
		for slot in actors.iter() {
			if let Some(cell) = slot {
				if cell.is_alive() {
					let _ = catch_unwind(AssertUnwindSafe(|| cell.post_stop()));
					cell.mark_dead();
				}
			}
		}
		self.inner.alive_count.set(0);
	}

	/// Get the clock for this system.
	pub fn clock(&self) -> &Clock {
		&self.inner.clock
	}

	/// Get the mock clock (DST-specific).
	pub(crate) fn mock_clock(&self) -> &MockClock {
		&self.inner.mock_clock
	}

	/// Get the timer heap (DST-specific).
	pub(crate) fn timer_heap(&self) -> &DstTimerHeap {
		&self.inner.timer_heap
	}

	/// Wait for all actors to finish after shutdown (no-op in DST).
	pub fn join(&self) -> Result<(), JoinError> {
		Ok(())
	}

	/// Wait for all actors to finish after shutdown with timeout (no-op in DST).
	pub fn join_timeout(&self, _timeout: Duration) -> Result<(), JoinError> {
		Ok(())
	}

	/// Spawn an actor.
	///
	/// The actor is initialized synchronously. Messages sent during init
	/// are enqueued and will be processed by subsequent `step()` calls.
	///
	/// If the system has already been shut down, returns a handle to a
	/// pre-terminated actor (sends will fail with `SendError::Closed`).
	pub fn spawn<A: Actor>(&self, _name: &str, actor: A) -> ActorHandle<A::Message> {
		let (actor_ref, queue) = create_dst_mailbox::<A::Message>();

		// If the system is already shut down, return a dead actor handle.
		if self.is_cancelled() {
			actor_ref.mark_stopped();
			return ActorHandle {
				actor_ref,
			};
		}

		let cancel = self.cancellation_token();
		let ctx = Context::new(actor_ref.clone(), self.clone(), cancel);

		let cell = Rc::new(DstActorCell {
			actor,
			state: RefCell::new(None),
			queue,
			ctx: ctx.clone(),
			actor_ref: actor_ref.clone(),
		});

		// Register the actor and get its ID.
		let actor_id = {
			let mut actors = self.inner.actors.borrow_mut();
			let id = actors.len();
			actors.push(Some(cell.clone() as Rc<dyn DstProcessable>));
			id
		};

		// Install notify callback: on each send(), increment logical clock
		// and push a ReadyEntry.
		let inner = self.inner.clone();
		actor_ref.set_notify(Box::new(move || {
			let ts = inner.logical_clock.get();
			inner.logical_clock.set(ts + 1);
			inner.ready_queue.borrow_mut().push(ReadyEntry {
				logical_ts: ts,
				actor_id,
			});
		}));

		// Initialize actor synchronously, catching panics.
		match catch_unwind(AssertUnwindSafe(|| cell.actor.init(&ctx))) {
			Ok(initial_state) => {
				*cell.state.borrow_mut() = Some(initial_state);
				self.inner.alive_count.set(self.inner.alive_count.get() + 1);
			}
			Err(payload) => {
				// Init panicked — mark actor dead immediately and stash
				// the panic so step() can report it as StepResult::Panicked.
				cell.mark_dead();
				self.inner.init_panics.borrow_mut().push((actor_id, payload));
			}
		}

		ActorHandle {
			actor_ref,
		}
	}

	/// Spawn an actor on the query pool. In DST, same as [`spawn`].
	pub fn spawn_query<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message> {
		self.spawn(name, actor)
	}

	/// Process one message from the actor with the smallest logical timestamp.
	///
	/// Returns immediately — either processes one message or reports idle.
	pub fn step(&self) -> StepResult {
		// Drain any init panics first (from spawn() calls that caught a panic).
		{
			let mut panics = self.inner.init_panics.borrow_mut();
			if let Some((actor_id, payload)) = panics.pop() {
				return StepResult::Panicked {
					actor_id,
					payload,
				};
			}
		}

		// Drain any post_stop panics (from prior step() calls where post_stop panicked).
		{
			let mut panics = self.inner.post_stop_panics.borrow_mut();
			if let Some((actor_id, payload)) = panics.pop() {
				return StepResult::Panicked {
					actor_id,
					payload,
				};
			}
		}

		loop {
			let entry = self.inner.ready_queue.borrow_mut().pop();
			let entry = match entry {
				Some(e) => e,
				None => return StepResult::Idle,
			};

			let actor_id = entry.actor_id;
			let cell = {
				let actors = self.inner.actors.borrow();
				match actors.get(actor_id) {
					Some(Some(cell)) => cell.clone(),
					_ => continue, // slot cleared
				}
			};

			// Skip dead actors or actors with no pending messages.
			if !cell.is_alive() || !cell.has_pending() {
				continue;
			}

			// Catch panics for reproducibility.
			let result = catch_unwind(AssertUnwindSafe(|| cell.process_one()));

			match result {
				Ok(Some(Directive::Stop)) => {
					if let Err(payload) = catch_unwind(AssertUnwindSafe(|| cell.post_stop())) {
						self.inner.post_stop_panics.borrow_mut().push((actor_id, payload));
					}
					cell.mark_dead();
					self.inner.alive_count.set(self.inner.alive_count.get() - 1);
					return StepResult::Stopped {
						actor_id,
					};
				}
				Ok(Some(_)) => {
					return StepResult::Processed {
						actor_id,
					};
				}
				Ok(None) => {
					// No message was actually available (edge case).
					continue;
				}
				Err(payload) => {
					if let Err(ps_payload) = catch_unwind(AssertUnwindSafe(|| cell.post_stop())) {
						self.inner.post_stop_panics.borrow_mut().push((actor_id, ps_payload));
					}
					cell.mark_dead();
					self.inner.alive_count.set(self.inner.alive_count.get() - 1);
					return StepResult::Panicked {
						actor_id,
						payload,
					};
				}
			}
		}
	}

	/// Advance mock time by `delta`, firing timers in deadline order.
	///
	/// Timer callbacks enqueue messages but do NOT process them.
	/// Call `step()` or `run_until_idle()` afterwards to process.
	pub fn advance_time(&self, delta: Duration) {
		let target_nanos = self.inner.mock_clock.now_nanos() + delta.as_nanos() as u64;

		loop {
			let next_deadline = self.inner.timer_heap.borrow().peek().map(|e| e.deadline_nanos);

			match next_deadline {
				Some(deadline) if deadline <= target_nanos => {
					// Advance clock to this deadline so timers see correct time.
					self.inner.mock_clock.set_nanos(deadline);
					fire_due_timers(&self.inner.timer_heap, deadline);
				}
				_ => break,
			}
		}

		// Set clock to final position.
		self.inner.mock_clock.set_nanos(target_nanos);

		// Propagate time advancement to child scopes.
		for child in self.inner.children.borrow().iter() {
			child.advance_time(delta);
		}
	}

	/// Process messages until no actors have pending messages.
	pub fn run_until_idle(&self) {
		loop {
			match self.step() {
				StepResult::Idle => break,
				_ => {}
			}
		}
	}

	/// Check if any actors have pending messages in the ready queue.
	pub fn has_pending(&self) -> bool {
		!self.inner.ready_queue.borrow().is_empty()
	}

	/// Get the number of alive actors.
	pub fn alive_count(&self) -> usize {
		self.inner.alive_count.get()
	}
}

impl fmt::Debug for ActorSystem {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorSystem")
			.field("cancelled", &self.is_cancelled())
			.field("alive_count", &self.alive_count())
			.finish_non_exhaustive()
	}
}

/// Handle to a spawned actor.
pub struct ActorHandle<M> {
	pub actor_ref: ActorRef<M>,
}

impl<M> ActorHandle<M> {
	/// Get the actor reference for sending messages.
	pub fn actor_ref(&self) -> &ActorRef<M> {
		&self.actor_ref
	}

	/// Wait for the actor to complete (no-op in DST).
	pub fn join(self) -> Result<(), JoinError> {
		Ok(())
	}
}

/// Error returned when joining an actor fails.
#[derive(Debug)]
pub struct JoinError {
	message: String,
}

impl JoinError {
	/// Create a new JoinError with a message.
	pub fn new(message: impl Into<String>) -> Self {
		Self {
			message: message.into(),
		}
	}
}

impl fmt::Display for JoinError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "actor join failed: {}", self.message)
	}
}

impl error::Error for JoinError {}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex};

	use super::*;
	use crate::pool::{PoolConfig, Pools};

	struct CounterActor;

	#[derive(Debug)]
	enum CounterMessage {
		Inc,
		Dec,
		Stop,
	}

	impl Actor for CounterActor {
		type State = i64;
		type Message = CounterMessage;

		fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
			0
		}

		fn handle(
			&self,
			state: &mut Self::State,
			msg: Self::Message,
			_ctx: &Context<Self::Message>,
		) -> Directive {
			match msg {
				CounterMessage::Inc => *state += 1,
				CounterMessage::Dec => *state -= 1,
				CounterMessage::Stop => return Directive::Stop,
			}
			Directive::Continue
		}
	}

	/// Actor that records the order in which it receives messages.
	struct OrderActor;

	#[derive(Debug, Clone)]
	struct OrderMessage(u64);

	impl Actor for OrderActor {
		type State = Vec<u64>;
		type Message = OrderMessage;

		fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
			Vec::new()
		}

		fn handle(
			&self,
			state: &mut Self::State,
			msg: Self::Message,
			_ctx: &Context<Self::Message>,
		) -> Directive {
			state.push(msg.0);
			Directive::Continue
		}
	}

	/// Actor that panics on a specific message.
	struct PanicActor;

	#[derive(Debug)]
	enum PanicMessage {
		Ok,
		Boom,
	}

	impl Actor for PanicActor {
		type State = u64;
		type Message = PanicMessage;

		fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
			0
		}

		fn handle(
			&self,
			state: &mut Self::State,
			msg: Self::Message,
			_ctx: &Context<Self::Message>,
		) -> Directive {
			match msg {
				PanicMessage::Ok => {
					*state += 1;
					Directive::Continue
				}
				PanicMessage::Boom => panic!("actor boom"),
			}
		}
	}

	/// Actor that forwards received messages to a shared log.
	///
	/// Uses `Arc<Mutex<..>>` to satisfy `Send + Sync` bounds on `Actor`.
	struct LogActor {
		log: Arc<Mutex<Vec<String>>>,
	}

	// SAFETY: DST is single-threaded. The Mutex is never contended.
	impl Actor for LogActor {
		type State = ();
		type Message = String;

		fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

		fn handle(
			&self,
			_state: &mut Self::State,
			msg: Self::Message,
			_ctx: &Context<Self::Message>,
		) -> Directive {
			self.log.lock().unwrap().push(msg);
			Directive::Continue
		}
	}

	/// Actor that sends to another actor when it receives a message.
	struct ForwardActor {
		target: ActorRef<String>,
	}

	impl Actor for ForwardActor {
		type State = ();
		type Message = String;

		fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

		fn handle(
			&self,
			_state: &mut Self::State,
			msg: Self::Message,
			_ctx: &Context<Self::Message>,
		) -> Directive {
			let _ = self.target.send(format!("fwd:{msg}"));
			Directive::Continue
		}
	}

	fn test_system() -> ActorSystem {
		let pools = Pools::new(PoolConfig::default());
		ActorSystem::new(pools, Clock::Mock(MockClock::from_millis(0)))
	}

	#[test]
	fn test_basic_step() {
		let system = test_system();
		let handle = system.spawn("counter", CounterActor);

		handle.actor_ref.send(CounterMessage::Inc).unwrap();
		handle.actor_ref.send(CounterMessage::Inc).unwrap();
		handle.actor_ref.send(CounterMessage::Inc).unwrap();

		// Step three times.
		for _ in 0..3 {
			match system.step() {
				StepResult::Processed {
					actor_id: 0,
				} => {}
				other => panic!("unexpected: {other:?}"),
			}
		}

		// Should be idle now.
		assert!(matches!(system.step(), StepResult::Idle));
	}

	#[test]
	fn test_strict_global_fifo() {
		let system = test_system();

		let log = Arc::new(Mutex::new(Vec::<String>::new()));

		let a = system.spawn(
			"a",
			LogActor {
				log: log.clone(),
			},
		);
		let b = system.spawn(
			"b",
			LogActor {
				log: log.clone(),
			},
		);

		// Send in interleaved order: a gets msg1 and msg2, b gets msg3.
		a.actor_ref.send("msg1".into()).unwrap();
		a.actor_ref.send("msg2".into()).unwrap();
		b.actor_ref.send("msg3".into()).unwrap();

		system.run_until_idle();

		// Strict global FIFO: msg1 (ts=0), msg2 (ts=1), msg3 (ts=2).
		assert_eq!(*log.lock().unwrap(), vec!["msg1", "msg2", "msg3"]);
	}

	#[test]
	fn test_timer_advance() {
		let system = test_system();
		let handle = system.spawn("order", OrderActor);

		// Schedule a timer for 100ms.
		let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
		ctx.schedule_once(Duration::from_millis(100), || OrderMessage(42));

		// No messages yet.
		assert!(matches!(system.step(), StepResult::Idle));

		// Advance time past the deadline.
		system.advance_time(Duration::from_millis(100));

		// Now the timer-fired message should be processable.
		match system.step() {
			StepResult::Processed {
				actor_id: 0,
			} => {}
			other => panic!("unexpected: {other:?}"),
		}
		assert!(matches!(system.step(), StepResult::Idle));
	}

	#[test]
	fn test_timer_deadline_ordering() {
		let system = test_system();
		let log = Arc::new(Mutex::new(Vec::<String>::new()));
		let handle = system.spawn(
			"log",
			LogActor {
				log: log.clone(),
			},
		);

		let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());

		// Schedule timers in reverse deadline order.
		ctx.schedule_once(Duration::from_millis(300), || "t300".into());
		ctx.schedule_once(Duration::from_millis(100), || "t100".into());
		ctx.schedule_once(Duration::from_millis(200), || "t200".into());

		// Advance past all deadlines.
		system.advance_time(Duration::from_millis(300));
		system.run_until_idle();

		// Timers should have fired in deadline order.
		assert_eq!(*log.lock().unwrap(), vec!["t100", "t200", "t300"]);
	}

	#[test]
	fn test_timer_repeat() {
		let system = test_system();
		let log = Arc::new(Mutex::new(Vec::<String>::new()));
		let handle = system.spawn(
			"log",
			LogActor {
				log: log.clone(),
			},
		);

		let ctx = Context::new(handle.actor_ref.clone(), system.clone(), system.cancellation_token());
		ctx.schedule_repeat(Duration::from_millis(100), "tick".to_string());

		// Advance 350ms — should fire at 100, 200, 300.
		system.advance_time(Duration::from_millis(350));
		system.run_until_idle();

		assert_eq!(log.lock().unwrap().len(), 3);
	}

	#[test]
	fn test_run_until_idle_with_forwarding() {
		let system = test_system();
		let log = Arc::new(Mutex::new(Vec::<String>::new()));
		let log_handle = system.spawn(
			"log",
			LogActor {
				log: log.clone(),
			},
		);
		let fwd_handle = system.spawn(
			"fwd",
			ForwardActor {
				target: log_handle.actor_ref.clone(),
			},
		);

		fwd_handle.actor_ref.send("hello".into()).unwrap();
		system.run_until_idle();

		assert_eq!(*log.lock().unwrap(), vec!["fwd:hello"]);
	}

	#[test]
	fn test_panic_handling() {
		let system = test_system();
		let handle = system.spawn("panic", PanicActor);

		handle.actor_ref.send(PanicMessage::Ok).unwrap();
		handle.actor_ref.send(PanicMessage::Boom).unwrap();

		// First message succeeds.
		assert!(matches!(
			system.step(),
			StepResult::Processed {
				actor_id: 0
			}
		));

		// Second message panics.
		match system.step() {
			StepResult::Panicked {
				actor_id: 0,
				..
			} => {}
			other => panic!("expected Panicked, got {other:?}"),
		}

		// Actor is dead, further sends should fail.
		assert!(handle.actor_ref.send(PanicMessage::Ok).is_err());
		assert_eq!(system.alive_count(), 0);
	}

	#[test]
	fn test_actor_lifecycle_stop() {
		let system = test_system();
		let handle = system.spawn("counter", CounterActor);

		handle.actor_ref.send(CounterMessage::Inc).unwrap();
		handle.actor_ref.send(CounterMessage::Stop).unwrap();
		handle.actor_ref.send(CounterMessage::Inc).unwrap(); // queued before stop processed

		assert!(matches!(
			system.step(),
			StepResult::Processed {
				actor_id: 0
			}
		));
		assert!(matches!(
			system.step(),
			StepResult::Stopped {
				actor_id: 0
			}
		));

		// Third message's ReadyEntry is still in the queue, but step() should skip it.
		assert!(matches!(system.step(), StepResult::Idle));

		// Further sends fail.
		assert!(handle.actor_ref.send(CounterMessage::Inc).is_err());
		assert_eq!(system.alive_count(), 0);
	}

	#[test]
	fn test_multiple_actors_fifo() {
		let system = test_system();
		let log = Arc::new(Mutex::new(Vec::<String>::new()));

		let a = system.spawn(
			"a",
			LogActor {
				log: log.clone(),
			},
		);
		let b = system.spawn(
			"b",
			LogActor {
				log: log.clone(),
			},
		);
		let c = system.spawn(
			"c",
			LogActor {
				log: log.clone(),
			},
		);

		// Interleave sends across three actors.
		a.actor_ref.send("a1".into()).unwrap();
		b.actor_ref.send("b1".into()).unwrap();
		c.actor_ref.send("c1".into()).unwrap();
		a.actor_ref.send("a2".into()).unwrap();
		b.actor_ref.send("b2".into()).unwrap();

		system.run_until_idle();

		assert_eq!(*log.lock().unwrap(), vec!["a1", "b1", "c1", "a2", "b2"]);
	}

	// Allow debug formatting for StepResult in test assertions.
	impl fmt::Debug for StepResult {
		fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
			match self {
				StepResult::Processed {
					actor_id,
				} => write!(f, "Processed {{ actor_id: {actor_id} }}"),
				StepResult::Panicked {
					actor_id,
					..
				} => write!(f, "Panicked {{ actor_id: {actor_id} }}"),
				StepResult::Stopped {
					actor_id,
				} => write!(f, "Stopped {{ actor_id: {actor_id} }}"),
				StepResult::Idle => write!(f, "Idle"),
			}
		}
	}
}
