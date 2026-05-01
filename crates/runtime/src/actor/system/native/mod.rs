// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

mod pool;

use std::{
	any::Any,
	error, fmt,
	fmt::{Debug, Formatter},
	mem,
	sync::{Arc, Mutex},
	time,
	time::Duration,
};

use crossbeam_channel::{Receiver, RecvTimeoutError as CcRecvTimeoutError};

use crate::{
	actor::{
		context::CancellationToken, system::native::pool::PoolActorHandle, timers::scheduler::SchedulerHandle,
		traits::Actor,
	},
	context::clock::Clock,
	pool::Pools,
};

/// Inner shared state for the actor system.
struct ActorSystemInner {
	cancel: CancellationToken,
	scheduler: SchedulerHandle,
	clock: Clock,
	pools: Pools,
	wakers: Mutex<Vec<Arc<dyn Fn() + Send + Sync>>>,
	keepalive: Mutex<Vec<Box<dyn Any + Send + Sync>>>,
	done_rxs: Mutex<Vec<Receiver<()>>>,
	children: Mutex<Vec<ActorSystem>>,
}

/// Unified system for all concurrent work.
///
/// Provides:
/// - Actor spawning on a shared work-stealing pool
/// - CPU-bound compute with admission control
/// - Graceful shutdown via cancellation token
#[derive(Clone)]
pub struct ActorSystem {
	inner: Arc<ActorSystemInner>,
}

impl ActorSystem {
	/// Create a new actor system with the given pools and clock.
	pub fn new(pools: Pools, clock: Clock) -> Self {
		let scheduler = SchedulerHandle::new(pools.system_pool().clone());

		Self {
			inner: Arc::new(ActorSystemInner {
				cancel: CancellationToken::new(),
				scheduler,
				clock,
				pools,
				wakers: Mutex::new(Vec::new()),
				keepalive: Mutex::new(Vec::new()),
				done_rxs: Mutex::new(Vec::new()),
				children: Mutex::new(Vec::new()),
			}),
		}
	}

	pub fn scope(&self) -> Self {
		let child = Self {
			inner: Arc::new(ActorSystemInner {
				cancel: self.inner.cancel.child_token(),
				scheduler: self.inner.scheduler.shared(),
				clock: self.inner.clock.clone(),
				pools: self.inner.pools.clone(),
				wakers: Mutex::new(Vec::new()),
				keepalive: Mutex::new(Vec::new()),
				done_rxs: Mutex::new(Vec::new()),
				children: Mutex::new(Vec::new()),
			}),
		};
		self.inner.children.lock().unwrap().push(child.clone());
		child
	}

	/// Get the pools for this system.
	pub fn pools(&self) -> Pools {
		self.inner.pools.clone()
	}

	/// Get the cancellation token for this system.
	pub fn cancellation_token(&self) -> CancellationToken {
		self.inner.cancel.clone()
	}

	/// Check if the system has been cancelled.
	pub fn is_cancelled(&self) -> bool {
		self.inner.cancel.is_cancelled()
	}

	/// Signal shutdown to all actors and the timer scheduler.
	///
	/// Cancels all actors, wakes any that are parked, then drops the waker
	/// and keepalive references so actor cells can be freed.
	pub fn shutdown(&self) {
		self.inner.cancel.cancel();

		// Propagate shutdown to child scopes.
		for child in self.inner.children.lock().unwrap().iter() {
			child.shutdown();
		}

		// Drain wakers: wake all parked actors and release the closures in one step.
		let wakers = mem::take(&mut *self.inner.wakers.lock().unwrap());
		for waker in &wakers {
			waker();
		}
		drop(wakers);

		// Release keepalive references so actor cells can be freed.
		self.inner.keepalive.lock().unwrap().clear();
	}

	/// Register a waker to be called on shutdown.
	pub(crate) fn register_waker(&self, f: Arc<dyn Fn() + Send + Sync>) {
		self.inner.wakers.lock().unwrap().push(f);
	}

	/// Register an actor cell to be kept alive while the system is running.
	///
	/// Cleared on shutdown so actor cells can be freed.
	pub(crate) fn register_keepalive(&self, cell: Box<dyn Any + Send + Sync>) {
		self.inner.keepalive.lock().unwrap().push(cell);
	}

	/// Register a done receiver for an actor, used by `join()` to wait for all actors.
	pub(crate) fn register_done_rx(&self, rx: Receiver<()>) {
		self.inner.done_rxs.lock().unwrap().push(rx);
	}

	/// Wait for all actors to finish after shutdown, with a default 5-second timeout.
	pub fn join(&self) -> Result<(), JoinError> {
		self.join_timeout(Duration::from_secs(5))
	}

	/// Wait for all actors to finish after shutdown, with a custom timeout.
	#[allow(clippy::disallowed_methods)]
	pub fn join_timeout(&self, timeout: Duration) -> Result<(), JoinError> {
		let deadline = time::Instant::now() + timeout;
		let rxs: Vec<_> = mem::take(&mut *self.inner.done_rxs.lock().unwrap());
		for rx in rxs {
			let remaining = deadline.saturating_duration_since(time::Instant::now());
			match rx.recv_timeout(remaining) {
				Ok(()) => {}
				Err(CcRecvTimeoutError::Disconnected) => {
					// Cell dropped without sending - actor already cleaned up
				}
				Err(CcRecvTimeoutError::Timeout) => {
					return Err(JoinError::new("timed out waiting for actors to stop"));
				}
			}
		}
		Ok(())
	}

	/// Get the timer scheduler for scheduling delayed/periodic callbacks.
	pub fn scheduler(&self) -> &SchedulerHandle {
		&self.inner.scheduler
	}

	/// Get the clock for this system.
	pub fn clock(&self) -> &Clock {
		&self.inner.clock
	}

	/// Spawn an actor on the system pool.
	///
	/// Use this for lightweight actors that must never stall
	/// (flow, CDC, watermark, metrics, etc.).
	pub fn spawn_system<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message>
	where
		A::State: Send,
	{
		pool::spawn_on_pool(self, name, actor, self.inner.pools.system_pool())
	}

	/// Spawn an actor on the query pool.
	///
	/// Use this for execution-heavy actors that may block on engine calls
	/// (WS, gRPC, HTTP server actors).
	pub fn spawn_query<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message>
	where
		A::State: Send,
	{
		pool::spawn_on_pool(self, name, actor, self.inner.pools.query_pool())
	}
}

impl Debug for ActorSystem {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorSystem").field("cancelled", &self.is_cancelled()).finish_non_exhaustive()
	}
}

/// Handle to a spawned actor.
pub type ActorHandle<M> = PoolActorHandle<M>;

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
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "actor join failed: {}", self.message)
	}
}

impl error::Error for JoinError {}

#[cfg(test)]
mod tests {
	use std::sync;

	use super::*;
	use crate::{
		actor::{context::Context, traits::Directive},
		pool::{PoolConfig, Pools},
	};

	fn test_system() -> ActorSystem {
		let pools = Pools::new(PoolConfig::default());
		ActorSystem::new(pools, Clock::Real)
	}

	struct CounterActor;

	#[derive(Debug)]
	enum CounterMessage {
		Inc,
		Get(sync::mpsc::Sender<i64>),
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
				CounterMessage::Get(tx) => {
					let _ = tx.send(*state);
				}
				CounterMessage::Stop => return Directive::Stop,
			}
			Directive::Continue
		}
	}

	#[test]
	fn test_spawn_and_send() {
		let system = test_system();
		let handle = system.spawn_system("counter", CounterActor);

		let actor_ref = handle.actor_ref().clone();
		actor_ref.send(CounterMessage::Inc).unwrap();
		actor_ref.send(CounterMessage::Inc).unwrap();
		actor_ref.send(CounterMessage::Inc).unwrap();

		let (tx, rx) = sync::mpsc::channel();
		actor_ref.send(CounterMessage::Get(tx)).unwrap();

		let value = rx.recv().unwrap();
		assert_eq!(value, 3);

		actor_ref.send(CounterMessage::Stop).unwrap();
		handle.join().unwrap();
	}

	#[test]
	fn test_shutdown_join() {
		let system = test_system();

		// Spawn several actors
		for i in 0..5 {
			system.spawn_system(&format!("counter-{i}"), CounterActor);
		}

		// Shutdown cancels all actors; join waits for them to finish
		system.shutdown();
		system.join().unwrap();
	}
}
