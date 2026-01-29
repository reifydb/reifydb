// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Native actor system implementation.
//!
//! Uses rayon for all actors on a shared work-stealing pool.

mod pool;

use std::{
	fmt::{Debug, Formatter},
	sync::Arc,
	thread::available_parallelism,
};

pub use pool::PoolActorHandle;
use rayon::{ThreadPool, ThreadPoolBuilder};
use tokio::{sync::Semaphore, task};

use crate::actor::{context::CancellationToken, timers::scheduler::SchedulerHandle, traits::Actor};

/// Configuration for the actor system.
#[derive(Debug, Clone)]
pub struct ActorSystemConfig {
	/// Number of worker threads in the shared rayon pool.
	pub pool_threads: usize,
	/// Maximum concurrent compute tasks (admission control).
	pub max_in_flight: usize,
}

impl Default for ActorSystemConfig {
	fn default() -> Self {
		Self {
			pool_threads: available_parallelism().map(|p| p.get()).unwrap_or(4),
			max_in_flight: 32,
		}
	}
}

impl ActorSystemConfig {
	/// Set the number of pool threads.
	pub fn pool_threads(mut self, threads: usize) -> Self {
		self.pool_threads = threads;
		self
	}

	/// Set the maximum number of in-flight compute tasks.
	pub fn max_in_flight(mut self, max: usize) -> Self {
		self.max_in_flight = max;
		self
	}
}

/// Inner shared state for the actor system.
struct ActorSystemInner {
	pool: Arc<ThreadPool>,
	permits: Arc<Semaphore>,
	cancel: CancellationToken,
	scheduler: SchedulerHandle,
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
	/// Create a new actor system with the given configuration.
	pub fn new(config: ActorSystemConfig) -> Self {
		let pool = Arc::new(
			ThreadPoolBuilder::new()
				.num_threads(config.pool_threads)
				.thread_name(|i| format!("actor-pool-{i}"))
				.build()
				.expect("failed to build rayon pool"),
		);

		let scheduler = SchedulerHandle::new(pool.clone());

		Self {
			inner: Arc::new(ActorSystemInner {
				pool,
				permits: Arc::new(Semaphore::new(config.max_in_flight)),
				cancel: CancellationToken::new(),
				scheduler,
			}),
		}
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
	pub fn shutdown(&self) {
		self.inner.cancel.cancel();
	}

	/// Get the timer scheduler for scheduling delayed/periodic callbacks.
	pub fn scheduler(&self) -> &SchedulerHandle {
		&self.inner.scheduler
	}

	/// Spawn an actor on the shared work-stealing pool.
	///
	/// Returns a handle to the spawned actor.
	pub fn spawn<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message>
	where
		A::State: Send,
	{
		pool::spawn_on_pool(self, name, actor)
	}

	/// Executes a closure on the rayon thread pool directly.
	///
	/// Synchronous and bypasses admission control.
	/// Use this when you're already in a synchronous context and need parallel execution.
	pub fn install<R, F>(&self, f: F) -> R
	where
		R: Send,
		F: FnOnce() -> R + Send,
	{
		self.inner.pool.install(f)
	}

	/// Runs a CPU-bound function on the compute pool.
	///
	/// The task is scheduled via `spawn_blocking` and executed on the
	/// dedicated rayon pool using `install`. Admission control ensures
	/// no more than `max_in_flight` tasks run concurrently.
	pub async fn compute<R, F>(&self, f: F) -> Result<R, task::JoinError>
	where
		R: Send + 'static,
		F: FnOnce() -> R + Send + 'static,
	{
		let permit = self.inner.permits.clone().acquire_owned().await.expect("semaphore closed");
		let inner = self.inner.clone();

		let handle = task::spawn_blocking(move || {
			let _permit = permit; // released when closure returns
			inner.pool.install(f)
		});

		handle.await
	}

	/// Get direct access to the rayon pool (for advanced use cases).
	pub(crate) fn pool(&self) -> &Arc<ThreadPool> {
		&self.inner.pool
	}
}

impl Debug for ActorSystem {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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

impl std::fmt::Display for JoinError {
	fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
		write!(f, "actor join failed: {}", self.message)
	}
}

impl std::error::Error for JoinError {}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::actor::{context::Context, traits::Flow};

	struct CounterActor;

	#[derive(Debug)]
	enum CounterMsg {
		Inc,
		Get(std::sync::mpsc::Sender<i64>),
		Stop,
	}

	impl Actor for CounterActor {
		type State = i64;
		type Message = CounterMsg;

		fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
			0
		}

		fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Flow {
			match msg {
				CounterMsg::Inc => *state += 1,
				CounterMsg::Get(tx) => {
					let _ = tx.send(*state);
				}
				CounterMsg::Stop => return Flow::Stop,
			}
			Flow::Continue
		}
	}

	#[test]
	fn test_spawn_and_send() {
		let system = ActorSystem::new(ActorSystemConfig::default());
		let handle = system.spawn("counter", CounterActor);

		let actor_ref = handle.actor_ref().clone();
		actor_ref.send(CounterMsg::Inc).unwrap();
		actor_ref.send(CounterMsg::Inc).unwrap();
		actor_ref.send(CounterMsg::Inc).unwrap();

		let (tx, rx) = std::sync::mpsc::channel();
		actor_ref.send(CounterMsg::Get(tx)).unwrap();

		let value = rx.recv().unwrap();
		assert_eq!(value, 3);

		actor_ref.send(CounterMsg::Stop).unwrap();
		handle.join().unwrap();
	}

	#[test]
	fn test_install() {
		let system = ActorSystem::new(ActorSystemConfig::default());
		let result = system.install(|| 42);
		assert_eq!(result, 42);
	}

	#[tokio::test]
	async fn test_compute() {
		let system = ActorSystem::new(ActorSystemConfig::default());
		let result = system.compute(|| 42).await.unwrap();
		assert_eq!(result, 42);
	}
}
