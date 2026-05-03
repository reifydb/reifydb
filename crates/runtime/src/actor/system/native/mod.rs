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

#[derive(Clone)]
pub struct ActorSystem {
	inner: Arc<ActorSystemInner>,
}

impl ActorSystem {
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

	pub fn pools(&self) -> Pools {
		self.inner.pools.clone()
	}

	pub fn cancellation_token(&self) -> CancellationToken {
		self.inner.cancel.clone()
	}

	pub fn is_cancelled(&self) -> bool {
		self.inner.cancel.is_cancelled()
	}

	pub fn shutdown(&self) {
		self.inner.cancel.cancel();

		for child in self.inner.children.lock().unwrap().iter() {
			child.shutdown();
		}

		let wakers = mem::take(&mut *self.inner.wakers.lock().unwrap());
		for waker in &wakers {
			waker();
		}
		drop(wakers);

		self.inner.keepalive.lock().unwrap().clear();
	}

	pub(crate) fn register_waker(&self, f: Arc<dyn Fn() + Send + Sync>) {
		self.inner.wakers.lock().unwrap().push(f);
	}

	pub(crate) fn register_keepalive(&self, cell: Box<dyn Any + Send + Sync>) {
		self.inner.keepalive.lock().unwrap().push(cell);
	}

	pub(crate) fn register_done_rx(&self, rx: Receiver<()>) {
		self.inner.done_rxs.lock().unwrap().push(rx);
	}

	pub fn join(&self) -> Result<(), JoinError> {
		self.join_timeout(Duration::from_secs(5))
	}

	#[allow(clippy::disallowed_methods)]
	pub fn join_timeout(&self, timeout: Duration) -> Result<(), JoinError> {
		let deadline = time::Instant::now() + timeout;
		let rxs: Vec<_> = mem::take(&mut *self.inner.done_rxs.lock().unwrap());
		for rx in rxs {
			let remaining = deadline.saturating_duration_since(time::Instant::now());
			match rx.recv_timeout(remaining) {
				Ok(()) => {}
				Err(CcRecvTimeoutError::Disconnected) => {}
				Err(CcRecvTimeoutError::Timeout) => {
					return Err(JoinError::new("timed out waiting for actors to stop"));
				}
			}
		}
		Ok(())
	}

	pub fn scheduler(&self) -> &SchedulerHandle {
		&self.inner.scheduler
	}

	pub fn clock(&self) -> &Clock {
		&self.inner.clock
	}

	pub fn spawn_system<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message>
	where
		A::State: Send,
	{
		pool::spawn_on_pool(self, name, actor, self.inner.pools.system_pool())
	}

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

pub type ActorHandle<M> = PoolActorHandle<M>;

#[derive(Debug)]
pub struct JoinError {
	message: String,
}

impl JoinError {
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
