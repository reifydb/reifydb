// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	error, fmt,
	fmt::{Debug, Formatter},
	sync::{Arc, Weak},
};

use crate::{
	actor::{
		context::CancellationToken,
		mailbox::{ActorRef, create_mailbox},
		traits::Actor,
	},
	context::clock::Clock,
	pool::Pools,
	sync::mutex::Mutex,
};

struct ActorSystemInner {
	cancel: CancellationToken,
	clock: Clock,
	parked: Mutex<Vec<Box<dyn Any + Send + Sync>>>,
}

#[derive(Clone)]
pub struct ActorSystem {
	inner: Arc<ActorSystemInner>,
}

impl ActorSystem {
	pub fn new(_pools: Pools, clock: Clock) -> Self {
		Self::with_clock(clock)
	}

	pub fn testing(clock: Clock) -> Self {
		Self::with_clock(clock)
	}

	fn with_clock(clock: Clock) -> Self {
		Self {
			inner: Arc::new(ActorSystemInner {
				cancel: CancellationToken::new(),
				clock,
				parked: Mutex::new(Vec::new()),
			}),
		}
	}

	pub fn spawner(&self) -> ActorSpawner {
		ActorSpawner {
			inner: Arc::downgrade(&self.inner),
		}
	}

	pub fn shutdown(&self) {
		self.inner.cancel.cancel();
		self.inner.parked.lock().clear();
	}

	pub fn join(&self) -> Result<(), JoinError> {
		Ok(())
	}

	pub fn clock(&self) -> &Clock {
		&self.inner.clock
	}

	pub fn spawn_coordination<A: Actor>(&self, _name: &str, actor: A) -> ActorHandle<A::Message>
	where
		A::State: Send,
	{
		self.park(actor)
	}

	pub fn spawn_maintenance<A: Actor>(&self, _name: &str, actor: A) -> ActorHandle<A::Message>
	where
		A::State: Send,
	{
		self.park(actor)
	}

	fn park<A: Actor>(&self, actor: A) -> ActorHandle<A::Message> {
		let (actor_ref, mailbox) = create_mailbox(actor.config().mailbox_capacity);
		self.inner.parked.lock().push(Box::new((actor, mailbox)));
		ActorHandle {
			actor_ref,
		}
	}
}

impl Debug for ActorSystem {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorSystem")
			.field("cancelled", &self.inner.cancel.is_cancelled())
			.finish_non_exhaustive()
	}
}

#[derive(Clone)]
pub struct ActorSpawner {
	inner: Weak<ActorSystemInner>,
}

impl ActorSpawner {
	fn system(&self) -> ActorSystem {
		ActorSystem {
			inner: self.inner.upgrade().expect("runtime already shut down: cannot spawn actor"),
		}
	}

	pub fn cancellation_token(&self) -> Option<CancellationToken> {
		self.inner.upgrade().map(|inner| inner.cancel.clone())
	}

	pub fn spawn_coordination<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message>
	where
		A::State: Send,
	{
		self.system().spawn_coordination(name, actor)
	}

	pub fn spawn_maintenance<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message>
	where
		A::State: Send,
	{
		self.system().spawn_maintenance(name, actor)
	}
}

impl Debug for ActorSpawner {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorSpawner").field("alive", &(self.inner.strong_count() > 0)).finish_non_exhaustive()
	}
}

pub struct ActorHandle<M> {
	pub actor_ref: ActorRef<M>,
}

impl<M> ActorHandle<M> {
	pub fn actor_ref(&self) -> &ActorRef<M> {
		&self.actor_ref
	}
}

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
