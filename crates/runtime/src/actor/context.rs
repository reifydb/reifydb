// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	time::Duration,
};

#[cfg(reifydb_target = "dst")]
use crate::actor::timers::dst as dst_timers;
#[cfg(reifydb_target = "wasi")]
use crate::actor::timers::wasi::{schedule_once_fn, schedule_repeat, schedule_repeat_fn};
#[cfg(reifydb_target = "wasm")]
use crate::actor::timers::wasm::{schedule_once_fn, schedule_repeat, schedule_repeat_fn};
use crate::actor::{mailbox::ActorRef, system::ActorSystem, timers::TimerHandle};

#[derive(Clone)]
pub struct CancellationToken {
	cancelled: Arc<AtomicBool>,
	parent: Option<Arc<AtomicBool>>,
}

impl CancellationToken {
	pub fn new() -> Self {
		Self {
			cancelled: Arc::new(AtomicBool::new(false)),
			parent: None,
		}
	}

	pub fn child_token(&self) -> Self {
		Self {
			cancelled: Arc::new(AtomicBool::new(false)),
			parent: Some(Arc::clone(&self.cancelled)),
		}
	}

	pub fn cancel(&self) {
		self.cancelled.store(true, Ordering::SeqCst);
	}

	pub fn is_cancelled(&self) -> bool {
		self.cancelled.load(Ordering::SeqCst) || self.parent.as_ref().is_some_and(|p| p.load(Ordering::SeqCst))
	}
}

impl Default for CancellationToken {
	fn default() -> Self {
		Self::new()
	}
}

pub struct Context<M> {
	self_ref: ActorRef<M>,
	system: ActorSystem,
	cancel: CancellationToken,
}

impl<M: Send + 'static> Context<M> {
	pub fn new(self_ref: ActorRef<M>, system: ActorSystem, cancel: CancellationToken) -> Self {
		Self {
			self_ref,
			system,
			cancel,
		}
	}

	pub fn self_ref(&self) -> ActorRef<M> {
		self.self_ref.clone()
	}

	pub fn system(&self) -> &ActorSystem {
		&self.system
	}

	pub fn is_cancelled(&self) -> bool {
		self.cancel.is_cancelled()
	}

	pub fn cancellation_token(&self) -> CancellationToken {
		self.cancel.clone()
	}
}

impl<M: Send + 'static> Context<M> {
	#[cfg(not(reifydb_single_threaded))]
	pub fn schedule_once<F: FnOnce() -> M + Send + 'static>(&self, delay: Duration, factory: F) -> TimerHandle {
		let actor_ref = self.self_ref.clone();
		self.system.scheduler().schedule_once(delay, move || {
			let _ = actor_ref.send(factory());
		})
	}

	#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
	pub fn schedule_once<F: FnOnce() -> M + Send + 'static>(&self, delay: Duration, factory: F) -> TimerHandle {
		schedule_once_fn(self.self_ref.clone(), delay, factory)
	}

	#[cfg(reifydb_target = "dst")]
	pub fn schedule_once<F: FnOnce() -> M + Send + 'static>(&self, delay: Duration, factory: F) -> TimerHandle {
		dst_timers::schedule_once_fn(
			self.system.timer_heap(),
			self.system.mock_clock(),
			self.self_ref.clone(),
			delay,
			factory,
		)
	}
}

impl<M: Send + Sync + Clone + 'static> Context<M> {
	#[cfg(not(reifydb_single_threaded))]
	pub fn schedule_repeat(&self, interval: Duration, msg: M) -> TimerHandle {
		let actor_ref = self.self_ref.clone();
		self.system.scheduler().schedule_repeat(interval, move || actor_ref.send(msg.clone()).is_ok())
	}

	#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
	pub fn schedule_repeat(&self, interval: Duration, msg: M) -> TimerHandle {
		schedule_repeat(self.self_ref.clone(), interval, msg)
	}

	#[cfg(reifydb_target = "dst")]
	pub fn schedule_repeat(&self, interval: Duration, msg: M) -> TimerHandle {
		dst_timers::schedule_repeat(
			self.system.timer_heap(),
			self.system.mock_clock(),
			self.self_ref.clone(),
			interval,
			msg,
		)
	}

	#[cfg(not(reifydb_single_threaded))]
	pub fn schedule_repeat_fn<F: Fn() -> M + Send + Sync + 'static>(
		&self,
		interval: Duration,
		factory: F,
	) -> TimerHandle {
		let actor_ref = self.self_ref.clone();
		self.system.scheduler().schedule_repeat(interval, move || actor_ref.send(factory()).is_ok())
	}

	#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
	pub fn schedule_repeat_fn<F: Fn() -> M + Send + Sync + 'static>(
		&self,
		interval: Duration,
		factory: F,
	) -> TimerHandle {
		schedule_repeat_fn(self.self_ref.clone(), interval, factory)
	}

	#[cfg(reifydb_target = "dst")]
	pub fn schedule_repeat_fn<F: Fn() -> M + Send + Sync + 'static>(
		&self,
		interval: Duration,
		factory: F,
	) -> TimerHandle {
		dst_timers::schedule_repeat_fn(
			self.system.timer_heap(),
			self.system.mock_clock(),
			self.self_ref.clone(),
			interval,
			factory,
		)
	}

	pub fn schedule_tick<F: Fn(u64) -> M + Send + Sync + 'static>(
		&self,
		interval: Duration,
		factory: F,
	) -> TimerHandle {
		let actor_ref = self.self_ref.clone();
		let clock = self.system.clock().clone();

		#[cfg(not(reifydb_single_threaded))]
		{
			self.system.scheduler().schedule_repeat(interval, move || {
				let now = clock.now_nanos();
				actor_ref.send(factory(now)).is_ok()
			})
		}

		#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
		{
			schedule_repeat_fn(actor_ref, interval, move || {
				let now = clock.now_nanos();
				factory(now)
			})
		}

		#[cfg(reifydb_target = "dst")]
		{
			dst_timers::schedule_repeat_fn(
				self.system.timer_heap(),
				self.system.mock_clock(),
				actor_ref,
				interval,
				move || {
					let now = clock.now_nanos();
					factory(now)
				},
			)
		}
	}
}

impl<M> Clone for Context<M> {
	fn clone(&self) -> Self {
		Self {
			self_ref: self.self_ref.clone(),
			system: self.system.clone(),
			cancel: self.cancel.clone(),
		}
	}
}
