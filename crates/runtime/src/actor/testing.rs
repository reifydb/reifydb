// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::VecDeque, marker::PhantomData};

#[cfg(not(reifydb_single_threaded))]
use crossbeam_channel::unbounded;

#[cfg(not(reifydb_single_threaded))]
use crate::actor::mailbox::ActorRef;
#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
use crate::actor::mailbox::create_actor_ref;
#[cfg(reifydb_target = "dst")]
use crate::actor::mailbox::create_dst_mailbox;
#[cfg(reifydb_target = "dst")]
use crate::context::clock::MockClock;
use crate::{
	actor::{
		context::{CancellationToken, Context},
		system::ActorSystem,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};

pub struct TestHarness<A: Actor> {
	actor: A,
	state: A::State,
	mailbox: VecDeque<A::Message>,
	ctx: TestContext<A::Message>,
}

impl<A: Actor> TestHarness<A> {
	pub fn new(actor: A) -> Self {
		let ctx = TestContext::new();
		let state = actor.init(&ctx.to_context());

		Self {
			actor,
			state,
			mailbox: VecDeque::new(),
			ctx,
		}
	}

	pub fn with_state(actor: A, state: A::State) -> Self {
		let ctx = TestContext::new();

		Self {
			actor,
			state,
			mailbox: VecDeque::new(),
			ctx,
		}
	}

	pub fn send(&mut self, msg: A::Message) {
		self.mailbox.push_back(msg);
	}

	pub fn process_one(&mut self) -> Option<Directive> {
		let msg = self.mailbox.pop_front()?;
		let flow = self.actor.handle(&mut self.state, msg, &self.ctx.to_context());
		Some(flow)
	}

	pub fn process_all(&mut self) -> Vec<Directive> {
		let mut flows = Vec::new();

		while let Some(flow) = self.process_one() {
			flows.push(flow);
			if flow == Directive::Stop {
				break;
			}
		}

		flows
	}

	pub fn process_until<F>(&mut self, mut condition: F) -> Vec<Directive>
	where
		F: FnMut(&A::State) -> bool,
	{
		let mut flows = Vec::new();

		while !self.mailbox.is_empty() {
			if condition(&self.state) {
				break;
			}

			if let Some(flow) = self.process_one() {
				flows.push(flow);
				if flow == Directive::Stop {
					break;
				}
			}
		}

		flows
	}

	pub fn idle(&mut self) -> Directive {
		self.actor.idle(&self.ctx.to_context())
	}

	pub fn post_stop(&mut self) {
		self.actor.post_stop();
	}

	pub fn state(&self) -> &A::State {
		&self.state
	}

	pub fn state_mut(&mut self) -> &mut A::State {
		&mut self.state
	}

	pub fn is_empty(&self) -> bool {
		self.mailbox.is_empty()
	}

	pub fn mailbox_len(&self) -> usize {
		self.mailbox.len()
	}

	pub fn cancel(&mut self) {
		self.ctx.cancel();
	}

	pub fn is_cancelled(&self) -> bool {
		self.ctx.is_cancelled()
	}
}

struct TestContext<M> {
	cancel: CancellationToken,
	_marker: PhantomData<M>,
}

impl<M: Send + 'static> TestContext<M> {
	fn new() -> Self {
		Self {
			cancel: CancellationToken::new(),
			_marker: PhantomData,
		}
	}

	fn cancel(&self) {
		self.cancel.cancel();
	}

	fn is_cancelled(&self) -> bool {
		self.cancel.is_cancelled()
	}

	fn to_context(&self) -> Context<M> {
		#[cfg(not(reifydb_single_threaded))]
		let actor_ref = {
			let (tx, _rx) = unbounded();
			ActorRef::new(tx)
		};

		#[cfg(all(reifydb_single_threaded, not(reifydb_target = "dst")))]
		let actor_ref = create_actor_ref();

		#[cfg(reifydb_target = "dst")]
		let actor_ref = {
			let (actor_ref, _queue) = create_dst_mailbox();
			actor_ref
		};

		let pools = Pools::new(PoolConfig::default());

		#[cfg(reifydb_target = "dst")]
		let clock = Clock::Mock(MockClock::new(0));
		#[cfg(not(reifydb_target = "dst"))]
		let clock = Clock::Real;

		let system = ActorSystem::new(pools, clock);

		Context::new(actor_ref, system, self.cancel.clone())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	struct CounterActor;

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
				CounterMessage::Set(v) => *state = v,
				CounterMessage::Stop => return Directive::Stop,
			}
			Directive::Continue
		}

		fn idle(&self, _ctx: &Context<Self::Message>) -> Directive {
			Directive::Park
		}
	}

	#[derive(Debug)]
	enum CounterMessage {
		Inc,
		Dec,
		Set(i64),
		Stop,
	}

	#[test]
	fn test_harness_basic() {
		let mut harness = TestHarness::new(CounterActor);

		harness.send(CounterMessage::Inc);
		harness.send(CounterMessage::Inc);
		harness.send(CounterMessage::Inc);

		assert_eq!(harness.mailbox_len(), 3);

		let flows = harness.process_all();

		assert_eq!(flows.len(), 3);
		assert!(flows.iter().all(|f| *f == Directive::Continue));
		assert_eq!(*harness.state(), 3);
	}

	#[test]
	fn test_harness_stops_on_stop() {
		let mut harness = TestHarness::new(CounterActor);

		harness.send(CounterMessage::Inc);
		harness.send(CounterMessage::Stop);
		harness.send(CounterMessage::Inc); // Should not be processed

		let flows = harness.process_all();

		assert_eq!(flows.len(), 2);
		assert_eq!(flows[1], Directive::Stop);
		assert_eq!(*harness.state(), 1);
		assert_eq!(harness.mailbox_len(), 1); // One message left
	}

	#[test]
	fn test_harness_process_one() {
		let mut harness = TestHarness::new(CounterActor);

		harness.send(CounterMessage::Set(42));
		harness.send(CounterMessage::Inc);

		assert_eq!(harness.process_one(), Some(Directive::Continue));
		assert_eq!(*harness.state(), 42);

		assert_eq!(harness.process_one(), Some(Directive::Continue));
		assert_eq!(*harness.state(), 43);

		assert_eq!(harness.process_one(), None);
	}

	#[test]
	fn test_harness_idle() {
		let mut harness = TestHarness::new(CounterActor);

		let flow = harness.idle();
		assert_eq!(flow, Directive::Park);
	}
}
