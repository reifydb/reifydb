// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Shared test actors and utilities for DST storage tests.

use std::sync::{Arc, Mutex};

use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::{ActorSystem, dst::StepResult},
		traits::{Actor, Directive},
	},
	context::clock::{Clock, MockClock},
	pool::{PoolConfig, Pools},
};

pub fn test_system() -> ActorSystem {
	let pools = Pools::new(PoolConfig::default());
	ActorSystem::new(pools, Clock::Mock(MockClock::from_millis(0)))
}

pub fn test_system_with_seed(seed: u64) -> ActorSystem {
	let pools = Pools::new(PoolConfig::default());
	ActorSystem::new(pools, Clock::Mock(MockClock::from_millis(seed)))
}

pub type SharedLog = Arc<Mutex<Vec<String>>>;

pub fn new_log() -> SharedLog {
	Arc::new(Mutex::new(Vec::new()))
}

pub fn log_contents(log: &SharedLog) -> Vec<String> {
	log.lock().unwrap().clone()
}

pub struct CounterActor;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum CounterMessage {
	Inc,
	Dec,
	Set(i64),
	Stop,
}

impl Actor for CounterActor {
	type State = i64;
	type Message = CounterMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		0
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			CounterMessage::Inc => *state += 1,
			CounterMessage::Dec => *state -= 1,
			CounterMessage::Set(v) => *state = v,
			CounterMessage::Stop => return Directive::Stop,
		}
		Directive::Continue
	}
}

pub struct LogActor {
	pub log: SharedLog,
}

impl Actor for LogActor {
	type State = ();
	type Message = String;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		self.log.lock().unwrap().push(msg);
		Directive::Continue
	}
}

pub struct ForwardActor {
	pub target: ActorRef<String>,
}

impl Actor for ForwardActor {
	type State = ();
	type Message = String;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		let _ = self.target.send(format!("fwd:{msg}"));
		Directive::Continue
	}
}

pub struct FanOutActor {
	pub targets: Vec<ActorRef<String>>,
}

impl Actor for FanOutActor {
	type State = ();
	type Message = String;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		for (i, target) in self.targets.iter().enumerate() {
			let _ = target.send(format!("{msg}->t{i}"));
		}
		Directive::Continue
	}
}

pub struct PanicActor;

#[derive(Debug)]
#[allow(dead_code)]
pub enum PanicMessage {
	Ok,
	Boom,
}

impl Actor for PanicActor {
	type State = u64;
	type Message = PanicMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		0
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			PanicMessage::Ok => {
				*state += 1;
				Directive::Continue
			}
			PanicMessage::Boom => panic!("actor boom"),
		}
	}
}

pub struct PostStopActor {
	pub stopped: Arc<Mutex<bool>>,
}

impl Actor for PostStopActor {
	type State = ();
	type Message = PostStopMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			PostStopMessage::Stop => Directive::Stop,
			PostStopMessage::Boom => panic!("post_stop test boom"),
			PostStopMessage::Noop => Directive::Continue,
		}
	}

	fn post_stop(&self) {
		*self.stopped.lock().unwrap() = true;
	}
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum PostStopMessage {
	Stop,
	Boom,
	Noop,
}

pub struct SpawnChildActor {
	pub log: SharedLog,
}

#[derive(Debug)]
pub enum SpawnChildMessage {
	SpawnAndSend(String),
}

impl Actor for SpawnChildActor {
	type State = ();
	type Message = SpawnChildMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, ctx: &Context<Self::Message>) -> Directive {
		match msg {
			SpawnChildMessage::SpawnAndSend(text) => {
				let child = ctx.system().spawn(
					"child",
					LogActor {
						log: self.log.clone(),
					},
				);
				let _ = child.actor_ref.send(text);
			}
		}
		Directive::Continue
	}
}

pub struct InitSenderActor {
	pub target: ActorRef<String>,
	pub init_msg: String,
}

impl Actor for InitSenderActor {
	type State = ();
	type Message = String;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		let _ = self.target.send(self.init_msg.clone());
	}

	fn handle(&self, _state: &mut Self::State, _msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		Directive::Continue
	}
}

pub struct TickActor {
	pub timestamps: Arc<Mutex<Vec<u64>>>,
}

#[derive(Debug, Clone)]
pub struct TickMessage(pub u64);

impl Actor for TickActor {
	type State = ();
	type Message = TickMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		self.timestamps.lock().unwrap().push(msg.0);
		Directive::Continue
	}
}

pub struct InitPanicActor;

impl Actor for InitPanicActor {
	type State = ();
	type Message = ();

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		panic!("init boom");
	}

	fn handle(&self, _state: &mut Self::State, _msg: (), _ctx: &Context<Self::Message>) -> Directive {
		Directive::Continue
	}
}

pub struct PostStopPanicActor;

impl Actor for PostStopPanicActor {
	type State = ();
	type Message = ();

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, _msg: (), _ctx: &Context<Self::Message>) -> Directive {
		Directive::Stop
	}

	fn post_stop(&self) {
		panic!("post_stop boom");
	}
}

/// Run steps until idle, collecting all step results as actor_ids.
pub fn collect_step_trace(system: &ActorSystem) -> Vec<usize> {
	let mut trace = Vec::new();
	loop {
		match system.step() {
			StepResult::Processed {
				actor_id,
			} => trace.push(actor_id),
			StepResult::Stopped {
				actor_id,
			} => trace.push(actor_id),
			StepResult::Panicked {
				actor_id,
				..
			} => trace.push(actor_id),
			StepResult::Idle => break,
		}
	}
	trace
}
