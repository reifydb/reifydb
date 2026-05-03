// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cell::{Cell, RefCell},
	error, fmt,
	rc::Rc,
	sync::{Arc, Mutex, atomic::AtomicBool},
	time,
};

use tracing::{debug, warn};

use crate::{
	actor::{
		context::{CancellationToken, Context},
		mailbox::ActorRef,
		traits::{Actor, Directive},
	},
	context::clock::Clock,
	pool::Pools,
};

struct ActorSystemInner {
	cancel: CancellationToken,
	clock: Clock,
	children: Mutex<Vec<ActorSystem>>,
}

#[derive(Clone)]
pub struct ActorSystem {
	inner: Arc<ActorSystemInner>,
}

impl ActorSystem {
	pub fn new(_pools: Pools, clock: Clock) -> Self {
		Self {
			inner: Arc::new(ActorSystemInner {
				cancel: CancellationToken::new(),
				clock,
				children: Mutex::new(Vec::new()),
			}),
		}
	}

	pub fn scope(&self) -> Self {
		let child = Self {
			inner: Arc::new(ActorSystemInner {
				cancel: self.inner.cancel.child_token(),
				clock: self.inner.clock.clone(),
				children: Mutex::new(Vec::new()),
			}),
		};
		self.inner.children.lock().unwrap().push(child.clone());
		child
	}

	pub fn pools(&self) -> Pools {
		Pools::default()
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
	}

	pub fn clock(&self) -> &Clock {
		&self.inner.clock
	}

	pub fn join(&self) -> Result<(), JoinError> {
		Ok(())
	}

	pub fn join_timeout(&self, _timeout: time::Duration) -> Result<(), JoinError> {
		Ok(())
	}

	pub fn spawn_system<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message> {
		let actor_ref = create_actor_ref::<A::Message>();

		let actor = Rc::new(actor);
		let actor_for_processor = actor.clone();

		let state: Rc<RefCell<Option<A::State>>> = Rc::new(RefCell::new(None));
		let state_for_processor = state.clone();

		let ctx = Context::new(actor_ref.clone(), self.clone(), self.cancellation_token());
		let ctx_for_init = ctx.clone();
		let ctx_for_processor = ctx.clone();

		let _name = name.to_string();
		let _name_for_drain = _name.clone();
		let actor_ref_for_closure = actor_ref.clone();
		let actor_ref_for_drain = actor_ref.clone();
		let cancel = self.cancellation_token();

		let init_queue: Rc<RefCell<Option<Vec<A::Message>>>> = Rc::new(RefCell::new(Some(Vec::new())));
		let init_queue_for_processor = init_queue.clone();

		let processor = move |msg: A::Message| {
			{
				let mut queue_ref = init_queue_for_processor.borrow_mut();
				if let Some(ref mut queue) = *queue_ref {
					debug!(actor = %_name, "Queueing message during initialization");
					queue.push(msg);
					return;
				}
			}

			if cancel.is_cancelled() {
				debug!(actor = %_name, "Actor cancelled, ignoring message");
				actor_ref_for_closure.mark_stopped();
				return;
			}

			let mut state_ref = state_for_processor.borrow_mut();

			if state_ref.is_none() {
				warn!(actor = %_name, "Actor state unexpectedly not initialized");
				return;
			}

			if let Some(ref mut s) = *state_ref {
				match actor_for_processor.handle(s, msg, &ctx_for_processor) {
					Directive::Stop => {
						debug!(actor = %_name, "Actor returned Directive::Stop");
						actor_for_processor.post_stop();
						actor_ref_for_closure.mark_stopped();
					}

					Directive::Continue | Directive::Yield | Directive::Park => {}
				}
			}
		};

		{
			let mut processor_ref = actor_ref.processor().borrow_mut();
			*processor_ref = Some(Box::new(processor));
		}

		{
			let mut state_ref = state.borrow_mut();
			let initial_state = actor.init(&ctx_for_init);
			*state_ref = Some(initial_state);
		}

		let queued_messages = init_queue.borrow_mut().take().unwrap_or_default();
		if !queued_messages.is_empty() {
			debug!(
				actor = %_name_for_drain,
				count = queued_messages.len(),
				"Draining queued messages after init"
			);
		}
		for msg in queued_messages {
			let _ = actor_ref_for_drain.send(msg);
		}

		ActorHandle {
			actor_ref,
		}
	}

	pub fn spawn_query<A: Actor>(&self, name: &str, actor: A) -> ActorHandle<A::Message> {
		self.spawn_system(name, actor)
	}
}

impl fmt::Debug for ActorSystem {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActorSystem").field("cancelled", &self.is_cancelled()).finish_non_exhaustive()
	}
}

pub struct ActorHandle<M> {
	pub actor_ref: ActorRef<M>,
}

impl<M> ActorHandle<M> {
	pub fn actor_ref(&self) -> &ActorRef<M> {
		&self.actor_ref
	}

	pub fn join(self) -> Result<(), JoinError> {
		Ok(())
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
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "actor join failed: {}", self.message)
	}
}

impl error::Error for JoinError {}

#[derive(Debug)]
pub struct WasmJoinError;

impl fmt::Display for WasmJoinError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "WASM task failed")
	}
}

impl error::Error for WasmJoinError {}

struct ActorRefInner<M> {
	processor: Rc<RefCell<Option<Box<dyn FnMut(M)>>>>,
	alive: Arc<AtomicBool>,
	queue: Rc<RefCell<Vec<M>>>,
	processing: Rc<Cell<bool>>,
}

impl<M> Clone for ActorRefInner<M> {
	fn clone(&self) -> Self {
		Self {
			processor: self.processor.clone(),
			alive: self.alive.clone(),
			queue: self.queue.clone(),
			processing: self.processing.clone(),
		}
	}
}

fn create_actor_ref<M>() -> ActorRef<M> {
	let inner = ActorRefInner {
		processor: Rc::new(RefCell::new(None)),
		alive: Arc::new(AtomicBool::new(true)),
		queue: Rc::new(RefCell::new(Vec::new())),
		processing: Rc::new(Cell::new(false)),
	};

	ActorRef::from_wasm_inner(inner.processor, inner.alive, inner.queue, inner.processing)
}
