// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Dispatcher actors for async event handling.
//!
//! This module provides actor-based dispatching of events, allowing
//! `emit()` to return immediately after queuing events to dispatcher actors.

use std::{any::Any, marker::PhantomData, sync::Arc};

use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	traits::{Actor, Flow},
};

use super::{Event, EventListener};

/// Messages for the dispatcher actor.
pub enum DispatcherMsg<E: Event> {
	/// Emit an event to all registered listeners.
	Emit(E),
	/// Register a new listener.
	Register(Arc<dyn EventListener<E>>),
}

/// Actor that dispatches events to listeners.
///
/// Each event type gets its own dispatcher actor, spawned lazily
/// on first registration.
pub struct DispatcherActor<E: Event> {
	_marker: PhantomData<E>,
}

impl<E: Event> DispatcherActor<E> {
	pub fn new() -> Self {
		Self {
			_marker: PhantomData,
		}
	}
}

impl<E: Event> Default for DispatcherActor<E> {
	fn default() -> Self {
		Self::new()
	}
}

impl<E: Event> Actor for DispatcherActor<E> {
	type State = Vec<Arc<dyn EventListener<E>>>;
	type Message = DispatcherMsg<E>;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		Vec::new()
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Flow {
		match msg {
			DispatcherMsg::Emit(event) => {
				for listener in state.iter() {
					listener.on(&event); // panic = process terminates
				}
			}
			DispatcherMsg::Register(listener) => {
				state.push(listener);
			}
		}
		Flow::Continue
	}
}

/// Type-erased trait for dispatching events.
///
/// This allows storing dispatchers for different event types in a single map.
pub trait ErasedDispatcher: Send + Sync {
	/// Emit an event (type-erased). The event must match the dispatcher's type.
	fn emit_any(&self, event: Box<dyn Any + Send>);

	/// Register a listener (type-erased). Internal use only.
	fn register_any(&self, listener: Box<dyn Any + Send>);
}

/// Typed wrapper around an actor ref for dispatching events.
pub struct TypedDispatcher<E: Event> {
	actor_ref: ActorRef<DispatcherMsg<E>>,
}

impl<E: Event> TypedDispatcher<E> {
	/// Create a new typed dispatcher from an actor ref.
	pub fn new(actor_ref: ActorRef<DispatcherMsg<E>>) -> Self {
		Self {
			actor_ref,
		}
	}

	/// Emit an event to all registered listeners.
	pub fn emit(&self, event: E) {
		let _ = self.actor_ref.send(DispatcherMsg::Emit(event));
	}

	/// Register a new listener.
	pub fn register(&self, listener: Arc<dyn EventListener<E>>) {
		let _ = self.actor_ref.send(DispatcherMsg::Register(listener));
	}
}

impl<E: Event> ErasedDispatcher for TypedDispatcher<E> {
	fn emit_any(&self, event: Box<dyn Any + Send>) {
		if let Ok(event) = event.downcast::<E>() {
			self.emit(*event);
		}
	}

	fn register_any(&self, listener: Box<dyn Any + Send>) {
		if let Ok(listener) = listener.downcast::<Arc<dyn EventListener<E>>>() {
			self.register(*listener);
		}
	}
}

/// Entry in the dispatchers map, holding the type-erased dispatcher.
///
/// The actor stays alive as long as the `ActorRef` inside `TypedDispatcher` exists.
/// We don't store the `ActorHandle` since we don't need to join/wait on dispatcher
/// actors (shutdown cancels immediately without draining).
pub struct DispatcherEntry {
	/// The type-erased dispatcher for sending messages.
	pub dispatcher: Box<dyn ErasedDispatcher>,
}
