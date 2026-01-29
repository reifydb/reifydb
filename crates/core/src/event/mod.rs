// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	any::{Any, TypeId},
	collections::HashMap,
	sync::Arc,
};

use reifydb_runtime::actor::{
	context::Context,
	mailbox::ActorRef,
	system::ActorSystem,
	traits::{Actor, Directive},
};

pub mod flow;
pub mod lifecycle;
#[macro_use]
pub mod r#macro;
pub mod metric;
pub mod store;
pub mod transaction;

pub trait Event: Any + Send + Sync + Clone + 'static {
	fn as_any(&self) -> &dyn Any;
	fn into_any(self) -> Box<dyn Any + Send>;
}

pub trait EventListener<E>: Send + Sync + 'static
where
	E: Event,
{
	fn on(&self, event: &E);
}

trait EventListenerList: Any + Send + Sync {
	fn on_any(&self, event: Box<dyn Any + Send>);
	fn as_any_mut(&mut self) -> &mut dyn Any;
}

struct EventListenerListImpl<E> {
	listeners: Vec<Arc<dyn EventListener<E>>>,
}

impl<E> EventListenerListImpl<E>
where
	E: Event,
{
	fn new() -> Self {
		Self {
			listeners: Vec::new(),
		}
	}

	fn add(&mut self, listener: Arc<dyn EventListener<E>>) {
		self.listeners.push(listener);
	}
}

impl<E> EventListenerList for EventListenerListImpl<E>
where
	E: Event,
{
	fn on_any(&self, event: Box<dyn Any + Send>) {
		if let Ok(event) = event.downcast::<E>() {
			for listener in &self.listeners {
				listener.on(&*event);
			}
		}
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

// --- Actor-based EventBus ---

struct EventEnvelope {
	type_id: TypeId,
	event: Box<dyn Any + Send>,
}

enum EventBusMsg {
	Emit(EventEnvelope),
	Register {
		installer: Box<dyn FnOnce(&mut HashMap<TypeId, Box<dyn EventListenerList>>) + Send>,
	},
	WaitForCompletion(std::sync::mpsc::Sender<()>),
}

struct EventBusActor;

impl Actor for EventBusActor {
	type State = HashMap<TypeId, Box<dyn EventListenerList>>;
	type Message = EventBusMsg;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {
		HashMap::new()
	}

	fn handle(&self, state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			EventBusMsg::Emit(envelope) => {
				if let Some(list) = state.get(&envelope.type_id) {
					list.on_any(envelope.event);
				}
			}
			EventBusMsg::Register {
				installer,
			} => {
				installer(state);
			}
			EventBusMsg::WaitForCompletion(tx) => {
				let _ = tx.send(());
			}
		}
		Directive::Continue
	}
}

#[derive(Clone)]
pub struct EventBus {
	actor_ref: ActorRef<EventBusMsg>,
	_actor_system: ActorSystem,
}

impl EventBus {
	pub fn new(actor_system: &ActorSystem) -> Self {
		let handle = actor_system.spawn("event-bus", EventBusActor);
		Self {
			actor_ref: handle.actor_ref().clone(),
			_actor_system: actor_system.clone(),
		}
	}

	pub fn register<E, L>(&self, listener: L)
	where
		E: Event,
		L: EventListener<E>,
	{
		let type_id = TypeId::of::<E>();
		let listener = Arc::new(listener);

		let installer: Box<dyn FnOnce(&mut HashMap<TypeId, Box<dyn EventListenerList>>) + Send> =
			Box::new(move |map| {
				let list = map
					.entry(type_id)
					.or_insert_with(|| Box::new(EventListenerListImpl::<E>::new()));
				list.as_any_mut().downcast_mut::<EventListenerListImpl<E>>().unwrap().add(listener);
			});

		let _ = self.actor_ref.send(EventBusMsg::Register {
			installer,
		});
	}

	pub fn emit<E>(&self, event: E)
	where
		E: Event,
	{
		let type_id = TypeId::of::<E>();
		let _ = self.actor_ref.send(EventBusMsg::Emit(EventEnvelope {
			type_id,
			event: event.into_any(),
		}));
	}

	pub fn wait_for_completion(&self) {
		let (tx, rx) = std::sync::mpsc::channel();
		let _ = self.actor_ref.send(EventBusMsg::WaitForCompletion(tx));
		let _ = rx.recv();
	}
}

#[cfg(test)]
pub mod tests {
	use std::sync::{Arc, Mutex};

	use reifydb_runtime::{SharedRuntimeConfig, actor::system::ActorSystem};

	use crate::event::{Event, EventBus, EventListener};

	fn test_actor_system() -> ActorSystem {
		ActorSystem::new(SharedRuntimeConfig::default().actor_system_config())
	}

	define_event! {
		pub struct TestEvent{}
	}

	define_event! {
		pub struct AnotherEvent{}
	}

	#[derive(Default, Debug, Clone)]
	pub struct TestEventListener(Arc<TestHandlerInner>);

	#[derive(Default, Debug)]
	pub struct TestHandlerInner {
		pub counter: Arc<Mutex<i32>>,
	}

	impl EventListener<TestEvent> for TestEventListener {
		fn on(&self, _event: &TestEvent) {
			let mut x = self.0.counter.lock().unwrap();
			*x += 1;
		}
	}

	impl EventListener<AnotherEvent> for TestEventListener {
		fn on(&self, _event: &AnotherEvent) {
			let mut x = self.0.counter.lock().unwrap();
			*x *= 2;
		}
	}

	#[test]
	fn test_event_bus_new() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system);
		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
	}

	#[test]
	fn test_register_single_listener() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system);
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);
	}

	#[test]
	fn test_emit_unregistered_event() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system);
		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
	}

	#[test]
	fn test_multiple_listeners_same_event() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system);
		let listener1 = TestEventListener::default();
		let listener2 = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener1.clone());
		event_bus.register::<TestEvent, TestEventListener>(listener2.clone());

		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
		assert_eq!(*listener1.0.counter.lock().unwrap(), 1);
		assert_eq!(*listener2.0.counter.lock().unwrap(), 1);
	}

	#[test]
	fn test_event_bus_clone() {
		let actor_system = test_actor_system();
		let event_bus1 = EventBus::new(&actor_system);
		let listener = TestEventListener::default();
		event_bus1.register::<TestEvent, TestEventListener>(listener.clone());

		let event_bus2 = event_bus1.clone();
		event_bus2.emit(TestEvent::new());
		event_bus2.wait_for_completion();
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);
	}

	#[test]
	fn test_concurrent_registration() {
		let actor_system = test_actor_system();
		let event_bus = Arc::new(EventBus::new(&actor_system));
		let mut handles = Vec::new();

		for _ in 0..10 {
			let event_bus = event_bus.clone();
			handles.push(std::thread::spawn(move || {
				let listener = TestEventListener::default();
				event_bus.register::<TestEvent, TestEventListener>(listener);
			}));
		}

		for handle in handles {
			handle.join().unwrap();
		}

		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
	}

	#[test]
	fn test_concurrent_emitting() {
		let actor_system = test_actor_system();
		let event_bus = Arc::new(EventBus::new(&actor_system));
		let listener = TestEventListener::default();
		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.wait_for_completion();

		let mut handles = Vec::new();

		for _ in 0..10 {
			let event_bus = event_bus.clone();
			handles.push(std::thread::spawn(move || {
				event_bus.emit(TestEvent::new());
			}));
		}

		for handle in handles {
			handle.join().unwrap();
		}

		event_bus.wait_for_completion();
		assert_eq!(*listener.0.counter.lock().unwrap(), 10);
	}

	define_event! {
		pub struct MacroTestEvent {
			pub value: i32,
		}
	}

	#[test]
	fn test_define_event_macro() {
		let event = MacroTestEvent::new(42);
		let any_ref = event.as_any();
		assert!(any_ref.downcast_ref::<MacroTestEvent>().is_some());
		assert_eq!(any_ref.downcast_ref::<MacroTestEvent>().unwrap().value(), &42);
	}

	#[test]
	fn test_multi_event_listener() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system);
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.register::<AnotherEvent, TestEventListener>(listener.clone());

		// Each event type triggers only its own listeners
		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);

		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
		assert_eq!(*listener.0.counter.lock().unwrap(), 2);

		event_bus.emit(AnotherEvent::new());
		event_bus.wait_for_completion();
		assert_eq!(*listener.0.counter.lock().unwrap(), 4); // 2 * 2
	}
}
