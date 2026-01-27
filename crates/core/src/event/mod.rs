// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{
	any::{Any, TypeId},
	collections::HashMap,
	sync::Arc,
};

use reifydb_runtime::{actor::system::ActorSystem, sync::rwlock::RwLock};

mod dispatcher;
pub mod flow;
pub mod lifecycle;
#[macro_use]
pub mod r#macro;
pub mod metric;
pub mod store;
pub mod transaction;

use dispatcher::{DispatcherActor, DispatcherEntry, DispatcherMsg, TypedDispatcher};

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

/// Event bus for async event dispatch via actors.
///
/// The hot path `emit()` returns immediately after queueing events
/// to dispatcher actors running on the actor system's thread pool.
#[derive(Clone)]
pub struct EventBus {
	dispatchers: Arc<RwLock<HashMap<TypeId, DispatcherEntry>>>,
	actor_system: ActorSystem,
}

impl EventBus {
	/// Create a new EventBus with the given actor system.
	pub fn new(actor_system: ActorSystem) -> Self {
		Self {
			dispatchers: Arc::new(RwLock::new(HashMap::new())),
			actor_system,
		}
	}

	/// Register a listener for an event type.
	///
	/// On first registration for a given event type, spawns a dispatcher actor.
	pub fn register<E, L>(&self, listener: L)
	where
		E: Event,
		L: EventListener<E>,
	{
		let type_id = TypeId::of::<E>();
		let listener_arc: Arc<dyn EventListener<E>> = Arc::new(listener);

		// Fast path: check if dispatcher exists (read lock)
		{
			let dispatchers = self.dispatchers.read();
			if let Some(entry) = dispatchers.get(&type_id) {
				entry.dispatcher.register_any(Box::new(listener_arc));
				return;
			}
		}

		// Slow path: spawn dispatcher (write lock)
		let mut dispatchers = self.dispatchers.write();

		// Double-check after acquiring write lock
		if let Some(entry) = dispatchers.get(&type_id) {
			entry.dispatcher.register_any(Box::new(listener_arc));
			return;
		}

		// Spawn new dispatcher actor
		let actor = DispatcherActor::<E>::new();
		let type_name = std::any::type_name::<E>();
		let actor_name = format!("event-dispatcher-{}", type_name);
		let handle = self.actor_system.spawn(&actor_name, actor);
		let actor_ref = handle.actor_ref().clone();

		// Register the listener
		let _ = actor_ref.send(DispatcherMsg::Register(listener_arc));

		// Store entry (actor stays alive via ActorRef inside dispatcher)
		let dispatcher = TypedDispatcher::new(actor_ref);
		let entry = DispatcherEntry {
			dispatcher: Box::new(dispatcher),
		};
		dispatchers.insert(type_id, entry);

		// Note: We don't store the handle. The actor will stop when the ActorRef is dropped
		// (which happens when DispatcherEntry is removed from the map, i.e., when EventBus is dropped)
		drop(handle);
	}

	/// Emit an event to all registered listeners.
	///
	/// Returns immediately after queueing to the dispatcher actor (~50ns).
	pub fn emit<E: Event>(&self, event: E) {
		let type_id = TypeId::of::<E>();
		let dispatchers = self.dispatchers.read();
		if let Some(entry) = dispatchers.get(&type_id) {
			entry.dispatcher.emit_any(event.into_any());
		}
	}
}

#[cfg(test)]
pub mod tests {
	use std::{
		sync::{Arc, Mutex},
		time::Duration,
	};

	use reifydb_runtime::actor::system::{ActorSystem, ActorSystemConfig};

	use crate::event::{Event, EventBus, EventListener};

	/// Create an actor system for testing.
	fn test_actor_system() -> ActorSystem {
		ActorSystem::new(ActorSystemConfig::default().pool_threads(2))
	}

	/// Wait for async event processing to complete.
	fn wait_for_processing() {
		std::thread::sleep(Duration::from_millis(50));
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
		let event_bus = EventBus::new(actor_system);
		event_bus.emit(TestEvent::new());
	}

	#[test]
	fn test_register_single_listener() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(actor_system);
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.emit(TestEvent::new());
		wait_for_processing();
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);
	}

	#[test]
	fn test_emit_unregistered_event() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(actor_system);
		event_bus.emit(TestEvent::new());
	}

	#[test]
	fn test_multiple_listeners_same_event() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(actor_system);
		let listener1 = TestEventListener::default();
		let listener2 = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener1.clone());
		event_bus.register::<TestEvent, TestEventListener>(listener2.clone());

		event_bus.emit(TestEvent::new());
		wait_for_processing();
		assert_eq!(*listener1.0.counter.lock().unwrap(), 1);
		assert_eq!(*listener2.0.counter.lock().unwrap(), 1);
	}

	#[test]
	fn test_event_bus_clone() {
		let actor_system = test_actor_system();
		let event_bus1 = EventBus::new(actor_system);
		let listener = TestEventListener::default();
		event_bus1.register::<TestEvent, TestEventListener>(listener.clone());

		let event_bus2 = event_bus1.clone();
		event_bus2.emit(TestEvent::new());
		wait_for_processing();
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);
	}

	#[test]
	fn test_concurrent_registration() {
		let actor_system = test_actor_system();
		let event_bus = Arc::new(EventBus::new(actor_system));
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
	}

	#[test]
	fn test_concurrent_emitting() {
		let actor_system = test_actor_system();
		let event_bus = Arc::new(EventBus::new(actor_system));
		let listener = TestEventListener::default();
		event_bus.register::<TestEvent, TestEventListener>(listener.clone());

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

		wait_for_processing();
		assert!(*listener.0.counter.lock().unwrap() >= 10);
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
		let event_bus = EventBus::new(actor_system);
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.register::<AnotherEvent, TestEventListener>(listener.clone());

		// Each event type triggers only its own listeners
		event_bus.emit(TestEvent::new());
		wait_for_processing();
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);

		event_bus.emit(TestEvent::new());
		wait_for_processing();
		assert_eq!(*listener.0.counter.lock().unwrap(), 2);

		event_bus.emit(AnotherEvent::new());
		wait_for_processing();
		assert_eq!(*listener.0.counter.lock().unwrap(), 4); // 2 * 2
	}
}
