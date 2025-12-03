// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	any::{Any, TypeId},
	collections::HashMap,
	sync::{Arc, RwLock},
};

use tracing::error;

pub mod catalog;
pub mod cdc;
pub mod flow;
pub mod lifecycle;
pub mod transaction;

pub trait Event: Any + Send + Sync + 'static {
	fn as_any(&self) -> &dyn Any;
}

pub trait EventListener<E>: Send + Sync + 'static
where
	E: Event,
{
	fn on(&self, event: &E);
}

trait EventListenerList: Any + Send + Sync {
	fn on_any(&self, event: &dyn Any);
	fn as_any_mut(&mut self) -> &mut dyn Any;
}

struct EventListenerListImpl<E> {
	listeners: RwLock<Vec<Box<dyn EventListener<E>>>>,
}

impl<E> EventListenerListImpl<E>
where
	E: Event,
{
	fn new() -> Self {
		Self {
			listeners: RwLock::new(Vec::new()),
		}
	}

	fn add(&mut self, listener: Box<dyn EventListener<E>>) {
		self.listeners.write().unwrap().push(listener);
	}
}

impl<E> EventListenerList for EventListenerListImpl<E>
where
	E: Event,
{
	fn on_any(&self, event: &dyn Any) {
		if let Some(event) = event.downcast_ref::<E>() {
			for listener in self.listeners.read().unwrap().iter() {
				// Add panic safety - catch panics and continue
				// with other listeners
				let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
					listener.on(event);
				}));
				if let Err(_) = result {
					error!("Event listener panicked for event type {}", std::any::type_name::<E>());
				}
			}
		}
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

#[derive(Clone)]
pub struct EventBus {
	listeners: Arc<RwLock<HashMap<TypeId, Box<dyn EventListenerList>>>>,
}

impl Default for EventBus {
	fn default() -> Self {
		Self::new()
	}
}

impl EventBus {
	pub fn new() -> Self {
		Self {
			listeners: Arc::new(RwLock::new(HashMap::new())),
		}
	}

	pub fn register<E, L>(&self, listener: L)
	where
		E: Event,
		L: EventListener<E>,
	{
		let type_id = TypeId::of::<E>();

		self.listeners
			.write()
			.unwrap()
			.entry(type_id)
			.or_insert_with(|| Box::new(EventListenerListImpl::<E>::new()))
			.as_any_mut()
			.downcast_mut::<EventListenerListImpl<E>>()
			.unwrap()
			.add(Box::new(listener));
	}

	pub fn emit<E>(&self, event: E)
	where
		E: Event,
	{
		// Infallible emit with panic safety
		let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
			let type_id = TypeId::of::<E>();
			let listeners = self.listeners.read().unwrap();

			if let Some(listener_list) = listeners.get(&type_id) {
				listener_list.on_any(event.as_any());
			}
		}));

		if let Err(_) = result {
			error!("Event emission panicked for type {}", std::any::type_name::<E>());
		}
	}
}

#[macro_export]
macro_rules! impl_event {
	($ty:ty) => {
		impl $crate::event::Event for $ty {
			fn as_any(&self) -> &dyn std::any::Any {
				self
			}
		}
	};
}

#[cfg(test)]
mod tests {
	use std::{
		sync::{Arc, Mutex},
		thread,
	};

	use crate::event::{Event, EventBus, EventListener};

	#[derive(Debug)]
	pub struct TestEvent {}

	impl_event!(TestEvent);

	#[derive(Debug)]
	pub struct AnotherEvent {}

	impl_event!(AnotherEvent);

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
		let event_bus = EventBus::new();
		event_bus.emit(TestEvent {});
	}

	#[test]
	fn test_event_bus_default() {
		let event_bus = EventBus::default();
		event_bus.emit(TestEvent {});
	}

	#[test]
	fn test_register_single_listener() {
		let event_bus = EventBus::new();
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.emit(TestEvent {});
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);
	}

	#[test]
	fn test_emit_unregistered_event() {
		let event_bus = EventBus::new();
		event_bus.emit(TestEvent {});
	}

	#[test]
	fn test_multiple_listeners_same_event() {
		let event_bus = EventBus::new();
		let listener1 = TestEventListener::default();
		let listener2 = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener1.clone());
		event_bus.register::<TestEvent, TestEventListener>(listener2.clone());

		event_bus.emit(TestEvent {});
		assert_eq!(*listener1.0.counter.lock().unwrap(), 1);
		assert_eq!(*listener2.0.counter.lock().unwrap(), 1);
	}

	#[test]
	fn test_event_bus_clone() {
		let event_bus1 = EventBus::new();
		let listener = TestEventListener::default();
		event_bus1.register::<TestEvent, TestEventListener>(listener.clone());

		let event_bus2 = event_bus1.clone();
		event_bus2.emit(TestEvent {});
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);
	}

	#[test]
	fn test_concurrent_registration() {
		let event_bus = Arc::new(EventBus::new());
		let handles: Vec<_> = (0..10)
			.map(|_| {
				let event_bus = event_bus.clone();
				thread::spawn(move || {
					let listener = TestEventListener::default();
					event_bus.register::<TestEvent, TestEventListener>(listener);
				})
			})
			.collect();

		for handle in handles {
			handle.join().unwrap();
		}

		event_bus.emit(TestEvent {});
	}

	#[test]
	fn test_concurrent_emitting() {
		let event_bus = Arc::new(EventBus::new());
		let listener = TestEventListener::default();
		event_bus.register::<TestEvent, TestEventListener>(listener.clone());

		let handles: Vec<_> = (0..10)
			.map(|_| {
				let event_bus = event_bus.clone();
				thread::spawn(move || {
					event_bus.emit(TestEvent {});
				})
			})
			.collect();

		for handle in handles {
			handle.join().unwrap();
		}

		assert!(*listener.0.counter.lock().unwrap() >= 10);
	}

	#[derive(Debug)]
	pub struct MacroTestEvent {
		pub value: i32,
	}

	impl_event!(MacroTestEvent);

	#[test]
	fn test_impl_event_macro() {
		let event = MacroTestEvent {
			value: 42,
		};
		let any_ref = event.as_any();
		assert!(any_ref.downcast_ref::<MacroTestEvent>().is_some());
		assert_eq!(any_ref.downcast_ref::<MacroTestEvent>().unwrap().value, 42);
	}

	#[test]
	fn test_multi_event_listener() {
		let event_bus = EventBus::default();
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.register::<AnotherEvent, TestEventListener>(listener.clone());

		// Each event type triggers only its own listeners
		event_bus.emit(TestEvent {});
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);

		event_bus.emit(TestEvent {});
		assert_eq!(*listener.0.counter.lock().unwrap(), 2);

		event_bus.emit(AnotherEvent {});
		assert_eq!(*listener.0.counter.lock().unwrap(), 4); // 2 * 2
	}
}
