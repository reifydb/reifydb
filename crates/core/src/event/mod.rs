// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{
	any::{Any, TypeId},
	collections::HashMap,
	sync::{Arc, RwLock},
};

use async_trait::async_trait;

pub mod catalog;
pub mod flow;
pub mod lifecycle;
pub mod transaction;

pub trait Event: Any + Send + Sync + Clone + 'static {
	fn as_any(&self) -> &dyn Any;
	fn into_any(self) -> Box<dyn Any + Send>;
}

#[async_trait]
pub trait EventListener<E>: Send + Sync + 'static
where
	E: Event,
{
	async fn on(&self, event: &E);
}

#[async_trait]
trait EventListenerList: Any + Send + Sync {
	async fn on_any(&self, event: Box<dyn Any + Send>);
	fn as_any_mut(&mut self) -> &mut dyn Any;
}

struct EventListenerListImpl<E> {
	listeners: RwLock<Vec<Arc<dyn EventListener<E>>>>,
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

	fn add(&mut self, listener: Arc<dyn EventListener<E>>) {
		self.listeners.write().unwrap().push(listener);
	}
}

#[async_trait]
impl<E> EventListenerList for EventListenerListImpl<E>
where
	E: Event,
{
	async fn on_any(&self, event: Box<dyn Any + Send>) {
		if let Ok(event) = event.downcast::<E>() {
			// Get a snapshot of listeners (hold lock briefly)
			let listeners: Vec<_> = {
				let guard = self.listeners.read().unwrap();
				guard.iter().cloned().collect()
			};

			// Now we can await without holding the lock
			for listener in listeners {
				listener.on(&*event).await;
			}
		}
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

#[derive(Clone)]
pub struct EventBus {
	listeners: Arc<std::sync::RwLock<HashMap<TypeId, Box<dyn EventListenerList>>>>,
}

impl Default for EventBus {
	fn default() -> Self {
		Self::new()
	}
}

impl EventBus {
	pub fn new() -> Self {
		Self {
			listeners: Arc::new(std::sync::RwLock::new(HashMap::new())),
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
			.add(Arc::new(listener));
	}

	pub async fn emit<E>(&self, event: E)
	where
		E: Event,
	{
		let type_id = TypeId::of::<E>();

		// Get the listener list while holding the sync lock briefly
		let listener_list = {
			let listeners = self.listeners.read().unwrap();
			listeners.get(&type_id).map(|l| l.as_ref() as *const dyn EventListenerList)
		};

		// Now call on_any without holding the lock
		if let Some(listener_list_ptr) = listener_list {
			// SAFETY: The listener_list is stored in an Arc inside self.listeners,
			// so it remains valid as long as self exists
			let listener_list = unsafe { &*listener_list_ptr };
			listener_list.on_any(event.into_any()).await;
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

			fn into_any(self) -> Box<dyn std::any::Any + Send> {
				Box::new(self)
			}
		}
	};
}

#[cfg(test)]
mod tests {
	use std::sync::{Arc, Mutex};

	use async_trait::async_trait;

	use crate::event::{Event, EventBus, EventListener};

	#[derive(Debug, Clone)]
	pub struct TestEvent {}

	impl_event!(TestEvent);

	#[derive(Debug, Clone)]
	pub struct AnotherEvent {}

	impl_event!(AnotherEvent);

	#[derive(Default, Debug, Clone)]
	pub struct TestEventListener(Arc<TestHandlerInner>);

	#[derive(Default, Debug)]
	pub struct TestHandlerInner {
		pub counter: Arc<Mutex<i32>>,
	}

	#[async_trait]
	impl EventListener<TestEvent> for TestEventListener {
		async fn on(&self, _event: &TestEvent) {
			let mut x = self.0.counter.lock().unwrap();
			*x += 1;
		}
	}

	#[async_trait]
	impl EventListener<AnotherEvent> for TestEventListener {
		async fn on(&self, _event: &AnotherEvent) {
			let mut x = self.0.counter.lock().unwrap();
			*x *= 2;
		}
	}

	#[tokio::test]
	async fn test_event_bus_new() {
		let event_bus = EventBus::new();
		event_bus.emit(TestEvent {}).await;
	}

	#[tokio::test]
	async fn test_event_bus_default() {
		let event_bus = EventBus::default();
		event_bus.emit(TestEvent {}).await;
	}

	#[tokio::test]
	async fn test_register_single_listener() {
		let event_bus = EventBus::new();
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.emit(TestEvent {}).await;
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);
	}

	#[tokio::test]
	async fn test_emit_unregistered_event() {
		let event_bus = EventBus::new();
		event_bus.emit(TestEvent {}).await;
	}

	#[tokio::test]
	async fn test_multiple_listeners_same_event() {
		let event_bus = EventBus::new();
		let listener1 = TestEventListener::default();
		let listener2 = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener1.clone());
		event_bus.register::<TestEvent, TestEventListener>(listener2.clone());

		event_bus.emit(TestEvent {}).await;
		assert_eq!(*listener1.0.counter.lock().unwrap(), 1);
		assert_eq!(*listener2.0.counter.lock().unwrap(), 1);
	}

	#[tokio::test]
	async fn test_event_bus_clone() {
		let event_bus1 = EventBus::new();
		let listener = TestEventListener::default();
		event_bus1.register::<TestEvent, TestEventListener>(listener.clone());

		let event_bus2 = event_bus1.clone();
		event_bus2.emit(TestEvent {}).await;
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);
	}

	#[tokio::test]
	async fn test_concurrent_registration() {
		let event_bus = Arc::new(EventBus::new());
		let mut handles = Vec::new();

		for _ in 0..10 {
			let event_bus = event_bus.clone();
			handles.push(tokio::spawn(async move {
				let listener = TestEventListener::default();
				event_bus.register::<TestEvent, TestEventListener>(listener);
			}));
		}

		for handle in handles {
			handle.await.unwrap();
		}

		event_bus.emit(TestEvent {}).await;
	}

	#[tokio::test]
	async fn test_concurrent_emitting() {
		let event_bus = Arc::new(EventBus::new());
		let listener = TestEventListener::default();
		event_bus.register::<TestEvent, TestEventListener>(listener.clone());

		let mut handles = Vec::new();

		for _ in 0..10 {
			let event_bus = event_bus.clone();
			handles.push(tokio::spawn(async move {
				event_bus.emit(TestEvent {}).await;
			}));
		}

		for handle in handles {
			handle.await.unwrap();
		}

		assert!(*listener.0.counter.lock().unwrap() >= 10);
	}

	#[derive(Debug, Clone)]
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

	#[tokio::test]
	async fn test_multi_event_listener() {
		let event_bus = EventBus::default();
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.register::<AnotherEvent, TestEventListener>(listener.clone());

		// Each event type triggers only its own listeners
		event_bus.emit(TestEvent {}).await;
		assert_eq!(*listener.0.counter.lock().unwrap(), 1);

		event_bus.emit(TestEvent {}).await;
		assert_eq!(*listener.0.counter.lock().unwrap(), 2);

		event_bus.emit(AnotherEvent {}).await;
		assert_eq!(*listener.0.counter.lock().unwrap(), 4); // 2 * 2
	}
}
