// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::{Any, TypeId, type_name},
	collections::HashMap,
	sync,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
};

use reifydb_runtime::{
	actor::{
		context::Context,
		mailbox::ActorRef,
		system::ActorSpawner,
		traits::{Actor, Directive},
	},
	sync::rwlock::RwLock,
};
use sync::mpsc::Sender;

pub mod catalog;
pub mod lifecycle;
pub mod operator;
#[macro_use]
pub mod r#macro;
pub mod metric;
pub mod procedure;
pub mod row;
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
	fn on_any(&self, event: Box<dyn Any + Send>, emitted_at: Option<u64>);
	fn as_any(&self) -> &dyn Any;
}

struct EventListenerListImpl<E> {
	listeners: Vec<(u64, Arc<dyn EventListener<E>>)>,
}

impl<E> Clone for EventListenerListImpl<E> {
	fn clone(&self) -> Self {
		Self {
			listeners: self.listeners.clone(),
		}
	}
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

	fn add(&mut self, registered_at: u64, listener: Arc<dyn EventListener<E>>) {
		self.listeners.push((registered_at, listener));
	}
}

impl<E> EventListenerList for EventListenerListImpl<E>
where
	E: Event,
{
	fn on_any(&self, event: Box<dyn Any + Send>, emitted_at: Option<u64>) {
		if let Ok(event) = event.downcast::<E>() {
			for (registered_at, listener) in &self.listeners {
				if let Some(at) = emitted_at
					&& *registered_at > at
				{
					continue;
				}
				listener.on(&*event);
			}
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}
}

struct Listeners {
	map: RwLock<HashMap<TypeId, Arc<dyn EventListenerList>>>,
	registrations: AtomicU64,
	stopped: AtomicBool,
}

struct EventEnvelope {
	type_id: TypeId,
	event: Box<dyn Any + Send>,
	emitted_at: u64,
}

enum EventBusMessage {
	Emit(EventEnvelope),
	WaitForCompletion(Sender<()>),
}

struct EventBusActor {
	listeners: Arc<Listeners>,
}

impl Actor for EventBusActor {
	type State = ();
	type Message = EventBusMessage;

	fn init(&self, _ctx: &Context<Self::Message>) -> Self::State {}

	fn handle(&self, _state: &mut Self::State, msg: Self::Message, _ctx: &Context<Self::Message>) -> Directive {
		match msg {
			EventBusMessage::Emit(envelope) => {
				let list = {
					let map = self.listeners.map.read();
					map.get(&envelope.type_id).cloned()
				};
				if let Some(list) = list {
					list.on_any(envelope.event, Some(envelope.emitted_at));
				}
			}
			EventBusMessage::WaitForCompletion(tx) => {
				let _ = tx.send(());
			}
		}
		Directive::Continue
	}

	fn post_stop(&self) {
		self.listeners.stopped.store(true, Ordering::SeqCst);
	}
}

#[derive(Clone)]
pub struct EventBus {
	actor_ref: ActorRef<EventBusMessage>,
	spawner: ActorSpawner,
	listeners: Arc<Listeners>,
}

impl EventBus {
	pub fn new(spawner: &ActorSpawner) -> Self {
		let listeners = Arc::new(Listeners {
			map: RwLock::new(HashMap::new()),
			registrations: AtomicU64::new(0),
			stopped: AtomicBool::new(false),
		});
		let handle = spawner.spawn_coordination(
			"event-bus",
			EventBusActor {
				listeners: listeners.clone(),
			},
		);
		Self {
			actor_ref: handle.actor_ref().clone(),
			spawner: spawner.clone(),
			listeners,
		}
	}

	pub fn register<E, L>(&self, listener: L)
	where
		E: Event,
		L: EventListener<E>,
	{
		if self.listeners.stopped.load(Ordering::SeqCst) {
			let shutting_down = self.spawner.cancellation_token().is_none_or(|token| token.is_cancelled());
			assert!(
				shutting_down,
				"the event bus rejected a listener for {} while the system was running; the \
				 listener would never install and every later event of this type is lost silently",
				type_name::<E>()
			);
			return;
		}

		let type_id = TypeId::of::<E>();
		let mut map = self.listeners.map.write();
		let registered_at = self.listeners.registrations.fetch_add(1, Ordering::SeqCst) + 1;
		let mut list = match map.get(&type_id) {
			Some(existing) => existing
				.as_any()
				.downcast_ref::<EventListenerListImpl<E>>()
				.unwrap_or_else(|| {
					panic!("event listener list for {} has the wrong type", type_name::<E>())
				})
				.clone(),
			None => EventListenerListImpl::<E>::new(),
		};
		list.add(registered_at, Arc::new(listener));
		map.insert(type_id, Arc::new(list));
	}

	pub fn emit<E>(&self, event: E)
	where
		E: Event,
	{
		let emitted_at = self.listeners.registrations.load(Ordering::SeqCst);
		let type_id = TypeId::of::<E>();
		if self.actor_ref
			.send(EventBusMessage::Emit(EventEnvelope {
				type_id,
				event: event.into_any(),
				emitted_at,
			}))
			.is_err()
		{
			let shutting_down = self.spawner.cancellation_token().is_none_or(|token| token.is_cancelled());
			assert!(
				shutting_down,
				"the event bus rejected a {} while the system was running; dropping it silently \
				 loses a post-commit update no subscriber will ever see",
				type_name::<E>()
			);
		}
	}

	pub fn emit_inline<E>(&self, event: E)
	where
		E: Event,
	{
		let list = {
			let map = self.listeners.map.read();
			map.get(&TypeId::of::<E>()).cloned()
		};
		let Some(list) = list else {
			return;
		};
		list.on_any(event.into_any(), None);
	}

	/// Waits until the bus has drained everything queued ahead of this call.
	///
	/// Under dst the caller's thread is also the thread that steps the actor system, so it drives
	/// the system to quiescence itself and then picks the reply up without blocking. A blocking
	/// receive here would park the only thread that can run the bus actor, so the reply it waits
	/// for could never be sent; that deadlocks subsystem start-up before any test can run.
	#[cfg(reifydb_dst)]
	pub fn wait_for_completion(&self) {
		let (tx, rx) = sync::mpsc::channel();
		if self.actor_ref.send(EventBusMessage::WaitForCompletion(tx)).is_err() {
			return;
		}
		if self.spawner.is_alive() {
			self.spawner.system().run_until_idle();
		}
		let _ = rx.try_recv();
	}

	/// Waits until the bus has drained everything queued ahead of this call.
	///
	/// Off dst the bus actor runs on its own thread (host) or inline on `send` (wasm), so a
	/// blocking receive is both correct and the cheapest way to wait.
	#[cfg(not(reifydb_dst))]
	pub fn wait_for_completion(&self) {
		let (tx, rx) = sync::mpsc::channel();
		let _ = self.actor_ref.send(EventBusMessage::WaitForCompletion(tx));
		let _ = rx.recv();
	}
}

#[cfg(test)]
pub mod tests {
	use std::{
		sync::{
			Arc,
			mpsc::{Receiver, channel},
		},
		thread,
	};

	use reifydb_runtime::{
		actor::{system::ActorSystem, traits::Actor},
		context::clock::Clock,
		pool::{PoolConfig, Pools},
		sync::mutex::Mutex,
	};

	use super::EventBusActor;
	use crate::event::{Event, EventBus, EventListener};

	fn test_actor_system() -> ActorSystem {
		let pools = Pools::new(PoolConfig::default());
		ActorSystem::new(pools, Clock::Real)
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
			let mut x = self.0.counter.lock();
			*x += 1;
		}
	}

	impl EventListener<AnotherEvent> for TestEventListener {
		fn on(&self, _event: &AnotherEvent) {
			let mut x = self.0.counter.lock();
			*x *= 2;
		}
	}

	#[test]
	fn test_event_bus_new() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
	}

	#[test]
	fn test_register_single_listener() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
		assert_eq!(*listener.0.counter.lock(), 1);
	}

	#[test]
	fn test_emit_unregistered_event() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
	}

	#[test]
	fn test_multiple_listeners_same_event() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		let listener1 = TestEventListener::default();
		let listener2 = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener1.clone());
		event_bus.register::<TestEvent, TestEventListener>(listener2.clone());

		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
		assert_eq!(*listener1.0.counter.lock(), 1);
		assert_eq!(*listener2.0.counter.lock(), 1);
	}

	#[test]
	fn test_event_bus_clone() {
		let actor_system = test_actor_system();
		let event_bus1 = EventBus::new(&actor_system.spawner());
		let listener = TestEventListener::default();
		event_bus1.register::<TestEvent, TestEventListener>(listener.clone());

		let event_bus2 = event_bus1.clone();
		event_bus2.emit(TestEvent::new());
		event_bus2.wait_for_completion();
		assert_eq!(*listener.0.counter.lock(), 1);
	}

	#[test]
	fn test_concurrent_registration() {
		let actor_system = test_actor_system();
		let event_bus = Arc::new(EventBus::new(&actor_system.spawner()));
		let mut handles = Vec::new();

		for _ in 0..10 {
			let event_bus = event_bus.clone();
			handles.push(thread::spawn(move || {
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
		let event_bus = Arc::new(EventBus::new(&actor_system.spawner()));
		let listener = TestEventListener::default();
		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.wait_for_completion();

		let mut handles = Vec::new();

		for _ in 0..10 {
			let event_bus = event_bus.clone();
			handles.push(thread::spawn(move || {
				event_bus.emit(TestEvent::new());
			}));
		}

		for handle in handles {
			handle.join().unwrap();
		}

		event_bus.wait_for_completion();
		assert_eq!(*listener.0.counter.lock(), 10);
	}

	define_event! {
		pub struct MacroTestEvent {
			pub value: i32,
		}
	}

	#[test]
	fn testine_event_macro() {
		let event = MacroTestEvent::new(42);
		let any_ref = event.as_any();
		assert!(any_ref.downcast_ref::<MacroTestEvent>().is_some());
		assert_eq!(any_ref.downcast_ref::<MacroTestEvent>().unwrap().value(), &42);
	}

	#[test]
	fn test_multi_event_listener() {
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.register::<AnotherEvent, TestEventListener>(listener.clone());

		// Each event type triggers only its own listeners
		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
		assert_eq!(*listener.0.counter.lock(), 1);

		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();
		assert_eq!(*listener.0.counter.lock(), 2);

		event_bus.emit(AnotherEvent::new());
		event_bus.wait_for_completion();
		assert_eq!(*listener.0.counter.lock(), 4); // 2 * 2
	}

	struct GateListener {
		gate: Mutex<Receiver<()>>,
	}

	impl EventListener<TestEvent> for GateListener {
		fn on(&self, _event: &TestEvent) {
			self.gate.lock().recv().expect("gate sender dropped");
		}
	}

	#[derive(Clone, Default)]
	struct RecordingListener(Arc<Mutex<Vec<i32>>>);

	impl EventListener<MacroTestEvent> for RecordingListener {
		fn on(&self, event: &MacroTestEvent) {
			self.0.lock().push(*event.value());
		}
	}

	#[test]
	fn an_inline_listener_has_run_when_emit_inline_returns() {
		// Without inline delivery the commit thread would publish before the plan cache is flushed.
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		let listener = TestEventListener::default();
		event_bus.register::<TestEvent, TestEventListener>(listener.clone());

		event_bus.emit_inline(TestEvent::new());

		assert_eq!(*listener.0.counter.lock(), 1, "emit_inline must run the listener before it returns");
	}

	#[test]
	fn a_listener_registered_after_an_emit_does_not_get_that_queued_event() {
		// The gate keeps event 2 queued past B's register, so only the barrier compare keeps it from B.
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		let (open, gate) = channel::<()>();
		event_bus.register::<TestEvent, GateListener>(GateListener {
			gate: Mutex::new(gate),
		});

		event_bus.emit(TestEvent::new());
		event_bus.emit(TestEvent::new());
		let late = TestEventListener::default();
		event_bus.register::<TestEvent, TestEventListener>(late.clone());
		open.send(()).unwrap();
		open.send(()).unwrap();
		event_bus.wait_for_completion();

		assert_eq!(*late.0.counter.lock(), 0, "a listener must never see an event emitted before its register");
	}

	#[test]
	fn a_listener_registered_before_an_emit_gets_it() {
		// A barrier compare that is flipped or off by one drops events the listener was registered for.
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		let listener = TestEventListener::default();
		event_bus.register::<TestEvent, TestEventListener>(listener.clone());

		event_bus.emit(TestEvent::new());
		event_bus.wait_for_completion();

		assert_eq!(*listener.0.counter.lock(), 1, "an emit ordered after the register must be delivered");
	}

	#[test]
	fn queued_events_reach_a_listener_in_emit_order() {
		// Queued delivery must keep emit order, otherwise post-commit readers see versions out of order.
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		let listener = RecordingListener::default();
		event_bus.register::<MacroTestEvent, RecordingListener>(listener.clone());

		event_bus.emit(MacroTestEvent::new(1));
		event_bus.emit(MacroTestEvent::new(2));
		event_bus.emit(MacroTestEvent::new(3));
		event_bus.wait_for_completion();

		assert_eq!(*listener.0.lock(), vec![1, 2, 3]);
	}

	#[test]
	fn an_inline_emit_reaches_a_listener_registered_after_earlier_queued_emits() {
		// Inline delivery must not take part in the barrier, otherwise it would skip a fresh listener.
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		event_bus.emit(TestEvent::new());
		let listener = TestEventListener::default();
		event_bus.register::<TestEvent, TestEventListener>(listener.clone());

		event_bus.emit_inline(TestEvent::new());

		assert_eq!(*listener.0.counter.lock(), 1, "the inline emit must reach the listener with no wait");
	}

	#[test]
	#[should_panic(expected = "while the system was running")]
	fn a_register_on_a_stopped_bus_panics_while_the_system_runs() {
		// A register on a dead bus while the system runs would install nothing and lose every later event.
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		EventBusActor {
			listeners: event_bus.listeners.clone(),
		}
		.post_stop();

		event_bus.register::<TestEvent, TestEventListener>(TestEventListener::default());
	}

	#[test]
	fn a_register_on_a_stopped_bus_during_shutdown_installs_nothing() {
		// During shutdown a register on a stopped bus must return quietly and must not install the listener.
		let actor_system = test_actor_system();
		let event_bus = EventBus::new(&actor_system.spawner());
		actor_system.shutdown();
		EventBusActor {
			listeners: event_bus.listeners.clone(),
		}
		.post_stop();
		let listener = TestEventListener::default();

		event_bus.register::<TestEvent, TestEventListener>(listener.clone());
		event_bus.emit_inline(TestEvent::new());

		assert_eq!(*listener.0.counter.lock(), 0, "a register on a stopped bus must install nothing");
	}
}
