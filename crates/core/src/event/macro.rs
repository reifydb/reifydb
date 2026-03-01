// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

#[macro_export]
macro_rules! define_event {
	// Handle empty structs (e.g., OnStartEvent)
	(
		$(#[$meta:meta])*
		$vis:vis struct $name:ident {}
	) => {
		$(#[$meta])*
		#[derive(Debug, Clone)]
		$vis struct $name {}

		impl $name {
			#[allow(clippy::new_without_default)]
			pub fn new() -> Self {
				Self {}
			}
		}

		impl $crate::event::Event for $name {
			fn as_any(&self) -> &dyn std::any::Any {
				self
			}

			fn into_any(self) -> Box<dyn std::any::Any + Send> {
				Box::new(self)
			}
		}
	};

	// Handle non-empty structs with fields
	(
		$(#[$meta:meta])*
		$vis:vis struct $name:ident {
			$(
				$(#[$field_meta:meta])*
				$field_vis:vis $field:ident: $field_ty:ty
			),* $(,)?
		}
	) => {
		// create unique inner module name
		::paste::paste! {
			// Inner struct with all fields
			#[doc(hidden)]
			#[allow(non_snake_case)]
			mod [<__inner_ $name:snake>] {
				#[allow(unused_imports)]
				use super::*;

				#[derive(Debug)]
				#[allow(dead_code)]
				pub(super) struct Inner {
					$(
						$(#[$field_meta])*
						pub(super) $field: $field_ty,
					)*
				}
			}

			// Wrapper struct with Arc
			$(#[$meta])*
			#[derive(Debug)]
			$vis struct $name {
				inner: std::sync::Arc<[<__inner_ $name:snake>]::Inner>,
			}

			// Clone implementation (cheap Arc clone)
			impl Clone for $name {
				fn clone(&self) -> Self {
					Self {
						inner: std::sync::Arc::clone(&self.inner),
					}
				}
			}

			// Constructor and accessor methods
			impl $name {
				#[allow(clippy::too_many_arguments)]
				#[allow(clippy::new_without_default)]
				pub fn new($($field: $field_ty),*) -> Self {
					Self {
						inner: std::sync::Arc::new([<__inner_ $name:snake>]::Inner {
							$($field),*
						}),
					}
				}

				$(
					#[allow(dead_code)]
					pub fn $field(&self) -> &$field_ty {
						&self.inner.$field
					}
				)*
			}

			// Event trait implementation
			impl $crate::event::Event for $name {
				fn as_any(&self) -> &dyn std::any::Any {
					self
				}

				fn into_any(self) -> Box<dyn std::any::Any + Send> {
					Box::new(self)
				}
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

	use reifydb_runtime::{SharedRuntimeConfig, actor::system::ActorSystem};

	use crate::event::{Event, EventBus, EventListener};

	define_event! {
		pub struct DefineTestEvent {
			pub data: Vec<i32>,
			pub name: String,
		}
	}

	define_event! {
		pub struct EmptyDefineEvent {}
	}

	#[test]
	fn test_define_event_cheap_clone() {
		let large_vec = vec![0; 10_000];
		let event = DefineTestEvent::new(large_vec, "test".to_string());

		// Clone should be cheap (just Arc increment)
		let clone1 = event.clone();
		let clone2 = event.clone();

		// Verify they share the same Arc by comparing pointers
		assert!(Arc::ptr_eq(&event.inner, &clone1.inner));
		assert!(Arc::ptr_eq(&event.inner, &clone2.inner));

		// Data should be accessible
		assert_eq!(event.data().len(), 10_000);
		assert_eq!(clone1.data().len(), 10_000);
		assert_eq!(clone2.data().len(), 10_000);
	}

	#[test]
	fn test_define_event_field_access() {
		let event = DefineTestEvent::new(vec![1, 2, 3], "my_event".to_string());

		assert_eq!(event.data(), &vec![1, 2, 3]);
		assert_eq!(event.name(), "my_event");

		// Test that we get references, not owned values
		let _data_ref: &Vec<i32> = event.data();
		let _name_ref: &String = event.name();
	}

	#[test]
	fn test_define_event_empty_struct() {
		let event = EmptyDefineEvent::new();
		let clone = event.clone();

		// Should compile and work
		drop(event);
		drop(clone);
	}

	#[test]
	fn test_define_event_implements_event_trait() {
		let event = DefineTestEvent::new(vec![42], "test".to_string());

		// Test Event trait methods
		let any_ref = event.as_any();
		assert!(any_ref.downcast_ref::<DefineTestEvent>().is_some());

		let event2 = DefineTestEvent::new(vec![99], "test2".to_string());
		let any_box = event2.into_any();
		assert!(any_box.downcast::<DefineTestEvent>().is_ok());
	}

	#[test]
	fn test_define_event_send_sync() {
		// This test verifies that events are Send + Sync
		fn assert_send<T: Send>() {}
		fn assert_sync<T: Sync>() {}

		assert_send::<DefineTestEvent>();
		assert_sync::<DefineTestEvent>();

		// Test that we can actually send across threads
		let event = DefineTestEvent::new(vec![1, 2, 3], "thread_test".to_string());
		let handle = thread::spawn(move || {
			assert_eq!(event.data(), &vec![1, 2, 3]);
		});
		handle.join().unwrap();
	}

	#[test]
	fn test_define_event_with_event_bus() {
		let actor_system = ActorSystem::new(SharedRuntimeConfig::default().actor_system_config());
		let event_bus = EventBus::new(&actor_system);

		// Create a listener for DefineTestEvent
		#[derive(Clone)]
		struct DefineTestListener {
			counter: Arc<Mutex<i32>>,
		}

		impl EventListener<DefineTestEvent> for DefineTestListener {
			fn on(&self, event: &DefineTestEvent) {
				let mut c = self.counter.lock().unwrap();
				*c += event.data().len() as i32;
			}
		}

		let listener = DefineTestListener {
			counter: Arc::new(Mutex::new(0)),
		};

		event_bus.register::<DefineTestEvent, DefineTestListener>(listener.clone());

		// Emit event
		event_bus.emit(DefineTestEvent::new(vec![1, 2, 3], "test".to_string()));
		event_bus.wait_for_completion();
		assert_eq!(*listener.counter.lock().unwrap(), 3);

		// Emit another
		event_bus.emit(DefineTestEvent::new(vec![1, 2, 3, 4, 5], "test2".to_string()));
		event_bus.wait_for_completion();
		assert_eq!(*listener.counter.lock().unwrap(), 8);
	}
}
