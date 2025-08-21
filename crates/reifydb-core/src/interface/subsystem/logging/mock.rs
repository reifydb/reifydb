// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Mock logger support for testing
//!
//! This module provides thread-local mock logger functionality that allows
//! tests to intercept and redirect log messages without interfering with
//! the global logger or other tests running in parallel.
//!
//! This functionality is only available in debug builds and is compiled out
//! in release builds for zero runtime overhead in production.

#[cfg(debug_assertions)]
use std::cell::RefCell;

#[cfg(debug_assertions)]
use crossbeam_channel::Sender;

#[cfg(debug_assertions)]
use super::Record;

#[cfg(debug_assertions)]
thread_local! {
    /// Thread-local storage for mock logger sender
    /// When set, log messages will be sent here instead of the global logger
    static MOCK_LOGGER: RefCell<Option<Sender<Record>>> = RefCell::new(None);
}

/// Set a mock logger for the current thread
#[cfg(debug_assertions)]
pub fn set_mock_logger(sender: Sender<Record>) {
	MOCK_LOGGER.with(|logger| {
		*logger.borrow_mut() = Some(sender);
	});
}

/// Clear the mock logger for the current thread
#[cfg(debug_assertions)]
pub fn clear_mock_logger() {
	MOCK_LOGGER.with(|logger| {
		*logger.borrow_mut() = None;
	});
}

/// Get the current mock logger sender if one is set
#[cfg(debug_assertions)]
pub fn get_mock_logger() -> Option<Sender<Record>> {
	MOCK_LOGGER.with(|logger| logger.borrow().clone())
}

/// Check if a mock logger is currently active
#[cfg(debug_assertions)]
pub fn is_mock_logger_active() -> bool {
	MOCK_LOGGER.with(|logger| logger.borrow().is_some())
}

/// RAII guard that sets a mock logger and clears it when dropped
#[cfg(debug_assertions)]
pub struct MockLoggerGuard {
	/// Previous logger that was set (if any)
	previous: Option<Sender<Record>>,
}

#[cfg(debug_assertions)]
impl MockLoggerGuard {
	/// Create a new mock logger guard that sets the given sender
	pub fn new(sender: Sender<Record>) -> Self {
		let previous = get_mock_logger();
		set_mock_logger(sender);
		Self {
			previous,
		}
	}
}

#[cfg(debug_assertions)]
impl Drop for MockLoggerGuard {
	fn drop(&mut self) {
		// Restore the previous logger (or clear if there wasn't one)
		if let Some(sender) = self.previous.take() {
			set_mock_logger(sender);
		} else {
			clear_mock_logger();
		}
	}
}

/// Run a function with a mock logger active
#[cfg(debug_assertions)]
pub fn with_mock_logger<T>(sender: Sender<Record>, f: impl FnOnce() -> T) -> T {
	let _guard = MockLoggerGuard::new(sender);
	f()
}

#[cfg(test)]
mod tests {
	use LogLevel::{Debug, Info};
	use crossbeam_channel::unbounded;

	use super::*;
	use crate::interface::subsystem::logging::{LogLevel, Record};

	#[test]
	fn test_mock_logger_basic() {
		let (sender, receiver) = unbounded();

		assert!(!is_mock_logger_active());

		set_mock_logger(sender.clone());
		assert!(is_mock_logger_active());

		// Should be able to get the logger back
		let retrieved = get_mock_logger().unwrap();
		let record = Record::new(Info, "test", "message");
		retrieved.send(record.clone()).unwrap();

		let received = receiver.try_recv().unwrap();
		assert_eq!(received.message, "message");

		clear_mock_logger();
		assert!(!is_mock_logger_active());
	}

	#[test]
	fn test_mock_logger_guard() {
		let (sender1, _) = unbounded();
		let (sender2, _) = unbounded();

		assert!(!is_mock_logger_active());

		{
			let _guard = MockLoggerGuard::new(sender1.clone());
			assert!(is_mock_logger_active());

			// Nested guard
			{
				let _guard2 =
					MockLoggerGuard::new(sender2.clone());
				assert!(is_mock_logger_active());
				// Should have sender2 active
			}

			// Should restore sender1
			assert!(is_mock_logger_active());
		}

		// Should be cleared after guard drops
		assert!(!is_mock_logger_active());
	}

	#[test]
	fn test_with_mock_logger() {
		let (sender, receiver) = unbounded();

		assert!(!is_mock_logger_active());

		let result = with_mock_logger(sender, || {
			assert!(is_mock_logger_active());

			let logger = get_mock_logger().unwrap();
			let record = Record::new(Debug, "test", "test message");
			logger.send(record).unwrap();

			42
		});

		assert_eq!(result, 42);
		assert!(!is_mock_logger_active());

		let received = receiver.try_recv().unwrap();
		assert_eq!(received.message, "test message");
	}

	#[test]
	fn test_thread_isolation() {
		use std::thread;

		let (sender1, receiver1) = unbounded();
		let (sender2, receiver2) = unbounded();

		let handle1 = thread::spawn(move || {
			with_mock_logger(sender1, || {
				let logger = get_mock_logger().unwrap();
				let record = Record::new(
					Info, "thread1", "message1",
				);
				logger.send(record).unwrap();
			});
		});

		let handle2 = thread::spawn(move || {
			with_mock_logger(sender2, || {
				let logger = get_mock_logger().unwrap();
				let record = Record::new(
					Info, "thread2", "message2",
				);
				logger.send(record).unwrap();
			});
		});

		handle1.join().unwrap();
		handle2.join().unwrap();

		// Each thread should have sent to its own receiver
		let received1 = receiver1.try_recv().unwrap();
		assert_eq!(received1.module, "thread1");
		assert_eq!(received1.message, "message1");

		let received2 = receiver2.try_recv().unwrap();
		assert_eq!(received2.module, "thread2");
		assert_eq!(received2.message, "message2");
	}
}
