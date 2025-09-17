// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Test utilities for the logging subsystem
//!
//! This module provides utilities for testing the logging subsystem in
//! isolation without interfering with the global logger or other tests.

#[cfg(debug_assertions)]
use crossbeam_channel::Sender;
#[cfg(debug_assertions)]
use reifydb_core::interface::subsystem::logging::Record;

/// Handle for test logging that automatically sets and clears the mock logger
#[cfg(debug_assertions)]
pub struct TestLoggerHandle {
	sender: Sender<Record>,
	#[allow(dead_code)]
	guard: reifydb_core::interface::subsystem::logging::mock::MockLoggerGuard,
}

#[cfg(debug_assertions)]
impl TestLoggerHandle {
	/// Create a new test logger handle
	pub fn new(sender: Sender<Record>) -> Self {
		use reifydb_core::interface::subsystem::logging::mock::MockLoggerGuard;

		let guard = MockLoggerGuard::new(sender.clone());
		Self {
			sender,
			guard,
		}
	}

	/// Get the sender for this test logger
	pub fn sender(&self) -> &Sender<Record> {
		&self.sender
	}

	/// Clone the sender for this test logger
	pub fn clone_sender(&self) -> Sender<Record> {
		self.sender.clone()
	}
}

/// Extension trait for LoggingBuilder to add test-specific methods
#[cfg(debug_assertions)]
pub trait LoggingBuilderTestExt {
	/// Build a logging subsystem for testing with isolated logger
	fn build_for_test(self) -> (crate::LoggingSubsystem, TestLoggerHandle);
}

#[cfg(debug_assertions)]
impl LoggingBuilderTestExt for crate::LoggingBuilder {
	fn build_for_test(self) -> (crate::LoggingSubsystem, TestLoggerHandle) {
		self.build_for_test()
	}
}
