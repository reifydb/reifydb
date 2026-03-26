// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use crate::value::column::columns::Columns;

/// A captured event dispatch during test execution.
#[derive(Clone, Debug)]
pub struct CapturedEvent {
	pub sequence: u64,
	pub namespace: String,
	pub event: String,
	pub variant: String,
	pub depth: u8,
	pub columns: Columns,
}

/// A captured handler invocation during test execution.
#[derive(Clone, Debug)]
pub struct HandlerInvocation {
	pub sequence: u64,
	pub namespace: String,
	pub handler: String,
	pub event: String,
	pub variant: String,
	pub duration_ns: u64,
	pub outcome: String,
	pub message: String,
}

/// Identifies the primitive type category for a `testing::*::changed()` generator.
pub struct TestingChanged {
	pub primitive_type: &'static str,
}

impl TestingChanged {
	pub fn new(primitive_type: &'static str) -> Self {
		Self {
			primitive_type,
		}
	}
}
