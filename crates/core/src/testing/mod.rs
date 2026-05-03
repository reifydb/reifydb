// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Capture types used by the test harness to record what happened during a run.
//!
//! `CapturedEvent` records each event the bus dispatched (sequence number, namespace, event family, variant, depth,
//! captured columns); `CapturedInvocation` records each handler invocation (sequence, namespace, handler, event,
//! variant, duration, outcome). `TestingChanged` is the marker emitted by handlers that opt in to test capture for a
//! specific row-shape type. These structures are runtime-only; they are not persisted and never appear in normal
//! operation.

use crate::value::column::columns::Columns;

#[derive(Clone, Debug)]
pub struct CapturedEvent {
	pub sequence: u64,
	pub namespace: String,
	pub event: String,
	pub variant: String,
	pub depth: u8,
	pub columns: Columns,
}

#[derive(Clone, Debug)]
pub struct CapturedInvocation {
	pub sequence: u64,
	pub namespace: String,
	pub handler: String,
	pub event: String,
	pub variant: String,
	pub duration_ns: u64,
	pub outcome: String,
	pub message: String,
}

pub struct TestingChanged {
	pub shape_type: &'static str,
}

impl TestingChanged {
	pub fn new(shape_type: &'static str) -> Self {
		Self {
			shape_type,
		}
	}
}
