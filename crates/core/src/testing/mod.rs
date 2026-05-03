// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

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
