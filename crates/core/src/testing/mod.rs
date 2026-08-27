// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;

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
	pub duration: Duration,
	pub outcome: String,
	pub message: String,
}

pub struct TestingChanged {
	pub object_type: &'static str,
}

impl TestingChanged {
	pub fn new(object_type: &'static str) -> Self {
		Self {
			object_type,
		}
	}
}
