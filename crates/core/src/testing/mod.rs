// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::{Value, duration::Duration};

use crate::{
	common::CommitVersion,
	interface::catalog::config::{ConfigKey, GetConfig},
	value::column::columns::Columns,
};

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

pub struct ProfileConfig;

impl GetConfig for ProfileConfig {
	fn get_config(&self, key: ConfigKey) -> Value {
		key.default_value()
	}

	fn get_config_at(&self, key: ConfigKey, _version: CommitVersion) -> Value {
		key.default_value()
	}
}
