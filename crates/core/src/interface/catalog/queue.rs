// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;
use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	column::Column,
	id::{NamespaceId, QueueId},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Queue {
	pub id: QueueId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<Column>,
	pub partitions: u16,
	pub ordered_by: Option<String>,
	pub retention: QueueRetention,
	pub retry: QueueRetry,
	pub underlying: bool,
}

impl Queue {
	pub const DEFAULT_PARTITIONS: u16 = 16;
	pub const MIN_PARTITIONS: u16 = 1;
	pub const MAX_PARTITIONS: u16 = 1024;
	pub const DEFAULT_RETRY_ATTEMPTS: u32 = 5;
	pub const DEFAULT_RETRY_BACKOFF: Duration = Duration::from_seconds_const(10);

	pub fn name(&self) -> &str {
		&self.name
	}
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueueRetention {
	pub done: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueRetry {
	pub attempts: u32,
	pub backoff: Duration,
}

impl Default for QueueRetry {
	fn default() -> Self {
		Self {
			attempts: Queue::DEFAULT_RETRY_ATTEMPTS,
			backoff: Queue::DEFAULT_RETRY_BACKOFF,
		}
	}
}
