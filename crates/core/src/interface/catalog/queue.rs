// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::duration::Duration;
use serde::{Deserialize, Serialize};

use crate::{
	common::TimeSource,
	interface::catalog::{
		column::Column,
		id::{NamespaceId, QueueId},
	},
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Queue {
	pub id: QueueId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<Column>,
	pub dispatch: QueueDispatch,
	pub deduplicate: Option<QueueDeduplicate>,
	pub retention: QueueRetention,
	pub retry: QueueRetry,
	pub time: TimeSource,
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

	pub fn partitions(&self) -> u16 {
		self.dispatch.partitions()
	}

	pub fn ordered_by(&self) -> Option<&str> {
		self.dispatch.ordered_by()
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum QueueDispatch {
	Fifo {
		partitions: u16,
		ordered_by: Option<String>,
	},
}

impl QueueDispatch {
	pub const TAG_FIFO: u8 = 0;

	pub fn tag(&self) -> u8 {
		match self {
			Self::Fifo {
				..
			} => Self::TAG_FIFO,
		}
	}

	pub fn partitions(&self) -> u16 {
		match self {
			Self::Fifo {
				partitions,
				..
			} => *partitions,
		}
	}

	pub fn ordered_by(&self) -> Option<&str> {
		match self {
			Self::Fifo {
				ordered_by,
				..
			} => ordered_by.as_deref(),
		}
	}
}

impl Default for QueueDispatch {
	fn default() -> Self {
		Self::Fifo {
			partitions: Queue::DEFAULT_PARTITIONS,
			ordered_by: None,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueueDeduplicate {
	pub by: Vec<String>,
	pub ttl: Duration,
}

impl QueueDeduplicate {
	pub fn is_forever(&self) -> bool {
		self.ttl == Duration::MAX
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
