// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::Value;
use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	column::ColumnDef,
	id::{NamespaceId, RingBufferId},
	key::PrimaryKeyDef,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RingBufferDef {
	pub id: RingBufferId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<ColumnDef>,
	pub capacity: u64,
	pub primary_key: Option<PrimaryKeyDef>,
	pub partition_by: Vec<String>,
}

impl RingBufferDef {
	pub fn name(&self) -> &str {
		&self.name
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RingBufferMetadata {
	pub id: RingBufferId,
	pub capacity: u64,
	pub count: u64,
	pub head: u64, // Position of oldest entry
	pub tail: u64, // Position for next insert
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedMetadata {
	pub metadata: RingBufferMetadata,
	pub partition_values: Vec<Value>,
}

impl RingBufferMetadata {
	pub fn new(buffer_id: RingBufferId, capacity: u64) -> Self {
		Self {
			id: buffer_id,
			capacity,
			count: 0,
			head: 1,
			tail: 1,
		}
	}

	pub fn is_full(&self) -> bool {
		self.count >= self.capacity
	}

	pub fn is_empty(&self) -> bool {
		self.count == 0
	}
}
