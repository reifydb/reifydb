// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::interface::{ColumnDef, NamespaceId, PrimaryKeyDef, RingBufferId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RingBufferDef {
	pub id: RingBufferId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<ColumnDef>,
	pub capacity: u64,
	pub primary_key: Option<PrimaryKeyDef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RingBufferMetadata {
	pub id: RingBufferId,
	pub capacity: u64,
	pub current_size: u64,
	pub head: u64, // Position of oldest entry
	pub tail: u64, // Position for next insert
}

impl RingBufferMetadata {
	pub fn new(buffer_id: RingBufferId, capacity: u64) -> Self {
		Self {
			id: buffer_id,
			capacity,
			current_size: 0,
			head: 0,
			tail: 0,
		}
	}

	pub fn is_full(&self) -> bool {
		self.current_size >= self.capacity
	}

	pub fn is_empty(&self) -> bool {
		self.current_size == 0
	}
}
