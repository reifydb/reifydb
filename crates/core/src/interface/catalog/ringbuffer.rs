// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::LazyLock;

use reifydb_codec::encoded::{
	row::EncodedRow,
	shape::{RowShape, RowShapeField},
};
use reifydb_value::value::{Value, value_type::ValueType};
use serde::{Deserialize, Serialize};

use crate::interface::catalog::{
	column::Column,
	id::{NamespaceId, RingBufferId},
	key::PrimaryKey,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RingBuffer {
	pub id: RingBufferId,
	pub namespace: NamespaceId,
	pub name: String,
	pub columns: Vec<Column>,
	pub capacity: u64,
	pub primary_key: Option<PrimaryKey>,
	pub partition_by: Vec<String>,
	pub underlying: bool,
}

impl RingBuffer {
	pub fn name(&self) -> &str {
		&self.name
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RingBufferMetadata {
	pub id: RingBufferId,
	pub capacity: u64,
	pub count: u64,
	pub head: u64,
	pub tail: u64,
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

mod metadata_shape {
	use super::*;

	pub(super) const ID: usize = 0;
	pub(super) const CAPACITY: usize = 1;
	pub(super) const HEAD: usize = 2;
	pub(super) const TAIL: usize = 3;
	pub(super) const COUNT: usize = 4;

	pub(super) static SHAPE: LazyLock<RowShape> = LazyLock::new(|| {
		RowShape::new(vec![
			RowShapeField::unconstrained("id", ValueType::Uint8),
			RowShapeField::unconstrained("capacity", ValueType::Uint8),
			RowShapeField::unconstrained("head", ValueType::Uint8),
			RowShapeField::unconstrained("tail", ValueType::Uint8),
			RowShapeField::unconstrained("count", ValueType::Uint8),
		])
	});
}

pub fn encode_ringbuffer_metadata(metadata: &RingBufferMetadata) -> EncodedRow {
	let mut row = metadata_shape::SHAPE.allocate();
	metadata_shape::SHAPE.set_u64(&mut row, metadata_shape::ID, metadata.id);
	metadata_shape::SHAPE.set_u64(&mut row, metadata_shape::CAPACITY, metadata.capacity);
	metadata_shape::SHAPE.set_u64(&mut row, metadata_shape::HEAD, metadata.head);
	metadata_shape::SHAPE.set_u64(&mut row, metadata_shape::TAIL, metadata.tail);
	metadata_shape::SHAPE.set_u64(&mut row, metadata_shape::COUNT, metadata.count);
	row
}

pub fn decode_ringbuffer_metadata(row: &EncodedRow) -> RingBufferMetadata {
	RingBufferMetadata {
		id: RingBufferId(metadata_shape::SHAPE.get_u64(row, metadata_shape::ID)),
		capacity: metadata_shape::SHAPE.get_u64(row, metadata_shape::CAPACITY),
		count: metadata_shape::SHAPE.get_u64(row, metadata_shape::COUNT),
		head: metadata_shape::SHAPE.get_u64(row, metadata_shape::HEAD),
		tail: metadata_shape::SHAPE.get_u64(row, metadata_shape::TAIL),
	}
}
