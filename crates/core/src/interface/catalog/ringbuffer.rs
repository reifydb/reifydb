// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_value::{Result, value::partition::Partition};
use serde::{Deserialize, Serialize};

use crate::{
	common::TimeSource,
	interface::catalog::{
		column::Column,
		id::{NamespaceId, RingBufferId},
		key::PrimaryKey,
	},
	return_internal_error,
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
	pub time: TimeSource,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvictionVisibility {
	#[default]
	Visible,
	Silent,
}

impl RingBuffer {
	pub fn name(&self) -> &str {
		&self.name
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RingBufferMetadata {
	pub count: u64,
	pub head: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedMetadata {
	pub metadata: RingBufferMetadata,
	pub partition: Option<Partition>,
}

impl RingBufferMetadata {
	pub fn is_empty(&self) -> bool {
		self.count == 0
	}
}

const RINGBUFFER_METADATA_WIDTH: usize = 16;

pub fn encode_ringbuffer_metadata(metadata: &RingBufferMetadata) -> EncodedPodRow {
	let mut bytes = Vec::with_capacity(RINGBUFFER_METADATA_WIDTH);
	bytes.extend_from_slice(&metadata.count.to_be_bytes());
	bytes.extend_from_slice(&metadata.head.to_be_bytes());
	EncodedPodRow::new(&bytes)
}

pub fn decode_ringbuffer_metadata(row: &EncodedPodRow) -> Result<RingBufferMetadata> {
	let bytes = row.body();
	if bytes.len() != RINGBUFFER_METADATA_WIDTH {
		return_internal_error!(
			"Ring buffer metadata is {} bytes wide, expected {}. This indicates a corrupt metadata row.",
			bytes.len(),
			RINGBUFFER_METADATA_WIDTH
		)
	}
	Ok(RingBufferMetadata {
		count: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
		head: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn count_and_head_survive_a_round_trip() {
		let metadata = RingBufferMetadata {
			count: 7,
			head: 4,
		};

		let row = encode_ringbuffer_metadata(&metadata);

		assert_eq!(row.len(), RINGBUFFER_METADATA_WIDTH);
		assert_eq!(decode_ringbuffer_metadata(&row).unwrap(), metadata);
	}

	#[test]
	fn extreme_fields_round_trip_rather_than_being_normalised() {
		let metadata = RingBufferMetadata {
			count: u64::MAX,
			head: u64::MAX,
		};

		assert_eq!(decode_ringbuffer_metadata(&encode_ringbuffer_metadata(&metadata)).unwrap(), metadata);
	}

	#[test]
	fn a_row_of_the_wrong_width_is_rejected_rather_than_misread_as_eviction_bounds() {
		// An old 40-byte row must fail loud, never decode as a new one.
		assert!(decode_ringbuffer_metadata(&EncodedPodRow::new(&[0u8; RINGBUFFER_METADATA_WIDTH - 1])).is_err());
		assert!(decode_ringbuffer_metadata(&EncodedPodRow::new(&[0u8; RINGBUFFER_METADATA_WIDTH + 1])).is_err());
		assert!(decode_ringbuffer_metadata(&EncodedPodRow::new(&[0u8; 40])).is_err());
	}
}
