// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_value::{
	Result,
	value::{partition::Partition, row_number::RowNumber},
};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RingBufferMetadata {
	pub base_rn: u64,
	pub next_sequence: u64,
	pub count: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartitionedMetadata {
	pub metadata: RingBufferMetadata,
	pub partition: Option<Partition>,
}

pub const UNPARTITIONED_BASE_RN: u64 = 1;

impl RingBufferMetadata {
	pub fn reserved(base_rn: u64) -> Self {
		Self {
			base_rn,
			next_sequence: 0,
			count: 0,
		}
	}

	pub fn unpartitioned() -> Self {
		Self::reserved(UNPARTITIONED_BASE_RN)
	}

	pub fn is_empty(&self) -> bool {
		self.count == 0
	}

	pub fn row_of(&self, sequence: u64, capacity: u64) -> RowNumber {
		RowNumber(self.base_rn + sequence % capacity)
	}

	pub fn holds(&self, row: RowNumber, capacity: u64) -> bool {
		row.0 >= self.base_rn && row.0 - self.base_rn < capacity
	}

	pub fn oldest_sequence(&self, capacity: u64) -> u64 {
		self.next_sequence.saturating_sub(capacity)
	}

	pub fn oldest_first(&self, capacity: u64) -> impl DoubleEndedIterator<Item = (u64, RowNumber)> + '_ {
		(self.oldest_sequence(capacity)..self.next_sequence)
			.map(move |sequence| (sequence, self.row_of(sequence, capacity)))
	}
}

const RINGBUFFER_METADATA_WIDTH: usize = 24;

pub fn encode_ringbuffer_metadata(metadata: &RingBufferMetadata) -> EncodedPodRow {
	let mut bytes = Vec::with_capacity(RINGBUFFER_METADATA_WIDTH);
	bytes.extend_from_slice(&metadata.base_rn.to_be_bytes());
	bytes.extend_from_slice(&metadata.next_sequence.to_be_bytes());
	bytes.extend_from_slice(&metadata.count.to_be_bytes());
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
		base_rn: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
		next_sequence: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
		count: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn base_sequence_and_count_survive_a_round_trip_because_they_address_every_slot() {
		let metadata = RingBufferMetadata {
			base_rn: 3,
			next_sequence: 11,
			count: 7,
		};

		let row = encode_ringbuffer_metadata(&metadata);

		assert_eq!(row.len(), RINGBUFFER_METADATA_WIDTH);
		assert_eq!(decode_ringbuffer_metadata(&row).unwrap(), metadata);
	}

	#[test]
	fn extreme_fields_round_trip_rather_than_being_normalised() {
		let metadata = RingBufferMetadata {
			base_rn: 99,
			next_sequence: u64::MAX,
			count: 0,
		};

		assert_eq!(decode_ringbuffer_metadata(&encode_ringbuffer_metadata(&metadata)).unwrap(), metadata);
	}

	#[test]
	fn a_row_of_the_wrong_width_is_rejected_rather_than_misread_as_eviction_bounds() {
		assert!(decode_ringbuffer_metadata(&EncodedPodRow::new(&[0u8; 23])).is_err());
		assert!(decode_ringbuffer_metadata(&EncodedPodRow::new(&[0u8; 25])).is_err());
		assert!(decode_ringbuffer_metadata(&EncodedPodRow::new(&[0u8; 40])).is_err());
	}
}
