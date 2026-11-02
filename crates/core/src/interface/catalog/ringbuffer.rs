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
	key::{any::TaggedKey, ringbuffer::ring_order_sequence},
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
	pub filled: u64,
	pub head: u64,
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
			filled: 0,
			head: 0,
		}
	}

	pub fn unpartitioned() -> Self {
		Self::reserved(UNPARTITIONED_BASE_RN)
	}

	pub fn is_empty(&self) -> bool {
		self.count == 0
	}

	pub fn holds(&self, row: RowNumber, capacity: u64) -> bool {
		row.0 >= self.base_rn && row.0 - self.base_rn < capacity
	}
}

const RINGBUFFER_METADATA_WIDTH: usize = 40;

pub fn encode_ringbuffer_metadata(metadata: &RingBufferMetadata) -> EncodedPodRow {
	let mut bytes = Vec::with_capacity(RINGBUFFER_METADATA_WIDTH);
	bytes.extend_from_slice(&metadata.base_rn.to_be_bytes());
	bytes.extend_from_slice(&metadata.next_sequence.to_be_bytes());
	bytes.extend_from_slice(&metadata.count.to_be_bytes());
	bytes.extend_from_slice(&metadata.filled.to_be_bytes());
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
		base_rn: u64::from_be_bytes(bytes[0..8].try_into().unwrap()),
		next_sequence: u64::from_be_bytes(bytes[8..16].try_into().unwrap()),
		count: u64::from_be_bytes(bytes[16..24].try_into().unwrap()),
		filled: u64::from_be_bytes(bytes[24..32].try_into().unwrap()),
		head: u64::from_be_bytes(bytes[32..40].try_into().unwrap()),
	})
}

pub fn encode_ringbuffer_order(row: RowNumber) -> EncodedPodRow {
	EncodedPodRow::new(&row.0.to_be_bytes())
}

pub fn decode_ringbuffer_order(key: &TaggedKey, value: &EncodedPodRow) -> Result<(u64, RowNumber)> {
	let Some(sequence) = ring_order_sequence(key) else {
		return_internal_error!("Ring buffer order scan returned a key that is not an order key: {:?}", key)
	};
	match <[u8; 8]>::try_from(value.body()) {
		Ok(bytes) => Ok((sequence, RowNumber(u64::from_be_bytes(bytes)))),
		Err(_) => return_internal_error!(
			"Ring buffer order entry for sequence {} is {} bytes wide, expected 8. This indicates a corrupt ring index.",
			sequence,
			value.body().len()
		),
	}
}

pub fn encode_ringbuffer_free_page(rows: &[RowNumber]) -> EncodedPodRow {
	let mut bytes = Vec::with_capacity(rows.len() * 8);
	for row in rows {
		bytes.extend_from_slice(&row.0.to_be_bytes());
	}
	EncodedPodRow::new(&bytes)
}

pub fn decode_ringbuffer_free_page(row: &EncodedPodRow) -> Result<Vec<RowNumber>> {
	let bytes = row.body();
	if bytes.len() % 8 != 0 {
		return_internal_error!(
			"Ring buffer free page is {} bytes wide, not a multiple of 8. This indicates a corrupt index row.",
			bytes.len()
		)
	}
	Ok(bytes.chunks_exact(8).map(|chunk| RowNumber(u64::from_be_bytes(chunk.try_into().unwrap()))).collect())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::{
		interface::catalog::{id::RingBufferId, storage::StorageId},
		key::ringbuffer::ring_order_key,
	};

	#[test]
	fn base_sequence_and_count_survive_a_round_trip_because_they_address_every_slot() {
		let metadata = RingBufferMetadata {
			base_rn: 3,
			next_sequence: 11,
			count: 7,
			filled: 9,
			head: 4,
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
			filled: u64::MAX,
			head: u64::MAX,
		};

		assert_eq!(decode_ringbuffer_metadata(&encode_ringbuffer_metadata(&metadata)).unwrap(), metadata);
	}

	#[test]
	fn a_row_of_the_wrong_width_is_rejected_rather_than_misread_as_eviction_bounds() {
		assert!(decode_ringbuffer_metadata(&EncodedPodRow::new(&[0u8; 32])).is_err());
		assert!(decode_ringbuffer_metadata(&EncodedPodRow::new(&[0u8; 39])).is_err());
		assert!(decode_ringbuffer_metadata(&EncodedPodRow::new(&[0u8; 41])).is_err());
	}

	#[test]
	fn an_order_row_keeps_its_eight_big_endian_bytes_and_any_other_width_is_corrupt() {
		// The order row format must not move with the free list, and a wider row must fail rather than
		// truncate.
		let key: TaggedKey = ring_order_key(StorageId::ringbuffer(RingBufferId(1)), None, 7);
		let row = RowNumber(0x0102_0304_0506_0708);
		let encoded = encode_ringbuffer_order(row);
		assert_eq!(encoded.body(), &[1, 2, 3, 4, 5, 6, 7, 8]);
		assert_eq!(decode_ringbuffer_order(&key, &encoded).unwrap(), (7, row));
		assert!(decode_ringbuffer_order(&key, &encode_ringbuffer_free_page(&[row, row])).is_err());
		assert!(decode_ringbuffer_order(&key, &encode_ringbuffer_free_page(&[])).is_err());
	}

	#[test]
	fn a_free_page_keeps_its_slots_in_stack_order_and_a_torn_page_is_corrupt() {
		// Reordered slots would change which row number is reused next.
		let rows = [RowNumber(3), RowNumber(1), RowNumber(2)];
		assert_eq!(decode_ringbuffer_free_page(&encode_ringbuffer_free_page(&rows)).unwrap(), rows.to_vec());
		assert!(decode_ringbuffer_free_page(&EncodedPodRow::new(&[0; 9])).is_err());
	}
}
