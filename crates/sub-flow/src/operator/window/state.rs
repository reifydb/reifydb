// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::{from_bytes, to_stdvec};
use reifydb_core::{encoded::key::EncodedKey, internal, util::encoding::keycode::serializer::KeySerializer};
use reifydb_runtime::hash::Hash128;
use reifydb_value::{
	Result,
	error::Error,
	value::{blob::Blob, row_number::RowNumber},
};

use super::operator::WindowOperator;
use crate::{operator::stateful::window::WindowStateful, transaction::FlowTransaction};

impl WindowOperator {
	pub(super) fn create_rolling_meta_key(&self, group_hash: Hash128) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"rwm:");
		serializer.extend_u128(group_hash);
		serializer.finish()
	}

	pub(super) fn create_row_index_key(&self, group_hash: Hash128, row_number: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"idx:");
		serializer.extend_u128(group_hash);
		serializer.extend_u64(row_number.0);
		serializer.finish()
	}

	pub fn store_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
		window_id: u64,
	) -> Result<()> {
		let index_key = self.create_row_index_key(group_hash, row_number);
		let mut window_ids = self.lookup_row_index(txn, group_hash, row_number)?;
		if !window_ids.contains(&window_id) {
			window_ids.push(window_id);
		}
		let serialized = to_stdvec(&window_ids)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize row index: {}", e))))?;
		let mut state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut state_row, 0, &blob);
		self.save_state(txn, &index_key, state_row)
	}

	pub(super) fn lookup_row_index(
		&self,
		txn: &mut FlowTransaction,
		group_hash: Hash128,
		row_number: RowNumber,
	) -> Result<Vec<u64>> {
		let index_key = self.create_row_index_key(group_hash, row_number);
		let state_row = self.load_state(txn, &index_key)?;
		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(Vec::new());
		}
		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(Vec::new());
		}
		let window_ids: Vec<u64> = from_bytes(blob.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize row index: {}", e))))?;
		Ok(window_ids)
	}

	pub fn get_and_increment_global_count(&self, txn: &mut FlowTransaction, group_hash: Hash128) -> Result<u64> {
		let count_key = self.create_count_key(group_hash);
		let count_row = self.load_state(txn, &count_key)?;

		let current_count = if count_row.is_empty() || !count_row.is_defined(0) {
			0
		} else {
			let blob = self.layout.get_blob(&count_row, 0);
			if blob.is_empty() {
				0
			} else {
				from_bytes(blob.as_ref()).unwrap_or(0)
			}
		};

		let new_count = current_count + 1;

		let serialized = to_stdvec(&new_count)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize count: {}", e))))?;

		let mut count_state_row = self.layout.allocate();
		let blob = Blob::from(serialized);
		self.layout.set_blob(&mut count_state_row, 0, &blob);

		self.save_state(txn, &count_key, count_state_row)?;

		Ok(current_count)
	}

	pub fn create_count_key(&self, group_hash: Hash128) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(32);
		serializer.extend_bytes(b"cnt:");
		serializer.extend_u128(group_hash);
		serializer.finish()
	}

	pub(super) fn event_watermark_key(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(8);
		serializer.extend_bytes(b"wmk:");
		serializer.finish()
	}

	pub(super) fn load_event_watermark(&self, txn: &mut FlowTransaction) -> Result<u64> {
		let key = self.event_watermark_key();
		let state_row = self.load_state(txn, &key)?;
		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(0);
		}
		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(0);
		}
		Ok(from_bytes(blob.as_ref()).unwrap_or(0))
	}

	pub(super) fn advance_event_watermark(&self, txn: &mut FlowTransaction, coord: u64) -> Result<()> {
		if coord > self.load_event_watermark(txn)? {
			let serialized = to_stdvec(&coord).map_err(|e| {
				Error(Box::new(internal!("Failed to serialize event watermark: {}", e)))
			})?;
			let mut state_row = self.layout.allocate();
			let blob = Blob::from(serialized);
			self.layout.set_blob(&mut state_row, 0, &blob);
			self.save_state(txn, &self.event_watermark_key(), state_row)?;
		}
		Ok(())
	}

	pub(super) fn event_time_cutoff(&self, txn: &mut FlowTransaction, span_ms: u64) -> Result<u64> {
		Ok(self.load_event_watermark(txn)?.saturating_sub(span_ms))
	}

	pub(super) fn expiry_watermark_key(&self) -> EncodedKey {
		let mut serializer = KeySerializer::with_capacity(8);
		serializer.extend_bytes(b"xwm:");
		serializer.finish()
	}

	pub(super) fn load_expiry_watermark(&self, txn: &mut FlowTransaction) -> Result<u64> {
		let key = self.expiry_watermark_key();
		let state_row = self.load_state(txn, &key)?;
		if state_row.is_empty() || !state_row.is_defined(0) {
			return Ok(0);
		}
		let blob = self.layout.get_blob(&state_row, 0);
		if blob.is_empty() {
			return Ok(0);
		}
		Ok(from_bytes(blob.as_ref()).unwrap_or(0))
	}

	pub(super) fn advance_expiry_watermark(&self, txn: &mut FlowTransaction, coord: u64) -> Result<()> {
		if coord > self.load_expiry_watermark(txn)? {
			let serialized = to_stdvec(&coord).map_err(|e| {
				Error(Box::new(internal!("Failed to serialize expiry watermark: {}", e)))
			})?;
			let mut state_row = self.layout.allocate();
			let blob = Blob::from(serialized);
			self.layout.set_blob(&mut state_row, 0, &blob);
			self.save_state(txn, &self.expiry_watermark_key(), state_row)?;
		}
		Ok(())
	}
}
