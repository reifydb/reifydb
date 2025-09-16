// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey,
	interface::{FlowNodeId, Transaction},
	row::EncodedRow,
	util::{CowVec, encoding::keycode::KeySerializer},
};
use reifydb_engine::StandardCommandTransaction;
use reifydb_type::RowNumber;

use crate::operator::transform::TransformOperator;

/// Provides stable row numbers for keys with automatic Insert/Update detection
///
/// This component maintains:
/// - A sequential counter for generating new row numbers
/// - A mapping from keys to their assigned row numbers
///
/// When a key is seen for the first time, it gets a new row number and returns
/// true. When a key is seen again, it returns the existing row number and
/// false.
pub struct RowNumberProvider {
	node: FlowNodeId,
}

impl RowNumberProvider {
	/// Create a new RowNumberProvider for the given node
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
		}
	}

	/// Get or create a RowNumber for a given key
	/// Returns (RowNumber, is_new) where is_new indicates if it was newly
	/// created
	pub fn get_or_create_row_number<
		T: Transaction,
		O: TransformOperator<T>,
	>(
		&self,
		operator: &O,
		txn: &mut StandardCommandTransaction<T>,
		key: &EncodedKey,
	) -> crate::Result<(RowNumber, bool)> {
		// Check if we already have a row number for this key
		let map_key = self.make_map_key(key);
		let encoded_map_key = EncodedKey::new(map_key);

		let existing_row = operator.get(txn, &encoded_map_key)?;

		if !existing_row.as_ref().is_empty() {
			// Key exists, return existing row number
			let bytes = existing_row.as_ref();
			if bytes.len() >= 8 {
				let row_num = u64::from_be_bytes([
					bytes[0], bytes[1], bytes[2], bytes[3],
					bytes[4], bytes[5], bytes[6], bytes[7],
				]);
				return Ok((RowNumber(row_num), false));
			}
		}

		// Key doesn't exist, generate a new row number
		let counter = self.load_counter(operator, txn)?;
		let new_row_number = RowNumber(counter);

		// Save the new counter value
		self.save_counter(operator, txn, counter + 1)?;

		// Save the mapping from key to row number
		let row_num_bytes = counter.to_be_bytes().to_vec();
		operator.set(
			txn,
			&encoded_map_key,
			EncodedRow(CowVec::new(row_num_bytes)),
		)?;

		Ok((new_row_number, true))
	}

	/// Load the current counter value
	fn load_counter<T: Transaction, O: TransformOperator<T>>(
		&self,
		operator: &O,
		txn: &mut StandardCommandTransaction<T>,
	) -> crate::Result<u64> {
		let key = self.make_counter_key();
		let encoded_key = EncodedKey::new(key);
		let state_row = operator.get(txn, &encoded_key)?;

		if state_row.as_ref().is_empty() {
			// First time, start at 1
			Ok(1)
		} else {
			// Parse the stored counter
			let bytes = state_row.as_ref();
			if bytes.len() >= 8 {
				Ok(u64::from_be_bytes([
					bytes[0], bytes[1], bytes[2], bytes[3],
					bytes[4], bytes[5], bytes[6], bytes[7],
				]))
			} else {
				Ok(1)
			}
		}
	}

	/// Save the counter value
	fn save_counter<T: Transaction, O: TransformOperator<T>>(
		&self,
		operator: &O,
		txn: &mut StandardCommandTransaction<T>,
		counter: u64,
	) -> crate::Result<()> {
		let key = self.make_counter_key();
		let encoded_key = EncodedKey::new(key);
		let value =
			EncodedRow(CowVec::new(counter.to_be_bytes().to_vec()));
		operator.set(txn, &encoded_key, value)?;
		Ok(())
	}

	/// Create a key for the counter, including node_id
	fn make_counter_key(&self) -> Vec<u8> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(self.node.0);
		serializer.extend_bytes(b"__COUNTER");
		serializer.finish()
	}

	/// Create a mapping key for a given encoded key, including node_id
	fn make_map_key(&self, key: &EncodedKey) -> Vec<u8> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(self.node.0);
		serializer.extend_bytes(b"__MAP");
		serializer.extend_bytes(key.as_ref());
		serializer.finish()
	}
}
