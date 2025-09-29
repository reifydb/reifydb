use std::marker::PhantomData;

use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	EncodedKey, Error,
	interface::{FlowNodeId, Transaction},
	value::row::EncodedRowLayout,
};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;
use reifydb_type::{Blob, RowNumber, Type, internal_error};

use super::{JoinSide, JoinSideEntry};
use crate::operator::stateful::{state_get, state_remove, state_set};

/// A key-value store backed by the stateful storage system
pub(crate) struct Store<T> {
	node_id: FlowNodeId,
	prefix: Vec<u8>,
	_phantom: PhantomData<T>,
}

impl Store<JoinSideEntry> {
	pub(crate) fn new(node_id: FlowNodeId, side: JoinSide) -> Self {
		// Use different prefixes for left and right stores
		let prefix = match side {
			JoinSide::Left => vec![0x01],
			JoinSide::Right => vec![0x02],
		};
		Self {
			node_id,
			prefix,
			_phantom: PhantomData,
		}
	}

	fn make_key(&self, hash: &Hash128) -> reifydb_core::EncodedKey {
		let mut key_bytes = self.prefix.clone();
		// Hash128 is a tuple struct containing u128
		key_bytes.extend_from_slice(&hash.0.to_le_bytes());
		reifydb_core::EncodedKey::new(key_bytes)
	}

	pub(crate) fn get<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
	) -> crate::Result<Option<JoinSideEntry>> {
		let key = self.make_key(hash);
		match state_get(self.node_id, txn, &key)? {
			Some(row) => {
				// Deserialize JoinSideEntry from the row
				let layout = EncodedRowLayout::new(&[Type::Blob]);
				let blob = layout.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(None);
				}
				let config = standard();
				let (entry, _): (JoinSideEntry, usize) = decode_from_slice(blob.as_ref(), config)
					.map_err(|e| {
						Error(internal_error!("Failed to deserialize JoinSideEntry: {}", e))
					})?;
				Ok(Some(entry))
			}
			None => Ok(None),
		}
	}

	pub(crate) fn set<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
		entry: &JoinSideEntry,
	) -> crate::Result<()> {
		let key = self.make_key(hash);

		// Serialize JoinSideEntry
		let config = standard();
		let serialized = encode_to_vec(entry, config)
			.map_err(|e| Error(internal_error!("Failed to serialize JoinSideEntry: {}", e)))?;

		// Store as a blob in an EncodedRow
		let layout = EncodedRowLayout::new(&[Type::Blob]);
		let mut row = layout.allocate_row();
		let blob = Blob::from(serialized);
		layout.set_blob(&mut row, 0, &blob);

		state_set(self.node_id, txn, &key, row)?;
		Ok(())
	}

	pub(crate) fn contains_key<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
	) -> crate::Result<bool> {
		let key = self.make_key(hash);
		Ok(state_get(self.node_id, txn, &key)?.is_some())
	}

	pub(crate) fn remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
	) -> crate::Result<()> {
		let key = self.make_key(hash);
		state_remove(self.node_id, txn, &key)?;
		Ok(())
	}

	pub(crate) fn get_or_insert_with<T: Transaction, F>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
		f: F,
	) -> crate::Result<JoinSideEntry>
	where
		F: FnOnce() -> JoinSideEntry,
	{
		if let Some(entry) = self.get(txn, hash)? {
			Ok(entry)
		} else {
			let entry = f();
			self.set(txn, hash, &entry)?;
			Ok(entry)
		}
	}

	pub(crate) fn update_entry<T: Transaction, F>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		hash: &Hash128,
		f: F,
	) -> crate::Result<()>
	where
		F: FnOnce(&mut JoinSideEntry),
	{
		if let Some(mut entry) = self.get(txn, hash)? {
			f(&mut entry);
			self.set(txn, hash, &entry)?;
		}
		Ok(())
	}
}

/// Tracks undefined joins for left rows persistently
pub(crate) struct UndefinedTracker {
	node_id: FlowNodeId,
}

impl UndefinedTracker {
	pub(crate) fn new(node_id: FlowNodeId) -> Self {
		Self {
			node_id,
		}
	}

	fn make_key(&self, row_number: RowNumber) -> EncodedKey {
		// Use prefix 0x03 for undefined tracking
		let mut key_bytes = vec![0x03];
		key_bytes.extend_from_slice(&row_number.0.to_le_bytes());
		EncodedKey::new(key_bytes)
	}

	pub(crate) fn insert<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		row_number: RowNumber,
	) -> crate::Result<()> {
		let key = self.make_key(row_number);
		// Store a simple marker (boolean true)
		let layout = EncodedRowLayout::new(&[Type::Boolean]);
		let mut row = layout.allocate_row();
		layout.set_bool(&mut row, 0, true);
		state_set(self.node_id, txn, &key, row)?;
		Ok(())
	}

	pub(crate) fn remove<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		row_number: RowNumber,
	) -> crate::Result<bool> {
		let key = self.make_key(row_number);
		// Check if it exists before removing
		let exists = state_get(self.node_id, txn, &key)?.is_some();
		if exists {
			state_remove(self.node_id, txn, &key)?;
		}
		Ok(exists)
	}

	pub(crate) fn contains<T: Transaction>(
		&self,
		txn: &mut StandardCommandTransaction<T>,
		row_number: RowNumber,
	) -> crate::Result<bool> {
		let key = self.make_key(row_number);
		Ok(state_get(self.node_id, txn, &key)?.is_some())
	}
}
