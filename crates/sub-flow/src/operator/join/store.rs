use bincode::{
	config::standard,
	serde::{decode_from_slice, encode_to_vec},
};
use reifydb_core::{
	Error,
	interface::{CommandTransaction, FlowNodeId},
	value::encoded::EncodedValuesLayout,
};
use reifydb_engine::StandardCommandTransaction;
use reifydb_hash::Hash128;
use reifydb_type::{Blob, Type, internal_error};

use super::{JoinSide, JoinSideEntry};
use crate::operator::stateful::{state_get, state_remove, state_set};

/// A key-value store backed by the stateful storage system
pub(crate) struct Store {
	node_id: FlowNodeId,
	prefix: Vec<u8>,
}

impl Store {
	pub(crate) fn new(node_id: FlowNodeId, side: JoinSide) -> Self {
		// Use different prefixes for left and right stores
		let prefix = match side {
			JoinSide::Left => vec![0x01],
			JoinSide::Right => vec![0x02],
		};
		Self {
			node_id,
			prefix,
		}
	}

	fn make_key(&self, hash: &Hash128) -> reifydb_core::EncodedKey {
		let mut key_bytes = self.prefix.clone();
		// Hash128 is a tuple struct containing u128
		key_bytes.extend_from_slice(&hash.0.to_le_bytes());
		reifydb_core::EncodedKey::new(key_bytes)
	}

	pub(crate) fn get(
		&self,
		txn: &mut StandardCommandTransaction,
		hash: &Hash128,
	) -> crate::Result<Option<JoinSideEntry>> {
		let key = self.make_key(hash);
		match state_get(self.node_id, txn, &key)? {
			Some(row) => {
				// Deserialize JoinSideEntry from the encoded
				let layout = EncodedValuesLayout::new(&[Type::Blob]);
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

	pub(crate) fn set(
		&self,
		txn: &mut StandardCommandTransaction,
		hash: &Hash128,
		entry: &JoinSideEntry,
	) -> crate::Result<()> {
		let key = self.make_key(hash);

		// Serialize JoinSideEntry
		let config = standard();
		let serialized = encode_to_vec(entry, config)
			.map_err(|e| Error(internal_error!("Failed to serialize JoinSideEntry: {}", e)))?;

		// Store as a blob in an EncodedRow
		let layout = EncodedValuesLayout::new(&[Type::Blob]);
		let mut row = layout.allocate();
		let blob = Blob::from(serialized);
		layout.set_blob(&mut row, 0, &blob);

		state_set(self.node_id, txn, &key, row)?;
		Ok(())
	}

	pub(crate) fn contains_key(&self, txn: &mut StandardCommandTransaction, hash: &Hash128) -> crate::Result<bool> {
		let key = self.make_key(hash);
		Ok(state_get(self.node_id, txn, &key)?.is_some())
	}

	pub(crate) fn remove(&self, txn: &mut StandardCommandTransaction, hash: &Hash128) -> crate::Result<()> {
		let key = self.make_key(hash);
		state_remove(self.node_id, txn, &key)?;
		Ok(())
	}

	pub(crate) fn get_or_insert_with<T: CommandTransaction, F>(
		&self,
		txn: &mut StandardCommandTransaction,
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

	pub(crate) fn update_entry<T: CommandTransaction, F>(
		&self,
		txn: &mut StandardCommandTransaction,
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
