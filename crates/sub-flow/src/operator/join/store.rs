// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{key::EncodedKey, schema::Schema},
	interface::catalog::flow::FlowNodeId,
	internal,
};
use reifydb_runtime::hash::Hash128;
use reifydb_type::{
	Result,
	error::Error,
	value::{blob::Blob, r#type::Type},
};

use super::state::{JoinSide, JoinSideEntry};
use crate::{
	operator::stateful::utils::{state_get, state_remove, state_set},
	transaction::FlowTransaction,
};

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

	fn make_key(&self, hash: &Hash128) -> EncodedKey {
		let mut key_bytes = self.prefix.clone();
		// Hash128 is a tuple struct containing u128
		key_bytes.extend_from_slice(&hash.0.to_le_bytes());
		EncodedKey::new(key_bytes)
	}

	pub(crate) fn get(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<Option<JoinSideEntry>> {
		let key = self.make_key(hash);
		match state_get(self.node_id, txn, &key)? {
			Some(row) => {
				// Deserialize JoinSideEntry from the encoded
				let schema = Schema::testing(&[Type::Blob]);
				let blob = schema.get_blob(&row, 0);
				if blob.is_empty() {
					return Ok(None);
				}
				let entry: JoinSideEntry = postcard::from_bytes(blob.as_ref())
					.map_err(|e| Error(internal!("Failed to deserialize JoinSideEntry: {}", e)))?;
				Ok(Some(entry))
			}
			None => Ok(None),
		}
	}

	pub(crate) fn set(&self, txn: &mut FlowTransaction, hash: &Hash128, entry: &JoinSideEntry) -> Result<()> {
		let key = self.make_key(hash);

		// Serialize JoinSideEntry
		let serialized = postcard::to_stdvec(entry)
			.map_err(|e| Error(internal!("Failed to serialize JoinSideEntry: {}", e)))?;

		// Store as a blob in an EncodedRow
		let schema = Schema::testing(&[Type::Blob]);
		let mut row = schema.allocate();
		let blob = Blob::from(serialized);
		schema.set_blob(&mut row, 0, &blob);

		state_set(self.node_id, txn, &key, row)?;
		Ok(())
	}

	pub(crate) fn contains_key(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<bool> {
		let key = self.make_key(hash);
		Ok(state_get(self.node_id, txn, &key)?.is_some())
	}

	pub(crate) fn remove(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<()> {
		let key = self.make_key(hash);
		state_remove(self.node_id, txn, &key)?;
		Ok(())
	}

	pub(crate) fn get_or_insert_with<F>(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		f: F,
	) -> Result<JoinSideEntry>
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

	pub(crate) fn update_entry<F>(&self, txn: &mut FlowTransaction, hash: &Hash128, f: F) -> Result<()>
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
