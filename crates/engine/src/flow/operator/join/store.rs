// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use postcard::{from_bytes, to_stdvec};
use reifydb_codec::{
	row::{
		operator::EncodedOperatorRow,
		shape::{RowFamily, RowShape, RowShapeField, fingerprint::RowShapeFingerprint},
	},
	key::encoded::{EncodedKey, EncodedKeyRange},
};
#[cfg(test)]
use reifydb_core::interface::catalog::config::{ConfigKey, GetConfig};
use reifydb_core::{common::CommitVersion, interface::catalog::flow::OperatorId};
use reifydb_value::value::value_type::ValueType;
use reifydb_value::{
	Result,
	error::Error,
	util::hash::Hash128,
	value::{blob::Blob, row_number::RowNumber},
};

use super::state::JoinSide;
use crate::flow::{
	error::FlowStateError,
	operator::stateful::utils::{
		state_drop, state_get, state_range, state_range_versioned, state_remove, state_set,
	},
	transaction::FlowTransaction,
};

const HASH_BYTES: usize = 16;
const ROW_NUMBER_BYTES: usize = 8;

pub(crate) struct Store {
	node_id: OperatorId,
	prefix: Vec<u8>,
	schema_prefix: u8,
}

impl Store {
	pub(crate) fn new(node_id: OperatorId, side: JoinSide) -> Self {
		let (prefix, schema_byte) = match side {
			JoinSide::Left => (vec![0x01], 0x03u8),
			JoinSide::Right => (vec![0x02], 0x04u8),
		};
		Self {
			node_id,
			prefix,
			schema_prefix: schema_byte,
		}
	}

	fn schema_key(&self, fingerprint: RowShapeFingerprint) -> EncodedKey {
		let mut bytes = Vec::with_capacity(1 + 8);
		bytes.push(self.schema_prefix);
		bytes.extend_from_slice(&fingerprint.to_le_bytes());
		EncodedKey::new(bytes)
	}

	fn hash_prefix(&self, hash: &Hash128) -> Vec<u8> {
		let mut bytes = Vec::with_capacity(self.prefix.len() + HASH_BYTES);
		bytes.extend_from_slice(&self.prefix);
		bytes.extend_from_slice(&hash.0.to_le_bytes());
		bytes
	}

	fn row_key(&self, hash: &Hash128, row_number: RowNumber) -> EncodedKey {
		let mut bytes = Vec::with_capacity(self.prefix.len() + HASH_BYTES + ROW_NUMBER_BYTES);
		bytes.extend_from_slice(&self.prefix);
		bytes.extend_from_slice(&hash.0.to_le_bytes());
		bytes.extend_from_slice(&row_number.0.to_be_bytes());
		EncodedKey::new(bytes)
	}

	pub(crate) fn put_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
		encoded: &EncodedOperatorRow,
	) -> Result<()> {
		let key = self.row_key(hash, row_number);
		state_set(self.node_id, txn, &key, encoded.clone())
	}

	pub(crate) fn get_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
	) -> Result<Option<EncodedOperatorRow>> {
		let key = self.row_key(hash, row_number);
		state_get(self.node_id, txn, &key)
	}

	pub(crate) fn update_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
		encoded: &EncodedOperatorRow,
	) -> Result<bool> {
		let key = self.row_key(hash, row_number);
		if state_get(self.node_id, txn, &key)?.is_none() {
			return Ok(false);
		}
		state_set(self.node_id, txn, &key, encoded.clone())?;
		Ok(true)
	}

	pub(crate) fn remove_row(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		row_number: RowNumber,
	) -> Result<bool> {
		let key = self.row_key(hash, row_number);
		let existed = state_get(self.node_id, txn, &key)?.is_some();
		if existed {
			state_remove(self.node_id, txn, &key)?;
		}
		Ok(existed)
	}

	pub(crate) fn evict_expired(
		&self,
		txn: &mut FlowTransaction,
		cutoff_version: CommitVersion,
		cursor: &mut Option<EncodedKey>,
		batch_size: usize,
	) -> Result<()> {
		let base = EncodedKeyRange::prefix(&self.prefix);
		let start = match cursor.clone() {
			Some(c) => Bound::Excluded(c),
			None => base.start.clone(),
		};
		let range = EncodedKeyRange::new(start, base.end.clone());
		let batch =
			state_range_versioned(self.node_id, txn, range).take(batch_size).collect::<Result<Vec<_>>>()?;
		let reached_end = batch.len() < batch_size;
		let last_key = batch.last().map(|(key, _, _)| key.clone());

		for (key, version, _row) in batch {
			if version > cutoff_version {
				continue;
			}
			state_drop(self.node_id, txn, &key)?;
		}

		*cursor = if reached_end {
			None
		} else {
			last_key
		};
		Ok(())
	}

	pub(crate) fn rows_for_key_block(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
		after: Option<&RowNumber>,
		limit: usize,
	) -> Result<Vec<(RowNumber, EncodedOperatorRow)>> {
		let prefix = self.hash_prefix(hash);
		let mut range = EncodedKeyRange::prefix(&prefix);
		if let Some(after) = after {
			range.start = Bound::Excluded(self.row_key(hash, *after));
		}
		let mut out = Vec::new();
		for entry in state_range(self.node_id, txn, range) {
			let (full_key, row) = entry?;
			if let Some(rn) = row_number_from_key(full_key.as_slice()) {
				out.push((rn, row));
				if out.len() >= limit {
					break;
				}
			}
		}
		Ok(out)
	}

	#[cfg(test)]
	pub(crate) fn rows_for_key(
		&self,
		txn: &mut FlowTransaction,
		hash: &Hash128,
	) -> Result<Vec<(RowNumber, EncodedOperatorRow)>> {
		let limit = txn.catalog().get_config_uint8(ConfigKey::FlowJoinProbeBlockSize) as usize;
		let mut out = Vec::new();
		let mut after: Option<RowNumber> = None;
		loop {
			let block = self.rows_for_key_block(txn, hash, after.as_ref(), limit)?;
			if block.is_empty() {
				break;
			}
			let last = block.last().unwrap().0;
			let exhausted = block.len() < limit;
			out.extend(block);
			if exhausted {
				break;
			}
			after = Some(last);
		}
		Ok(out)
	}

	pub(crate) fn contains_key(&self, txn: &mut FlowTransaction, hash: &Hash128) -> Result<bool> {
		let prefix = self.hash_prefix(hash);
		let range = EncodedKeyRange::prefix(&prefix);
		Ok(state_range(self.node_id, txn, range).next().transpose()?.is_some())
	}

	pub(crate) fn get_row_shape(
		&self,
		txn: &mut FlowTransaction,
		fingerprint: RowShapeFingerprint,
	) -> Result<Option<RowShape>> {
		let key = self.schema_key(fingerprint);
		match state_get(self.node_id, txn, &key)? {
			Some(row) => {
				let op = RowShape::new(RowFamily::Operator, vec![RowShapeField::unconstrained("state", ValueType::Blob)]);
				let blob = op.get_blob(row.bytes(), 0);
				if blob.is_empty() {
					return Ok(None);
				}
				let fields: Vec<RowShapeField> = from_bytes(blob.as_ref()).map_err(|e| {
					Error::from(FlowStateError::Decode {
						state: "row shape",
						cause: e.to_string(),
					})
				})?;
				let shape = RowShape::new(RowFamily::Operator, fields);
				Ok(Some(shape))
			}
			None => Ok(None),
		}
	}

	pub(crate) fn set_row_shape(&self, txn: &mut FlowTransaction, shape: &RowShape) -> Result<()> {
		let fingerprint = shape.fingerprint();
		let key = self.schema_key(fingerprint);
		if state_get(self.node_id, txn, &key)?.is_some() {
			return Ok(());
		}
		let serialized = to_stdvec(&shape.fields().to_vec()).map_err(|e| {
			Error::from(FlowStateError::Encode {
				state: "row shape",
				cause: e.to_string(),
			})
		})?;
		let op = RowShape::new(RowFamily::Operator, vec![RowShapeField::unconstrained("state", ValueType::Blob)]);
		let mut row = op.allocate_operator();
		op.set_blob(&mut row, 0, &Blob::from(serialized));
		state_set(self.node_id, txn, &key, row.freeze())?;
		Ok(())
	}
}

fn row_number_from_key(bytes: &[u8]) -> Option<RowNumber> {
	if bytes.len() < ROW_NUMBER_BYTES {
		return None;
	}
	let suffix: [u8; ROW_NUMBER_BYTES] = bytes[bytes.len() - ROW_NUMBER_BYTES..].try_into().ok()?;
	Some(RowNumber(u64::from_be_bytes(suffix)))
}
