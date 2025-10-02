// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, EncodedKey,
	interface::{Cdc, CdcChange, CdcSequencedChange, TransactionId},
	return_internal_error,
	value::encoded::EncodedValues,
};
use reifydb_type::Blob;

use super::layout::*;

/// Encode a CdcTransaction to a more memory-efficient format
/// This stores shared metadata once and then encodes all changes compactly
pub(crate) fn encode_cdc_transaction(transaction: &Cdc) -> crate::Result<EncodedValues> {
	let mut row = CDC_TRANSACTION_LAYOUT.allocate_row();

	CDC_TRANSACTION_LAYOUT.set_u64(&mut row, CDC_TX_VERSION_FIELD, transaction.version);

	CDC_TRANSACTION_LAYOUT.set_u64(&mut row, CDC_TX_TIMESTAMP_FIELD, transaction.timestamp);

	CDC_TRANSACTION_LAYOUT.set_blob(
		&mut row,
		CDC_TX_TRANSACTION_FIELD,
		&Blob::from_slice(transaction.transaction.as_bytes()),
	);

	let mut changes_bytes = Vec::new();

	changes_bytes.extend_from_slice(&(transaction.changes.len() as u32).to_le_bytes());

	for sequenced_change in &transaction.changes {
		changes_bytes.extend_from_slice(&sequenced_change.sequence.to_le_bytes());

		let encoded_change = encode_cdc_change(&sequenced_change.change)?;
		let change_bytes = encoded_change.as_slice();
		changes_bytes.extend_from_slice(&(change_bytes.len() as u32).to_le_bytes());
		changes_bytes.extend_from_slice(change_bytes);
	}

	CDC_TRANSACTION_LAYOUT.set_blob(&mut row, CDC_TX_CHANGES_FIELD, &Blob::from_slice(&changes_bytes));

	Ok(row)
}

/// Decode a CdcTransaction from its encoded format
pub(crate) fn decode_cdc_transaction(row: &EncodedValues) -> crate::Result<Cdc> {
	let version = CDC_TRANSACTION_LAYOUT.get_u64(row, CDC_TX_VERSION_FIELD);
	let timestamp = CDC_TRANSACTION_LAYOUT.get_u64(row, CDC_TX_TIMESTAMP_FIELD);
	let transaction_blob = CDC_TRANSACTION_LAYOUT.get_blob(row, CDC_TX_TRANSACTION_FIELD);
	let transaction = TransactionId::try_from(transaction_blob.as_bytes())?;

	let changes_blob = CDC_TRANSACTION_LAYOUT.get_blob(row, CDC_TX_CHANGES_FIELD);
	let changes_bytes = changes_blob.as_bytes();

	let mut offset = 0;

	if changes_bytes.len() < 4 {
		return_internal_error!("Invalid CDC transaction format: insufficient bytes for change count");
	}
	let num_changes =
		u32::from_le_bytes([changes_bytes[0], changes_bytes[1], changes_bytes[2], changes_bytes[3]]) as usize;
	offset += 4;

	let mut changes = Vec::with_capacity(num_changes);

	for _ in 0..num_changes {
		if offset + 2 > changes_bytes.len() {
			return_internal_error!("Invalid CDC transaction format: insufficient bytes for sequence");
		}
		let sequence = u16::from_le_bytes([changes_bytes[offset], changes_bytes[offset + 1]]);
		offset += 2;

		if offset + 4 > changes_bytes.len() {
			return_internal_error!("Invalid CDC transaction format: insufficient bytes for change length");
		}
		let change_len = u32::from_le_bytes([
			changes_bytes[offset],
			changes_bytes[offset + 1],
			changes_bytes[offset + 2],
			changes_bytes[offset + 3],
		]) as usize;
		offset += 4;

		if offset + change_len > changes_bytes.len() {
			return_internal_error!("Invalid CDC transaction format: insufficient bytes for change data");
		}
		let change_bytes = &changes_bytes[offset..offset + change_len];
		let change_row = EncodedValues(CowVec::new(change_bytes.to_vec()));
		let change = decode_cdc_change(&change_row)?;
		offset += change_len;

		changes.push(CdcSequencedChange {
			sequence,
			change,
		});
	}

	Ok(Cdc::new(version, timestamp, transaction, changes))
}

/// Encode just the CdcChange part (without metadata)
fn encode_cdc_change(change: &CdcChange) -> crate::Result<EncodedValues> {
	let mut row = CDC_CHANGE_LAYOUT.allocate_row();

	match change {
		CdcChange::Insert {
			key,
			post,
		} => {
			CDC_CHANGE_LAYOUT.set_u8(&mut row, CDC_COMPACT_CHANGE_TYPE_FIELD, ChangeType::Insert as u8);
			CDC_CHANGE_LAYOUT.set_blob(
				&mut row,
				CDC_COMPACT_CHANGE_KEY_FIELD,
				&Blob::from_slice(key.as_slice()),
			);
			CDC_CHANGE_LAYOUT.set_undefined(&mut row, CDC_COMPACT_CHANGE_PRE_FIELD);
			CDC_CHANGE_LAYOUT.set_blob(
				&mut row,
				CDC_COMPACT_CHANGE_POST_FIELD,
				&Blob::from_slice(post.as_slice()),
			);
		}
		CdcChange::Update {
			key,
			pre,
			post,
		} => {
			CDC_CHANGE_LAYOUT.set_u8(&mut row, CDC_COMPACT_CHANGE_TYPE_FIELD, ChangeType::Update as u8);
			CDC_CHANGE_LAYOUT.set_blob(
				&mut row,
				CDC_COMPACT_CHANGE_KEY_FIELD,
				&Blob::from_slice(key.as_slice()),
			);
			CDC_CHANGE_LAYOUT.set_blob(
				&mut row,
				CDC_COMPACT_CHANGE_PRE_FIELD,
				&Blob::from_slice(pre.as_slice()),
			);
			CDC_CHANGE_LAYOUT.set_blob(
				&mut row,
				CDC_COMPACT_CHANGE_POST_FIELD,
				&Blob::from_slice(post.as_slice()),
			);
		}
		CdcChange::Delete {
			key,
			pre,
		} => {
			CDC_CHANGE_LAYOUT.set_u8(&mut row, CDC_COMPACT_CHANGE_TYPE_FIELD, ChangeType::Delete as u8);
			CDC_CHANGE_LAYOUT.set_blob(
				&mut row,
				CDC_COMPACT_CHANGE_KEY_FIELD,
				&Blob::from_slice(key.as_slice()),
			);
			match pre {
				Some(pre_row) => {
					CDC_CHANGE_LAYOUT.set_blob(
						&mut row,
						CDC_COMPACT_CHANGE_PRE_FIELD,
						&Blob::from_slice(pre_row.as_slice()),
					);
				}
				None => {
					CDC_CHANGE_LAYOUT.set_undefined(&mut row, CDC_COMPACT_CHANGE_PRE_FIELD);
				}
			}
			CDC_CHANGE_LAYOUT.set_undefined(&mut row, CDC_COMPACT_CHANGE_POST_FIELD);
		}
	}

	Ok(row)
}

/// Decode just the CdcChange part
fn decode_cdc_change(row: &EncodedValues) -> crate::Result<CdcChange> {
	let change_type = ChangeType::from(CDC_CHANGE_LAYOUT.get_u8(row, CDC_COMPACT_CHANGE_TYPE_FIELD));
	let key_blob = CDC_CHANGE_LAYOUT.get_blob(row, CDC_COMPACT_CHANGE_KEY_FIELD);
	let key = EncodedKey::new(key_blob.as_bytes().to_vec());

	let change = match change_type {
		ChangeType::Insert => {
			let post_blob = CDC_CHANGE_LAYOUT.get_blob(row, CDC_COMPACT_CHANGE_POST_FIELD);
			let post = EncodedValues(CowVec::new(post_blob.as_bytes().to_vec()));
			CdcChange::Insert {
				key,
				post,
			}
		}
		ChangeType::Update => {
			let pre_blob = CDC_CHANGE_LAYOUT.get_blob(row, CDC_COMPACT_CHANGE_PRE_FIELD);
			let post_blob = CDC_CHANGE_LAYOUT.get_blob(row, CDC_COMPACT_CHANGE_POST_FIELD);
			let pre = EncodedValues(CowVec::new(pre_blob.as_bytes().to_vec()));
			let post = EncodedValues(CowVec::new(post_blob.as_bytes().to_vec()));
			CdcChange::Update {
				key,
				pre,
				post,
			}
		}
		ChangeType::Delete => {
			let pre = if row.is_defined(CDC_COMPACT_CHANGE_PRE_FIELD) {
				let pre_blob = CDC_CHANGE_LAYOUT.get_blob(row, CDC_COMPACT_CHANGE_PRE_FIELD);
				Some(EncodedValues(CowVec::new(pre_blob.as_bytes().to_vec())))
			} else {
				None
			};
			CdcChange::Delete {
				key,
				pre,
			}
		}
	};

	Ok(change)
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{CdcChange, TransactionId};

	use super::*;

	#[test]
	fn test_encode_decode_transaction_single_change() {
		let key = EncodedKey::new(vec![1, 2, 3]);
		let post = EncodedValues(CowVec::new(vec![4, 5, 6]));
		let change = CdcChange::Insert {
			key: key.clone(),
			post: post.clone(),
		};

		let changes = vec![CdcSequencedChange {
			sequence: 1,
			change: change.clone(),
		}];

		let transaction = Cdc::new(123456789, 1234567890, TransactionId::default(), changes);

		let encoded = encode_cdc_transaction(&transaction).unwrap();
		let decoded = decode_cdc_transaction(&encoded).unwrap();

		assert_eq!(decoded.version, 123456789);
		assert_eq!(decoded.timestamp, 1234567890);
		assert_eq!(decoded.changes.len(), 1);
		assert_eq!(decoded.changes[0].sequence, 1);
		assert_eq!(decoded.changes[0].change, change);
	}

	#[test]
	fn test_encode_decode_transaction_multiple_changes() {
		let changes = vec![
			CdcSequencedChange {
				sequence: 1,
				change: CdcChange::Insert {
					key: EncodedKey::new(vec![1]),
					post: EncodedValues(CowVec::new(vec![10])),
				},
			},
			CdcSequencedChange {
				sequence: 2,
				change: CdcChange::Update {
					key: EncodedKey::new(vec![2]),
					pre: EncodedValues(CowVec::new(vec![20])),
					post: EncodedValues(CowVec::new(vec![21])),
				},
			},
			CdcSequencedChange {
				sequence: 3,
				change: CdcChange::Delete {
					key: EncodedKey::new(vec![3]),
					pre: Some(EncodedValues(CowVec::new(vec![30]))),
				},
			},
		];

		let transaction = Cdc::new(987654321, 9876543210, TransactionId::default(), changes.clone());

		let encoded = encode_cdc_transaction(&transaction).unwrap();
		let decoded = decode_cdc_transaction(&encoded).unwrap();

		assert_eq!(decoded.version, 987654321);
		assert_eq!(decoded.timestamp, 9876543210);
		assert_eq!(decoded.changes.len(), 3);

		for (i, (original, decoded_change)) in changes.iter().zip(decoded.changes.iter()).enumerate() {
			assert_eq!(decoded_change.sequence, original.sequence, "Sequence mismatch at index {}", i);
			assert_eq!(decoded_change.change, original.change, "Change mismatch at index {}", i);
		}
	}
}
