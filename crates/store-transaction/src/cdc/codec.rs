// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{CommitVersion, CowVec, EncodedKey, return_internal_error, value::encoded::EncodedValues};
use reifydb_type::Blob;
use tracing::instrument;

use super::{InternalCdc, InternalCdcChange, InternalCdcSequencedChange, layout::*};

/// Encode an internal CdcTransaction to a more memory-efficient format
/// This stores shared metadata once and then encodes all changes compactly
#[instrument(level = "trace", skip(transaction), fields(change_count = transaction.changes.len()))]
pub(crate) fn encode_internal_cdc(transaction: &InternalCdc) -> crate::Result<EncodedValues> {
	let mut values = CDC_TRANSACTION_LAYOUT.allocate();

	CDC_TRANSACTION_LAYOUT.set_u64(&mut values, CDC_TX_VERSION_FIELD, transaction.version);

	CDC_TRANSACTION_LAYOUT.set_u64(&mut values, CDC_TX_TIMESTAMP_FIELD, transaction.timestamp);

	let mut changes_bytes = Vec::new();

	changes_bytes.extend_from_slice(&(transaction.changes.len() as u32).to_le_bytes());

	for sequenced_change in &transaction.changes {
		changes_bytes.extend_from_slice(&sequenced_change.sequence.to_le_bytes());

		let encoded_change = encode_internal_cdc_change(&sequenced_change.change)?;
		let change_bytes = encoded_change.as_slice();
		changes_bytes.extend_from_slice(&(change_bytes.len() as u32).to_le_bytes());
		changes_bytes.extend_from_slice(change_bytes);
	}

	CDC_TRANSACTION_LAYOUT.set_blob(&mut values, CDC_TX_CHANGES_FIELD, &Blob::from_slice(&changes_bytes));

	Ok(values)
}

/// Decode an internal CdcTransaction from its encoded format
pub(crate) fn decode_internal_cdc(values: &EncodedValues) -> crate::Result<InternalCdc> {
	let version = CDC_TRANSACTION_LAYOUT.get_u64(values, CDC_TX_VERSION_FIELD);
	let timestamp = CDC_TRANSACTION_LAYOUT.get_u64(values, CDC_TX_TIMESTAMP_FIELD);
	let changes_blob = CDC_TRANSACTION_LAYOUT.get_blob(values, CDC_TX_CHANGES_FIELD);
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
		let change = decode_internal_cdc_change(&change_row)?;
		offset += change_len;

		changes.push(InternalCdcSequencedChange {
			sequence,
			change,
		});
	}

	Ok(InternalCdc {
		version: CommitVersion(version),
		timestamp,
		changes,
	})
}

/// Encode just the internal CdcChange part (without metadata)
fn encode_internal_cdc_change(change: &InternalCdcChange) -> crate::Result<EncodedValues> {
	let mut values = CDC_CHANGE_LAYOUT.allocate();

	match change {
		InternalCdcChange::Insert {
			key,
			post_version,
		} => {
			CDC_CHANGE_LAYOUT.set_u8(&mut values, CDC_COMPACT_CHANGE_TYPE_FIELD, ChangeType::Insert as u8);
			CDC_CHANGE_LAYOUT.set_blob(
				&mut values,
				CDC_COMPACT_CHANGE_KEY_FIELD,
				&Blob::from_slice(key.as_slice()),
			);
			CDC_CHANGE_LAYOUT.set_u64(&mut values, CDC_COMPACT_CHANGE_PRE_VERSION_FIELD, 0u64); // No pre version for insert
			CDC_CHANGE_LAYOUT.set_u64(&mut values, CDC_COMPACT_CHANGE_POST_VERSION_FIELD, post_version.0);
		}
		InternalCdcChange::Update {
			key,
			pre_version,
			post_version,
		} => {
			CDC_CHANGE_LAYOUT.set_u8(&mut values, CDC_COMPACT_CHANGE_TYPE_FIELD, ChangeType::Update as u8);
			CDC_CHANGE_LAYOUT.set_blob(
				&mut values,
				CDC_COMPACT_CHANGE_KEY_FIELD,
				&Blob::from_slice(key.as_slice()),
			);
			CDC_CHANGE_LAYOUT.set_u64(&mut values, CDC_COMPACT_CHANGE_PRE_VERSION_FIELD, pre_version.0);
			CDC_CHANGE_LAYOUT.set_u64(&mut values, CDC_COMPACT_CHANGE_POST_VERSION_FIELD, post_version.0);
		}
		InternalCdcChange::Delete {
			key,
			pre_version,
		} => {
			CDC_CHANGE_LAYOUT.set_u8(&mut values, CDC_COMPACT_CHANGE_TYPE_FIELD, ChangeType::Delete as u8);
			CDC_CHANGE_LAYOUT.set_blob(
				&mut values,
				CDC_COMPACT_CHANGE_KEY_FIELD,
				&Blob::from_slice(key.as_slice()),
			);
			CDC_CHANGE_LAYOUT.set_u64(&mut values, CDC_COMPACT_CHANGE_PRE_VERSION_FIELD, pre_version.0);
			CDC_CHANGE_LAYOUT.set_u64(&mut values, CDC_COMPACT_CHANGE_POST_VERSION_FIELD, 0u64); // No post version for delete
		}
	}

	Ok(values)
}

/// Decode just the internal CdcChange part
fn decode_internal_cdc_change(values: &EncodedValues) -> crate::Result<InternalCdcChange> {
	let change_type = ChangeType::from(CDC_CHANGE_LAYOUT.get_u8(values, CDC_COMPACT_CHANGE_TYPE_FIELD));
	let key_blob = CDC_CHANGE_LAYOUT.get_blob(values, CDC_COMPACT_CHANGE_KEY_FIELD);
	let key = EncodedKey::new(key_blob.as_bytes().to_vec());

	let change = match change_type {
		ChangeType::Insert => {
			let post_version = CDC_CHANGE_LAYOUT.get_u64(values, CDC_COMPACT_CHANGE_POST_VERSION_FIELD);
			InternalCdcChange::Insert {
				key,
				post_version: CommitVersion(post_version),
			}
		}
		ChangeType::Update => {
			let pre_version = CDC_CHANGE_LAYOUT.get_u64(values, CDC_COMPACT_CHANGE_PRE_VERSION_FIELD);
			let post_version = CDC_CHANGE_LAYOUT.get_u64(values, CDC_COMPACT_CHANGE_POST_VERSION_FIELD);
			InternalCdcChange::Update {
				key,
				pre_version: CommitVersion(pre_version),
				post_version: CommitVersion(post_version),
			}
		}
		ChangeType::Delete => {
			let pre_version = CDC_CHANGE_LAYOUT.get_u64(values, CDC_COMPACT_CHANGE_PRE_VERSION_FIELD);
			InternalCdcChange::Delete {
				key,
				pre_version: CommitVersion(pre_version),
			}
		}
	};

	Ok(change)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_encode_decode_internal_transaction_single_change() {
		let key = EncodedKey::new(vec![1, 2, 3]);
		let post_version = CommitVersion(100);
		let change = InternalCdcChange::Insert {
			key: key.clone(),
			post_version,
		};

		let changes = vec![InternalCdcSequencedChange {
			sequence: 1,
			change: change.clone(),
		}];

		let transaction = InternalCdc {
			version: CommitVersion(123456789),
			timestamp: 1234567890,
			changes,
		};

		let encoded = encode_internal_cdc(&transaction).unwrap();
		let decoded = decode_internal_cdc(&encoded).unwrap();

		assert_eq!(decoded.version, CommitVersion(123456789));
		assert_eq!(decoded.timestamp, 1234567890);
		assert_eq!(decoded.changes.len(), 1);
		assert_eq!(decoded.changes[0].sequence, 1);
		assert_eq!(decoded.changes[0].change, change);
	}

	#[test]
	fn test_encode_decode_internal_transaction_multiple_changes() {
		let changes = vec![
			InternalCdcSequencedChange {
				sequence: 1,
				change: InternalCdcChange::Insert {
					key: EncodedKey::new(vec![1]),
					post_version: CommitVersion(10),
				},
			},
			InternalCdcSequencedChange {
				sequence: 2,
				change: InternalCdcChange::Update {
					key: EncodedKey::new(vec![2]),
					pre_version: CommitVersion(20),
					post_version: CommitVersion(21),
				},
			},
			InternalCdcSequencedChange {
				sequence: 3,
				change: InternalCdcChange::Delete {
					key: EncodedKey::new(vec![3]),
					pre_version: CommitVersion(30),
				},
			},
		];

		let transaction = InternalCdc {
			version: CommitVersion(987654321),
			timestamp: 9876543210,
			changes: changes.clone(),
		};

		let encoded = encode_internal_cdc(&transaction).unwrap();
		let decoded = decode_internal_cdc(&encoded).unwrap();

		assert_eq!(decoded.version, CommitVersion(987654321));
		assert_eq!(decoded.timestamp, 9876543210);
		assert_eq!(decoded.changes.len(), 3);

		for (i, (original, decoded_change)) in changes.iter().zip(decoded.changes.iter()).enumerate() {
			assert_eq!(decoded_change.sequence, original.sequence, "Sequence mismatch at index {}", i);
			assert_eq!(decoded_change.change, original.change, "Change mismatch at index {}", i);
		}
	}
}
