// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Blob, CowVec, EncodedKey,
	interface::{CdcChange, CdcEvent},
	row::EncodedRow,
};

use super::layout::*;

pub(crate) fn encode_cdc_event(
	event: &CdcEvent,
) -> reifydb_core::Result<EncodedRow> {
	let mut row = CDC_EVENT_LAYOUT.allocate_row();

	// Set fixed-size fields
	CDC_EVENT_LAYOUT.set_u64(&mut row, CDC_VERSION_FIELD, event.version);
	CDC_EVENT_LAYOUT.set_u16(&mut row, CDC_SEQUENCE_FIELD, event.sequence);
	CDC_EVENT_LAYOUT.set_u64(
		&mut row,
		CDC_TIMESTAMP_FIELD,
		event.timestamp,
	);

	// Encode change type and variable-length fields
	match &event.change {
		CdcChange::Insert {
			key,
			after,
		} => {
			CDC_EVENT_LAYOUT.set_u8(
				&mut row,
				CDC_CHANGE_TYPE_FIELD,
				ChangeType::Insert as u8,
			);
			CDC_EVENT_LAYOUT.set_blob(
				&mut row,
				CDC_KEY_FIELD,
				&Blob::from_slice(key.as_slice()),
			);
			CDC_EVENT_LAYOUT
				.set_undefined(&mut row, CDC_BEFORE_FIELD);
			CDC_EVENT_LAYOUT.set_blob(
				&mut row,
				CDC_AFTER_FIELD,
				&Blob::from_slice(after.as_slice()),
			);
		}
		CdcChange::Update {
			key,
			before,
			after,
		} => {
			CDC_EVENT_LAYOUT.set_u8(
				&mut row,
				CDC_CHANGE_TYPE_FIELD,
				ChangeType::Update as u8,
			);
			CDC_EVENT_LAYOUT.set_blob(
				&mut row,
				CDC_KEY_FIELD,
				&Blob::from_slice(key.as_slice()),
			);
			CDC_EVENT_LAYOUT.set_blob(
				&mut row,
				CDC_BEFORE_FIELD,
				&Blob::from_slice(before.as_slice()),
			);
			CDC_EVENT_LAYOUT.set_blob(
				&mut row,
				CDC_AFTER_FIELD,
				&Blob::from_slice(after.as_slice()),
			);
		}
		CdcChange::Delete {
			key,
			before,
		} => {
			CDC_EVENT_LAYOUT.set_u8(
				&mut row,
				CDC_CHANGE_TYPE_FIELD,
				ChangeType::Delete as u8,
			);
			CDC_EVENT_LAYOUT.set_blob(
				&mut row,
				CDC_KEY_FIELD,
				&Blob::from_slice(key.as_slice()),
			);
			CDC_EVENT_LAYOUT.set_blob(
				&mut row,
				CDC_BEFORE_FIELD,
				&Blob::from_slice(before.as_slice()),
			);
			CDC_EVENT_LAYOUT
				.set_undefined(&mut row, CDC_AFTER_FIELD);
		}
	}

	Ok(row)
}

pub(crate) fn decode_cdc_event(
	row: &EncodedRow,
) -> reifydb_core::Result<CdcEvent> {
	let version = CDC_EVENT_LAYOUT.get_u64(row, CDC_VERSION_FIELD);
	let sequence = CDC_EVENT_LAYOUT.get_u16(row, CDC_SEQUENCE_FIELD);
	let timestamp = CDC_EVENT_LAYOUT.get_u64(row, CDC_TIMESTAMP_FIELD);
	let change_type = ChangeType::from(
		CDC_EVENT_LAYOUT.get_u8(row, CDC_CHANGE_TYPE_FIELD),
	);

	let key_blob = CDC_EVENT_LAYOUT.get_blob(row, CDC_KEY_FIELD);
	let key = EncodedKey::new(key_blob.as_bytes().to_vec());

	let change = match change_type {
		ChangeType::Insert => {
			let after_blob =
				CDC_EVENT_LAYOUT.get_blob(row, CDC_AFTER_FIELD);
			let after = EncodedRow(CowVec::new(
				after_blob.as_bytes().to_vec(),
			));
			CdcChange::Insert {
				key,
				after,
			}
		}
		ChangeType::Update => {
			let before_blob = CDC_EVENT_LAYOUT
				.get_blob(row, CDC_BEFORE_FIELD);
			let after_blob =
				CDC_EVENT_LAYOUT.get_blob(row, CDC_AFTER_FIELD);
			let before = EncodedRow(CowVec::new(
				before_blob.as_bytes().to_vec(),
			));
			let after = EncodedRow(CowVec::new(
				after_blob.as_bytes().to_vec(),
			));
			CdcChange::Update {
				key,
				before,
				after,
			}
		}
		ChangeType::Delete => {
			let before_blob = CDC_EVENT_LAYOUT
				.get_blob(row, CDC_BEFORE_FIELD);
			let before = EncodedRow(CowVec::new(
				before_blob.as_bytes().to_vec(),
			));
			CdcChange::Delete {
				key,
				before,
			}
		}
	};

	Ok(CdcEvent::new(version, sequence, timestamp, change))
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{CdcChange, CdcEvent};

	use super::*;

	#[test]
	fn test_encode_decode_insert() {
		let key = EncodedKey::new(vec![1, 2, 3]);
		let after = EncodedRow(CowVec::new(vec![4, 5, 6]));
		let change = CdcChange::Insert {
			key: key.clone(),
			after: after.clone(),
		};
		let event = CdcEvent::new(123456789, 42, 1234567890, change);

		let encoded = encode_cdc_event(&event).unwrap();
		let decoded = decode_cdc_event(&encoded).unwrap();

		assert_eq!(decoded.version, 123456789);
		assert_eq!(decoded.sequence, 42);
		assert_eq!(decoded.timestamp, 1234567890);

		match decoded.change {
			CdcChange::Insert {
				key: k,
				after: a,
			} => {
				assert_eq!(k, key);
				assert_eq!(a, after);
			}
			_ => panic!("Expected Insert variant"),
		}
	}

	#[test]
	fn test_encode_decode_update() {
		let key = EncodedKey::new(vec![1, 2, 3]);
		let before = EncodedRow(CowVec::new(vec![4, 5, 6]));
		let after = EncodedRow(CowVec::new(vec![7, 8, 9]));
		let change = CdcChange::Update {
			key: key.clone(),
			before: before.clone(),
			after: after.clone(),
		};
		let event = CdcEvent::new(123456789, 43, 1234567890, change);

		let encoded = encode_cdc_event(&event).unwrap();
		let decoded = decode_cdc_event(&encoded).unwrap();

		assert_eq!(decoded.version, 123456789);
		assert_eq!(decoded.sequence, 43);
		assert_eq!(decoded.timestamp, 1234567890);

		match decoded.change {
			CdcChange::Update {
				key: k,
				before: b,
				after: a,
			} => {
				assert_eq!(k, key);
				assert_eq!(b, before);
				assert_eq!(a, after);
			}
			_ => panic!("Expected Update variant"),
		}
	}

	#[test]
	fn test_encode_decode_delete() {
		let key = EncodedKey::new(vec![1, 2, 3]);
		let before = EncodedRow(CowVec::new(vec![4, 5, 6]));
		let change = CdcChange::Delete {
			key: key.clone(),
			before: before.clone(),
		};
		let event = CdcEvent::new(123456789, 44, 1234567890, change);

		let encoded = encode_cdc_event(&event).unwrap();
		let decoded = decode_cdc_event(&encoded).unwrap();

		assert_eq!(decoded.version, 123456789);
		assert_eq!(decoded.sequence, 44);
		assert_eq!(decoded.timestamp, 1234567890);

		match decoded.change {
			CdcChange::Delete {
				key: k,
				before: b,
			} => {
				assert_eq!(k, key);
				assert_eq!(b, before);
			}
			_ => panic!("Expected Delete variant"),
		}
	}
}
