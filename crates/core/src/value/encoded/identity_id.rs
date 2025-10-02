// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::ptr;

use reifydb_type::{IdentityId, Type, Uuid7};
use uuid::Uuid;

use crate::value::encoded::{EncodedValues, EncodedValuesLayout};

impl EncodedValuesLayout {
	pub fn set_identity_id(&self, row: &mut EncodedValues, index: usize, value: IdentityId) {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::IdentityId);
		row.set_valid(index, true);
		unsafe {
			// IdentityId wraps Uuid7 which is 16 bytes
			ptr::write_unaligned(
				row.make_mut().as_mut_ptr().add(field.offset) as *mut [u8; 16],
				*value.as_bytes(),
			);
		}
	}

	pub fn get_identity_id(&self, row: &EncodedValues, index: usize) -> IdentityId {
		let field = &self.fields[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.r#type, Type::IdentityId);
		unsafe {
			// IdentityId wraps Uuid7 which is 16 bytes
			let bytes: [u8; 16] = ptr::read_unaligned(row.as_ptr().add(field.offset) as *const [u8; 16]);
			let uuid = Uuid::from_bytes(bytes);
			let uuid7 = Uuid7::from(uuid);
			IdentityId::from(uuid7)
		}
	}

	pub fn try_get_identity_id(&self, row: &EncodedValues, index: usize) -> Option<IdentityId> {
		if row.is_defined(index) {
			Some(self.get_identity_id(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_type::{IdentityId, Type};

	use crate::value::encoded::EncodedValuesLayout;

	#[test]
	fn test_set_get_identity_id() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId]);
		let mut row = layout.allocate();

		let id = IdentityId::generate();
		layout.set_identity_id(&mut row, 0, id.clone());
		assert_eq!(layout.get_identity_id(&row, 0), id);
	}

	#[test]
	fn test_try_get_identity_id() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId]);
		let mut row = layout.allocate();

		assert_eq!(layout.try_get_identity_id(&row, 0), None);

		let id = IdentityId::generate();
		layout.set_identity_id(&mut row, 0, id.clone());
		assert_eq!(layout.try_get_identity_id(&row, 0), Some(id));
	}

	#[test]
	fn test_multiple_generations() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId]);

		// Generate multiple Identity IDs and ensure they're different
		let mut ids = Vec::new();
		for _ in 0..10 {
			let mut row = layout.allocate();
			let id = IdentityId::generate();
			layout.set_identity_id(&mut row, 0, id.clone());
			let retrieved = layout.get_identity_id(&row, 0);
			assert_eq!(retrieved, id);
			ids.push(id);
		}

		// Ensure all generated Identity IDs are unique
		for i in 0..ids.len() {
			for j in (i + 1)..ids.len() {
				assert_ne!(ids[i], ids[j], "Identity IDs should be unique");
			}
		}
	}

	#[test]
	fn test_uuid7_properties() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId]);
		let mut row = layout.allocate();

		let id = IdentityId::generate();
		layout.set_identity_id(&mut row, 0, id.clone());
		let retrieved = layout.get_identity_id(&row, 0);

		// Verify it's backed by a version 7 UUID
		assert_eq!(retrieved.get_version_num(), 7);
		assert_eq!(id.get_version_num(), 7);
	}

	#[test]
	fn test_timestamp_ordering() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId]);

		// Generate Identity IDs in sequence - they should be ordered by
		// timestamp
		let mut ids = Vec::new();
		for _ in 0..5 {
			let mut row = layout.allocate();
			let id = IdentityId::generate();
			layout.set_identity_id(&mut row, 0, id.clone());
			let retrieved = layout.get_identity_id(&row, 0);
			assert_eq!(retrieved, id);
			ids.push(id);

			// Small delay to ensure different timestamps
			std::thread::sleep(std::time::Duration::from_millis(1));
		}

		// Verify that Identity IDs are ordered (timestamp-based)
		for i in 1..ids.len() {
			assert!(ids[i].as_bytes() >= ids[i - 1].as_bytes(), "Identity IDs should be timestamp-ordered");
		}
	}

	#[test]
	fn test_mixed_with_other_types() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId, Type::Boolean, Type::IdentityId, Type::Int4]);
		let mut row = layout.allocate();

		let id1 = IdentityId::generate();
		let id2 = IdentityId::generate();

		layout.set_identity_id(&mut row, 0, id1.clone());
		layout.set_bool(&mut row, 1, true);
		layout.set_identity_id(&mut row, 2, id2.clone());
		layout.set_i32(&mut row, 3, 42);

		assert_eq!(layout.get_identity_id(&row, 0), id1);
		assert_eq!(layout.get_bool(&row, 1), true);
		assert_eq!(layout.get_identity_id(&row, 2), id2);
		assert_eq!(layout.get_i32(&row, 3), 42);
	}

	#[test]
	fn test_undefined_handling() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId, Type::IdentityId]);
		let mut row = layout.allocate();

		let id = IdentityId::generate();
		layout.set_identity_id(&mut row, 0, id.clone());

		assert_eq!(layout.try_get_identity_id(&row, 0), Some(id));
		assert_eq!(layout.try_get_identity_id(&row, 1), None);

		layout.set_undefined(&mut row, 0);
		assert_eq!(layout.try_get_identity_id(&row, 0), None);
	}

	#[test]
	fn test_persistence() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId]);
		let mut row = layout.allocate();

		let id = IdentityId::generate();
		let id_string = id.to_string();

		layout.set_identity_id(&mut row, 0, id.clone());
		let retrieved = layout.get_identity_id(&row, 0);

		assert_eq!(retrieved, id);
		assert_eq!(retrieved.to_string(), id_string);
		assert_eq!(retrieved.as_bytes(), id.as_bytes());
	}

	#[test]
	fn test_clone_consistency() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId]);
		let mut row = layout.allocate();

		let original_id = IdentityId::generate();
		layout.set_identity_id(&mut row, 0, original_id.clone());

		let retrieved_id = layout.get_identity_id(&row, 0);
		assert_eq!(retrieved_id, original_id);

		// Verify that the underlying UUID7 byte representation is
		// identical
		assert_eq!(retrieved_id.as_bytes(), original_id.as_bytes());
	}

	#[test]
	fn test_multiple_fields() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId, Type::IdentityId, Type::IdentityId]);
		let mut row = layout.allocate();

		let id1 = IdentityId::generate();
		let id2 = IdentityId::generate();
		let id3 = IdentityId::generate();

		layout.set_identity_id(&mut row, 0, id1.clone());
		layout.set_identity_id(&mut row, 1, id2.clone());
		layout.set_identity_id(&mut row, 2, id3.clone());

		assert_eq!(layout.get_identity_id(&row, 0), id1);
		assert_eq!(layout.get_identity_id(&row, 1), id2);
		assert_eq!(layout.get_identity_id(&row, 2), id3);

		// Ensure all Identity IDs are different
		assert_ne!(id1, id2);
		assert_ne!(id1, id3);
		assert_ne!(id2, id3);
	}

	#[test]
	fn test_format_consistency() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId]);
		let mut row = layout.allocate();

		let id = IdentityId::generate();
		let original_string = id.to_string();

		layout.set_identity_id(&mut row, 0, id.clone());
		let retrieved = layout.get_identity_id(&row, 0);
		let retrieved_string = retrieved.to_string();

		assert_eq!(original_string, retrieved_string);

		// Verify UUID string format (8-4-4-4-12) since IdentityId is
		// based on UUID7
		assert_eq!(original_string.len(), 36);
		assert_eq!(original_string.matches('-').count(), 4);
	}

	#[test]
	fn test_byte_level_storage() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId]);
		let mut row = layout.allocate();

		let id = IdentityId::generate();
		let original_bytes = *id.as_bytes();

		layout.set_identity_id(&mut row, 0, id.clone());
		let retrieved = layout.get_identity_id(&row, 0);
		let retrieved_bytes = *retrieved.as_bytes();

		assert_eq!(original_bytes, retrieved_bytes);

		// Verify that it's exactly 16 bytes
		assert_eq!(original_bytes.len(), 16);
		assert_eq!(retrieved_bytes.len(), 16);
	}

	#[test]
	fn test_time_based_properties() {
		let layout = EncodedValuesLayout::new(&[Type::IdentityId]);

		// Generate Identity IDs at different times
		let id1 = IdentityId::generate();
		std::thread::sleep(std::time::Duration::from_millis(2));
		let id2 = IdentityId::generate();

		let mut row1 = layout.allocate();
		let mut row2 = layout.allocate();

		layout.set_identity_id(&mut row1, 0, id1.clone());
		layout.set_identity_id(&mut row2, 0, id2.clone());

		let retrieved1 = layout.get_identity_id(&row1, 0);
		let retrieved2 = layout.get_identity_id(&row2, 0);

		// The second Identity ID should be "greater" due to timestamp
		// ordering
		assert!(retrieved2.as_bytes() > retrieved1.as_bytes());
	}

	#[test]
	fn test_as_primary_key() {
		let layout = EncodedValuesLayout::new(&[
			Type::IdentityId, // Primary key
			Type::Utf8,       // Name field
			Type::Int4,       // Age field
		]);
		let mut row = layout.allocate();

		// Simulate a database record with Identity ID as primary key
		let primary_key = IdentityId::generate();
		layout.set_identity_id(&mut row, 0, primary_key.clone());
		layout.set_utf8(&mut row, 1, "John Doe");
		layout.set_i32(&mut row, 2, 30);

		assert_eq!(layout.get_identity_id(&row, 0), primary_key);
		assert_eq!(layout.get_utf8(&row, 1), "John Doe");
		assert_eq!(layout.get_i32(&row, 2), 30);

		// Verify that the primary key is suitable for ordering/indexing
		assert_eq!(primary_key.get_version_num(), 7);
	}
}
