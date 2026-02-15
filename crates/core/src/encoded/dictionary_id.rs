// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::ptr;

use reifydb_type::value::{constraint::Constraint, dictionary::DictionaryEntryId, r#type::Type};

use crate::encoded::{encoded::EncodedValues, schema::Schema};

impl Schema {
	pub fn set_dictionary_id(&self, row: &mut EncodedValues, index: usize, entry: &DictionaryEntryId) {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.constraint.get_type(), Type::DictionaryId);
		row.set_valid(index, true);
		unsafe {
			let ptr = row.make_mut().as_mut_ptr().add(field.offset as usize);
			match entry {
				DictionaryEntryId::U1(v) => ptr.write_unaligned(*v),
				DictionaryEntryId::U2(v) => ptr::write_unaligned(ptr as *mut u16, *v),
				DictionaryEntryId::U4(v) => ptr::write_unaligned(ptr as *mut u32, *v),
				DictionaryEntryId::U8(v) => ptr::write_unaligned(ptr as *mut u64, *v),
				DictionaryEntryId::U16(v) => ptr::write_unaligned(ptr as *mut u128, *v),
			}
		}
	}

	pub fn get_dictionary_id(&self, row: &EncodedValues, index: usize) -> DictionaryEntryId {
		let field = &self.fields()[index];
		debug_assert!(row.len() >= self.total_static_size());
		debug_assert_eq!(field.constraint.get_type(), Type::DictionaryId);
		let id_type = match field.constraint.constraint() {
			Some(Constraint::Dictionary(_, id_type)) => id_type.clone(),
			_ => Type::Uint4, // fallback
		};
		unsafe {
			let ptr = row.as_ptr().add(field.offset as usize);
			let raw: u128 = match id_type {
				Type::Uint1 => ptr.read_unaligned() as u128,
				Type::Uint2 => (ptr as *const u16).read_unaligned() as u128,
				Type::Uint4 => (ptr as *const u32).read_unaligned() as u128,
				Type::Uint8 => (ptr as *const u64).read_unaligned() as u128,
				Type::Uint16 => (ptr as *const u128).read_unaligned(),
				_ => (ptr as *const u32).read_unaligned() as u128,
			};
			DictionaryEntryId::from_u128(raw, id_type).unwrap()
		}
	}

	pub fn try_get_dictionary_id(&self, row: &EncodedValues, index: usize) -> Option<DictionaryEntryId> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == Type::DictionaryId {
			Some(self.get_dictionary_id(row, index))
		} else {
			None
		}
	}
}
