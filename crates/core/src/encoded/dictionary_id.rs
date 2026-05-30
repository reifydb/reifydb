// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::ptr;

use reifydb_value::value::{constraint::Constraint, dictionary::DictionaryEntryId, value_type::ValueType};

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	pub fn set_dictionary_id(&self, row: &mut EncodedRow, index: usize, entry: &DictionaryEntryId) {
		let field = &self.fields()[index];
		#[cfg(reifydb_assertions)]
		{
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::DictionaryId);
		}
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

	pub fn get_dictionary_id(&self, row: &EncodedRow, index: usize) -> DictionaryEntryId {
		let field = &self.fields()[index];
		#[cfg(reifydb_assertions)]
		{
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), ValueType::DictionaryId);
		}
		let id_type = match field.constraint.constraint() {
			Some(Constraint::Dictionary(_, id_type)) => id_type.clone(),
			_ => ValueType::Uint4,
		};
		unsafe {
			let ptr = row.as_ptr().add(field.offset as usize);
			let raw: u128 = match id_type {
				ValueType::Uint1 => ptr.read_unaligned() as u128,
				ValueType::Uint2 => (ptr as *const u16).read_unaligned() as u128,
				ValueType::Uint4 => (ptr as *const u32).read_unaligned() as u128,
				ValueType::Uint8 => (ptr as *const u64).read_unaligned() as u128,
				ValueType::Uint16 => (ptr as *const u128).read_unaligned(),
				_ => (ptr as *const u32).read_unaligned() as u128,
			};
			DictionaryEntryId::from_u128(raw, id_type).unwrap()
		}
	}

	pub fn try_get_dictionary_id(&self, row: &EncodedRow, index: usize) -> Option<DictionaryEntryId> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == ValueType::DictionaryId {
			Some(self.get_dictionary_id(row, index))
		} else {
			None
		}
	}
}
