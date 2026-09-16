// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{
	reifydb_assertions,
	value::{digest::Digest, value_type::ValueType},
};

use crate::row::{bytes::RowBuilder, shape::RowShape};

impl RowShape {
	pub fn set_digest(&self, row: &mut impl RowBuilder, index: usize, value: &Digest) {
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
		}
		self.check_digest_type(index, value);
		self.replace_dynamic_data(row, index, &value.encode());
	}

	pub fn get_digest(&self, row: &[u8], index: usize) -> Digest {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
		}

		let ref_slice = &row[field.offset as usize..field.offset as usize + 8];
		let offset = u32::from_le_bytes([ref_slice[0], ref_slice[1], ref_slice[2], ref_slice[3]]) as usize;
		let length = u32::from_le_bytes([ref_slice[4], ref_slice[5], ref_slice[6], ref_slice[7]]) as usize;

		let data_start = self.dynamic_section_start() + offset;
		let bytes = row.get(data_start..data_start + length).unwrap_or_else(|| {
			panic!(
				"corrupt digest in column {:?}: bytes {data_start}..{} lie outside the {}-byte row",
				field.name,
				data_start + length,
				row.len()
			)
		});
		let digest = Digest::decode(bytes)
			.unwrap_or_else(|error| panic!("corrupt digest in column {:?}: {error}", field.name));
		self.check_digest_type(index, &digest);
		digest
	}

	fn check_digest_type(&self, index: usize, digest: &Digest) {
		let field = &self.fields()[index];
		match field.constraint.get_type().inner_type() {
			ValueType::Digest {
				inner,
				accuracy,
			} if **inner == *digest.inner() && *accuracy == digest.accuracy() => {}
			declared => panic!(
				"column {:?} of type {declared} cannot hold a {}",
				field.name,
				ValueType::Digest {
					inner: Box::new(digest.inner().clone()),
					accuracy: digest.accuracy(),
				}
			),
		}
	}
}
