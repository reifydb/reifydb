// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::{encoding::RowField, reifydb_assertions};

use crate::encoded::{row::EncodedRow, shape::RowShape};

impl RowShape {
	#[inline]
	pub fn set<T: RowField>(&self, row: &mut EncodedRow, index: usize, value: T) {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), T::VALUE_TYPE);
		}
		let offset = field.offset as usize;
		row.set_valid(index, true);
		value.write_le(&mut row.make_mut()[offset..offset + T::ENCODED_SIZE]);
	}

	#[inline]
	pub fn get<T: RowField>(&self, row: &EncodedRow, index: usize) -> T {
		let field = &self.fields()[index];
		reifydb_assertions! {
			assert!(
				row.len() >= self.total_static_size(),
				"row/shape size mismatch: row.len()={} < total_static_size()={}",
				row.len(),
				self.total_static_size()
			);
			assert_eq!(*field.constraint.get_type().inner_type(), T::VALUE_TYPE);
		}
		let offset = field.offset as usize;
		T::read_le(&row.as_slice()[offset..offset + T::ENCODED_SIZE])
	}

	#[inline]
	pub fn try_get<T: RowField>(&self, row: &EncodedRow, index: usize) -> Option<T> {
		if row.is_defined(index) && self.fields()[index].constraint.get_type() == T::VALUE_TYPE {
			Some(self.get(row, index))
		} else {
			None
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::{
		encoding::LeBytes,
		value::{date::Date, datetime::DateTime, duration::Duration, value_type::ValueType},
	};

	use crate::encoded::{row::SHAPE_HEADER_SIZE, shape::RowShape};

	#[test]
	fn one_generic_path_writes_every_width_into_its_own_slot() {
		// Intent: all fixed-width accessors now funnel through set_le, so a single wrong bound or a
		// dropped offset would corrupt every type at once. This pins both halves for four different
		// widths (bool 1, Date 4, DateTime 8, Duration 16) at non-zero field indices: the bytes in
		// each slot are exactly that type's own LeBytes form, and every byte of the row outside the
		// four slots and the bitvec is byte-identical to what it was before the writes. Non-zero
		// indices matter - a slot written at the start of the data section still round-trips when it
		// is field 0, so only a later field can see the offset.
		// Mutation: slice the slot with a hard-coded width instead of T::ENCODED_SIZE, or write at
		// the data section start instead of field.offset, and either the per-slot byte assertion or
		// the untouched-neighbour sweep fires.
		let shape = RowShape::testing(&[
			ValueType::Uint8,
			ValueType::Boolean,
			ValueType::Date,
			ValueType::DateTime,
			ValueType::Duration,
			ValueType::Uint8,
		]);
		let mut row = shape.allocate();

		let date = Date::from_days_since_epoch(19_000).unwrap();
		let datetime = DateTime::from_nanos(0x0102_0304_0506_0708);
		let duration = Duration::new(13, 7, 1_234_567_890).unwrap();

		shape.set::<u64>(&mut row, 0, 0xAAAA_AAAA_AAAA_AAAAu64);
		shape.set::<u64>(&mut row, 5, 0xBBBB_BBBB_BBBB_BBBBu64);
		let before = row.as_slice().to_vec();

		shape.set::<bool>(&mut row, 1, true);
		shape.set::<Date>(&mut row, 2, date);
		shape.set::<DateTime>(&mut row, 3, datetime);
		shape.set::<Duration>(&mut row, 4, duration);

		let written = [
			(1usize, LeBytes::to_le_bytes(&true).as_ref().to_vec()),
			(2, LeBytes::to_le_bytes(&date).as_ref().to_vec()),
			(3, LeBytes::to_le_bytes(&datetime).as_ref().to_vec()),
			(4, LeBytes::to_le_bytes(&duration).as_ref().to_vec()),
		];

		for (index, bytes) in &written {
			let offset = shape.fields()[*index].offset as usize;
			assert_eq!(bytes.len(), shape.fields()[*index].size as usize, "field {index} slot width");
			assert_eq!(
				&row.as_slice()[offset..offset + bytes.len()],
				bytes.as_slice(),
				"field {index} does not hold its own little-endian form"
			);
		}

		for (position, (old, new)) in before.iter().zip(row.as_slice()).enumerate() {
			let in_slot = written.iter().any(|(index, bytes)| {
				let offset = shape.fields()[*index].offset as usize;
				position >= offset && position < offset + bytes.len()
			});
			let in_bitvec = position >= SHAPE_HEADER_SIZE && position < shape.data_offset();
			if !in_slot && !in_bitvec {
				assert_eq!(old, new, "byte {position} lies outside every written slot but changed");
			}
		}

		assert_eq!(shape.get::<u64>(&row, 0), 0xAAAA_AAAA_AAAA_AAAAu64);
		assert_eq!(shape.get::<u64>(&row, 5), 0xBBBB_BBBB_BBBB_BBBBu64);
		assert_eq!(shape.get::<bool>(&row, 1), true);
		assert_eq!(shape.get::<Date>(&row, 2), date);
		assert_eq!(shape.get::<DateTime>(&row, 3), datetime);
		assert_eq!(shape.get::<Duration>(&row, 4), duration);
	}
}
