// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{
	Blob, Date, DateTime, Decimal, Int, Interval, Time, Type, Uint, Uuid4, Uuid7, diagnostic::engine, return_error,
};

use crate::{
	BitVec,
	row::{EncodedRow, EncodedRowLayout},
	value::columnar::{ColumnData, Columns},
};

impl Columns {
	pub fn append_columns(&mut self, other: Columns) -> crate::Result<()> {
		if self.len() != other.len() {
			return_error!(engine::frame_error("mismatched column count".to_string()));
		}

		for (i, (l, r)) in self.iter_mut().zip(other.into_iter()).enumerate() {
			// Allow matching by original name if qualified names
			// don't match
			if l.qualified_name() != r.qualified_name() && l.name() != r.name() {
				return_error!(engine::frame_error(format!(
					"column name mismatch at index {}: '{}' vs '{}' (original: '{}' vs '{}')",
					i,
					l.qualified_name(),
					r.qualified_name(),
					l.name(),
					r.name()
				)));
			}
			l.extend(r)?;
		}
		Ok(())
	}
}

impl Columns {
	pub fn append_rows(
		&mut self,
		layout: &EncodedRowLayout,
		rows: impl IntoIterator<Item = EncodedRow>,
	) -> crate::Result<()> {
		if self.len() != layout.fields.len() {
			return_error!(engine::frame_error(format!(
				"mismatched column count: expected {}, got {}",
				self.len(),
				layout.fields.len()
			)));
		}

		let rows: Vec<EncodedRow> = rows.into_iter().collect();
		let values = layout.fields.iter().map(|f| f.value.clone()).collect::<Vec<_>>();
		let layout = EncodedRowLayout::new(&values);

		// if there is an undefined column and the new data contains
		// defined data convert this column into the new type and fill
		// the undefined part
		for (index, column) in self.iter_mut().enumerate() {
			if let ColumnData::Undefined(container) = column.data() {
				let size = container.len();
				let new_data = match layout.value(index) {
					Type::Boolean => ColumnData::bool_with_bitvec(
						vec![false; size],
						BitVec::repeat(size, false),
					),
					Type::Float4 => ColumnData::float4_with_bitvec(
						vec![0.0f32; size],
						BitVec::repeat(size, false),
					),
					Type::Float8 => ColumnData::float8_with_bitvec(
						vec![0.0f64; size],
						BitVec::repeat(size, false),
					),
					Type::Int1 => ColumnData::int1_with_bitvec(
						vec![0i8; size],
						BitVec::repeat(size, false),
					),
					Type::Int2 => ColumnData::int2_with_bitvec(
						vec![0i16; size],
						BitVec::repeat(size, false),
					),
					Type::Int4 => ColumnData::int4_with_bitvec(
						vec![0i32; size],
						BitVec::repeat(size, false),
					),
					Type::Int8 => ColumnData::int8_with_bitvec(
						vec![0i64; size],
						BitVec::repeat(size, false),
					),
					Type::Int16 => ColumnData::int16_with_bitvec(
						vec![0i128; size],
						BitVec::repeat(size, false),
					),
					Type::Utf8 => ColumnData::utf8_with_bitvec(
						vec![String::new(); size],
						BitVec::repeat(size, false),
					),
					Type::Uint1 => ColumnData::uint1_with_bitvec(
						vec![0u8; size],
						BitVec::repeat(size, false),
					),
					Type::Uint2 => ColumnData::uint2_with_bitvec(
						vec![0u16; size],
						BitVec::repeat(size, false),
					),
					Type::Uint4 => ColumnData::uint4_with_bitvec(
						vec![0u32; size],
						BitVec::repeat(size, false),
					),
					Type::Uint8 => ColumnData::uint8_with_bitvec(
						vec![0u64; size],
						BitVec::repeat(size, false),
					),
					Type::Uint16 => ColumnData::uint16_with_bitvec(
						vec![0u128; size],
						BitVec::repeat(size, false),
					),
					Type::Date => ColumnData::date_with_bitvec(
						vec![Date::default(); size],
						BitVec::repeat(size, false),
					),
					Type::DateTime => ColumnData::datetime_with_bitvec(
						vec![DateTime::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Time => ColumnData::time_with_bitvec(
						vec![Time::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Interval => ColumnData::interval_with_bitvec(
						vec![Interval::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Undefined => column.data().clone(),
					Type::RowNumber => ColumnData::row_number_with_bitvec(
						vec![Default::default(); size],
						BitVec::repeat(size, false),
					),
					Type::IdentityId => ColumnData::identity_id_with_bitvec(
						vec![Default::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Uuid4 => ColumnData::uuid4_with_bitvec(
						vec![Uuid4::from(uuid::Uuid::nil()); size],
						BitVec::repeat(size, false),
					),
					Type::Uuid7 => ColumnData::uuid7_with_bitvec(
						vec![Uuid7::from(uuid::Uuid::nil()); size],
						BitVec::repeat(size, false),
					),
					Type::Blob => ColumnData::blob_with_bitvec(
						vec![Blob::new(vec![]); size],
						BitVec::repeat(size, false),
					),
					Type::Int => ColumnData::int_with_bitvec(
						vec![Int::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Uint => ColumnData::uint_with_bitvec(
						vec![Uint::default(); size],
						BitVec::repeat(size, false),
					),
					Type::Decimal {
						..
					} => ColumnData::decimal_with_bitvec(
						vec![Decimal::from(0); size],
						BitVec::repeat(size, false),
					),
				};

				*column = column.with_new_data(new_data);
			}
		}

		for row in &rows {
			if layout.all_defined(&row) {
				// if all columns in the row are defined, then
				// we can take a simpler implementation
				self.append_all_defined(&layout, &row)?;
			} else {
				// at least one column is undefined
				self.append_fallback(&layout, &row)?;
			}
		}

		Ok(())
	}

	fn append_all_defined(&mut self, layout: &EncodedRowLayout, row: &EncodedRow) -> crate::Result<()> {
		for (index, column) in self.iter_mut().enumerate() {
			match (column.data_mut(), layout.value(index)) {
				(ColumnData::Bool(container), Type::Boolean) => {
					container.push(layout.get_bool(&row, index));
				}
				(ColumnData::Float4(container), Type::Float4) => {
					container.push(layout.get_f32(&row, index));
				}
				(ColumnData::Float8(container), Type::Float8) => {
					container.push(layout.get_f64(&row, index));
				}
				(ColumnData::Int1(container), Type::Int1) => {
					container.push(layout.get_i8(&row, index));
				}
				(ColumnData::Int2(container), Type::Int2) => {
					container.push(layout.get_i16(&row, index));
				}
				(ColumnData::Int4(container), Type::Int4) => {
					container.push(layout.get_i32(&row, index));
				}
				(ColumnData::Int8(container), Type::Int8) => {
					container.push(layout.get_i64(&row, index));
				}
				(ColumnData::Int16(container), Type::Int16) => {
					container.push(layout.get_i128(&row, index));
				}
				(
					ColumnData::Utf8 {
						container,
						..
					},
					Type::Utf8,
				) => {
					container.push(layout.get_utf8(&row, index).to_string());
				}
				(ColumnData::Uint1(container), Type::Uint1) => {
					container.push(layout.get_u8(&row, index));
				}
				(ColumnData::Uint2(container), Type::Uint2) => {
					container.push(layout.get_u16(&row, index));
				}
				(ColumnData::Uint4(container), Type::Uint4) => {
					container.push(layout.get_u32(&row, index));
				}
				(ColumnData::Uint8(container), Type::Uint8) => {
					container.push(layout.get_u64(&row, index));
				}
				(ColumnData::Uint16(container), Type::Uint16) => {
					container.push(layout.get_u128(&row, index));
				}
				(ColumnData::Date(container), Type::Date) => {
					container.push(layout.get_date(&row, index));
				}
				(ColumnData::DateTime(container), Type::DateTime) => {
					container.push(layout.get_datetime(&row, index));
				}
				(ColumnData::Time(container), Type::Time) => {
					container.push(layout.get_time(&row, index));
				}
				(ColumnData::Interval(container), Type::Interval) => {
					container.push(layout.get_interval(&row, index));
				}
				(ColumnData::Uuid4(container), Type::Uuid4) => {
					container.push(layout.get_uuid4(&row, index));
				}
				(ColumnData::Uuid7(container), Type::Uuid7) => {
					container.push(layout.get_uuid7(&row, index));
				}
				(
					ColumnData::Blob {
						container,
						..
					},
					Type::Blob,
				) => {
					container.push(layout.get_blob(&row, index));
				}
				(
					ColumnData::Int {
						container,
						..
					},
					Type::Int,
				) => {
					container.push(layout.get_int(&row, index));
				}
				(
					ColumnData::Uint {
						container,
						..
					},
					Type::Uint,
				) => {
					container.push(layout.get_uint(&row, index));
				}
				(
					ColumnData::Decimal {
						container,
						..
					},
					Type::Decimal {
						..
					},
				) => {
					container.push(layout.get_decimal(&row, index));
				}
				(_, v) => {
					return_error!(engine::frame_error(format!(
						"type mismatch for column '{}'({}): incompatible with value {}",
						column.qualified_name(),
						column.data().get_type(),
						v
					)));
				}
			}
		}
		Ok(())
	}

	fn append_fallback(&mut self, layout: &EncodedRowLayout, row: &EncodedRow) -> crate::Result<()> {
		for (index, column) in self.iter_mut().enumerate() {
			match (column.data_mut(), layout.value(index)) {
				(ColumnData::Bool(container), Type::Boolean) => match layout.try_get_bool(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Float4(container), Type::Float4) => match layout.try_get_f32(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Float8(container), Type::Float8) => match layout.try_get_f64(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Int1(container), Type::Int1) => match layout.try_get_i8(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Int2(container), Type::Int2) => match layout.try_get_i16(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Int4(container), Type::Int4) => match layout.try_get_i32(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Int8(container), Type::Int8) => match layout.try_get_i64(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Int16(container), Type::Int16) => match layout.try_get_i128(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(
					ColumnData::Utf8 {
						container,
						..
					},
					Type::Utf8,
				) => match layout.try_get_utf8(row, index) {
					Some(v) => container.push(v.to_string()),
					None => container.push_undefined(),
				},
				(ColumnData::Uint1(container), Type::Uint1) => match layout.try_get_u8(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Uint2(container), Type::Uint2) => match layout.try_get_u16(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Uint4(container), Type::Uint4) => match layout.try_get_u32(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Uint8(container), Type::Uint8) => match layout.try_get_u64(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Uint16(container), Type::Uint16) => {
					match layout.try_get_u128(row, index) {
						Some(v) => container.push(v),
						None => container.push_undefined(),
					}
				}
				(ColumnData::Date(container), Type::Date) => match layout.try_get_date(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::DateTime(container), Type::DateTime) => {
					match layout.try_get_datetime(row, index) {
						Some(v) => container.push(v),
						None => container.push_undefined(),
					}
				}
				(ColumnData::Time(container), Type::Time) => match layout.try_get_time(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Interval(container), Type::Interval) => {
					match layout.try_get_interval(row, index) {
						Some(v) => container.push(v),
						None => container.push_undefined(),
					}
				}
				(ColumnData::Uuid4(container), Type::Uuid4) => match layout.try_get_uuid4(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Uuid7(container), Type::Uuid7) => match layout.try_get_uuid7(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(
					ColumnData::Int {
						container,
						..
					},
					Type::Int,
				) => match layout.try_get_int(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(
					ColumnData::Uint {
						container,
						..
					},
					Type::Uint,
				) => match layout.try_get_uint(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(
					ColumnData::Decimal {
						container,
						..
					},
					Type::Decimal {
						..
					},
				) => match layout.try_get_decimal(row, index) {
					Some(v) => container.push(v),
					None => container.push_undefined(),
				},
				(ColumnData::Undefined(container), Type::Undefined) => {
					container.push_undefined();
				}
				(l, r) => unreachable!("{:#?} {:#?}", l, r),
			}
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	mod columns {
		use reifydb_type::{RowNumber, Uuid4, Uuid7};
		use uuid::{Timestamp, Uuid};

		use crate::value::columnar::{ColumnQualified, Columns};

		#[test]
		fn test_boolean() {
			let mut test_instance1 =
				Columns::new(vec![ColumnQualified::bool_with_bitvec("id", [true], [false])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::bool_with_bitvec("id", [false], [true])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::bool_with_bitvec("id", [true, false], [false, true])
			);
		}

		#[test]
		fn test_float4() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::float4("id", [1.0f32, 2.0])]);

			let test_instance2 = Columns::new(vec![ColumnQualified::float4_with_bitvec(
				"id",
				[3.0f32, 4.0],
				[true, false],
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::float4_with_bitvec(
					"id",
					[1.0f32, 2.0, 3.0, 4.0],
					[true, true, true, false]
				)
			);
		}

		#[test]
		fn test_float8() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::float8("id", [1.0f64, 2.0])]);

			let test_instance2 = Columns::new(vec![ColumnQualified::float8_with_bitvec(
				"id",
				[3.0f64, 4.0],
				[true, false],
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::float8_with_bitvec(
					"id",
					[1.0f64, 2.0, 3.0, 4.0],
					[true, true, true, false]
				)
			);
		}

		#[test]
		fn test_int1() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::int1("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::int1_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::int1_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_int2() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::int2("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::int2_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::int2_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_int4() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::int4("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::int4_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::int4_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_int8() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::int8("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::int8_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::int8_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_int16() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::int16("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::int16_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::int16_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_string() {
			let mut test_instance1 =
				Columns::new(vec![ColumnQualified::utf8_with_bitvec("id", ["a", "b"], [true, true])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::utf8_with_bitvec("id", ["c", "d"], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::utf8_with_bitvec(
					"id",
					["a", "b", "c", "d"],
					[true, true, true, false]
				)
			);
		}

		#[test]
		fn test_uint1() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::uint1("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::uint1_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::uint1_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uint2() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::uint2("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::uint2_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::uint2_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uint4() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::uint4("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::uint4_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::uint4_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uint8() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::uint8("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::uint8_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::uint8_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uint16() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::uint16("id", [1, 2])]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::uint16_with_bitvec("id", [3, 4], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::uint16_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
			);
		}

		#[test]
		fn test_uuid4() {
			use uuid::Uuid;

			let uuid1 = Uuid4::from(Uuid::new_v4());
			let uuid2 = Uuid4::from(Uuid::new_v4());
			let uuid3 = Uuid4::from(Uuid::new_v4());
			let uuid4 = Uuid4::from(Uuid::new_v4());

			let mut test_instance1 = Columns::new(vec![ColumnQualified::uuid4("id", [uuid1, uuid2])]);

			let test_instance2 = Columns::new(vec![ColumnQualified::uuid4_with_bitvec(
				"id",
				[uuid3, uuid4],
				[true, false],
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::uuid4_with_bitvec(
					"id",
					[uuid1, uuid2, uuid3, uuid4],
					[true, true, true, false]
				)
			);
		}

		#[test]
		fn test_uuid7() {
			let uuid1 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian(1, 1)));
			let uuid2 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian(1, 2)));
			let uuid3 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian(2, 1)));
			let uuid4 = Uuid7::from(Uuid::new_v7(Timestamp::from_gregorian(2, 2)));

			let mut test_instance1 = Columns::new(vec![ColumnQualified::uuid7("id", [uuid1, uuid2])]);

			let test_instance2 = Columns::new(vec![ColumnQualified::uuid7_with_bitvec(
				"id",
				[uuid3, uuid4],
				[true, false],
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::uuid7_with_bitvec(
					"id",
					[uuid1, uuid2, uuid3, uuid4],
					[true, true, true, false]
				)
			);
		}

		#[test]
		fn test_row_number() {
			let mut test_instance1 =
				Columns::new(vec![ColumnQualified::row_number([RowNumber(1), RowNumber(2)])]);

			let test_instance2 = Columns::new(vec![ColumnQualified::row_number_with_bitvec(
				[RowNumber(3), RowNumber(4)],
				[true, false],
			)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::row_number_with_bitvec(
					[RowNumber(1), RowNumber(2), RowNumber(3), RowNumber(4)],
					[true, true, true, false]
				)
			);
		}

		#[test]
		fn test_with_undefined_lr_promotes_correctly() {
			let mut test_instance1 =
				Columns::new(vec![ColumnQualified::int2_with_bitvec("id", [1, 2], [true, false])]);

			let test_instance2 = Columns::new(vec![ColumnQualified::undefined("id", 2)]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::int2_with_bitvec("id", [1, 2, 0, 0], [true, false, false, false])
			);
		}

		#[test]
		fn test_with_undefined_l_promotes_correctly() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::undefined("score", 2)]);

			let test_instance2 =
				Columns::new(vec![ColumnQualified::int2_with_bitvec("score", [10, 20], [true, false])]);

			test_instance1.append_columns(test_instance2).unwrap();

			assert_eq!(
				test_instance1[0],
				ColumnQualified::int2_with_bitvec("score", [0, 0, 10, 20], [false, false, true, false])
			);
		}

		#[test]
		fn test_fails_on_column_count_mismatch() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::int2("id", [1])]);

			let test_instance2 = Columns::new(vec![
				ColumnQualified::int2("id", [2]),
				ColumnQualified::utf8("name", ["Bob"]),
			]);

			let result = test_instance1.append_columns(test_instance2);
			assert!(result.is_err());
		}

		#[test]
		fn test_fails_on_column_name_mismatch() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::int2("id", [1])]);

			let test_instance2 = Columns::new(vec![ColumnQualified::int2("wrong", [2])]);

			let result = test_instance1.append_columns(test_instance2);
			assert!(result.is_err());
		}

		#[test]
		fn test_fails_on_type_mismatch() {
			let mut test_instance1 = Columns::new(vec![ColumnQualified::int2("id", [1])]);

			let test_instance2 = Columns::new(vec![ColumnQualified::utf8("id", ["A"])]);

			let result = test_instance1.append_columns(test_instance2);
			assert!(result.is_err());
		}
	}

	mod row {
		use reifydb_type::{OrderedF32, OrderedF64, Type, Value};

		use crate::{
			BitVec,
			row::EncodedRowLayout,
			value::columnar::{Column, ColumnData, ColumnQualified, Columns, SourceQualified},
		};

		#[test]
		fn test_before_undefined_bool() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);

			let layout = EncodedRowLayout::new(&[Type::Boolean]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Boolean(true)]);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::bool_with_bitvec(
					[false, false, true],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_float4() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Float4]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Float4(OrderedF32::try_from(1.5).unwrap())]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::float4_with_bitvec(
					[0.0, 0.0, 1.5],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_float8() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Float8]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Float8(OrderedF64::try_from(2.25).unwrap())]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::float8_with_bitvec(
					[0.0, 0.0, 2.25],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int1() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Int1]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Int1(42)]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int1_with_bitvec([0, 0, 42], BitVec::from_slice(&[false, false, true]))
			);
		}

		#[test]
		fn test_before_undefined_int2() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Int2]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Int2(-1234)]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int2_with_bitvec([0, 0, -1234], BitVec::from_slice(&[false, false, true]))
			);
		}

		#[test]
		fn test_before_undefined_int4() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Int4]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Int4(56789)]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int4_with_bitvec([0, 0, 56789], BitVec::from_slice(&[false, false, true]))
			);
		}

		#[test]
		fn test_before_undefined_int8() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Int8]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Int8(-987654321)]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int8_with_bitvec(
					[0, 0, -987654321],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_int16() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Int16]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Int16(123456789012345678901234567890i128)]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int16_with_bitvec(
					[0, 0, 123456789012345678901234567890i128],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_string() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Utf8]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Utf8("reifydb".into())]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::utf8_with_bitvec(
					["".to_string(), "".to_string(), "reifydb".to_string()],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint1() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Uint1]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Uint1(255)]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint1_with_bitvec([0, 0, 255], BitVec::from_slice(&[false, false, true]))
			);
		}

		#[test]
		fn test_before_undefined_uint2() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Uint2]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Uint2(65535)]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint2_with_bitvec([0, 0, 65535], BitVec::from_slice(&[false, false, true]))
			);
		}

		#[test]
		fn test_before_undefined_uint4() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Uint4]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Uint4(4294967295)]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint4_with_bitvec(
					[0, 0, 4294967295],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint8() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Uint8]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Uint8(18446744073709551615)]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint8_with_bitvec(
					[0, 0, 18446744073709551615],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_before_undefined_uint16() {
			let mut test_instance = Columns::new(vec![ColumnQualified::undefined("test_col", 2)]);
			let layout = EncodedRowLayout::new(&[Type::Uint16]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Uint16(340282366920938463463374607431768211455u128)]);
			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint16_with_bitvec(
					[0, 0, 340282366920938463463374607431768211455u128],
					BitVec::from_slice(&[false, false, true])
				)
			);
		}

		#[test]
		fn test_mismatched_columns() {
			let mut test_instance = Columns::new(vec![]);

			let layout = EncodedRowLayout::new(&[Type::Int2]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Int2(2)]);

			let err = test_instance.append_rows(&layout, [row]).err().unwrap();
			assert!(err.to_string().contains("mismatched column count: expected 0, got 1"));
		}

		#[test]
		fn test_ok() {
			let mut test_instance = test_instance_with_columns();

			let layout = EncodedRowLayout::new(&[Type::Int2, Type::Boolean]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Int2(2), Value::Boolean(true)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Int2(3), Value::Boolean(false)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int2([1, 2, 3]));
			assert_eq!(*test_instance[1].data(), ColumnData::bool([true, true, false]));
		}

		#[test]
		fn test_all_defined_bool() {
			let mut test_instance = Columns::new(vec![ColumnQualified::bool("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Boolean]);
			let mut row_one = layout.allocate_row();
			layout.set_bool(&mut row_one, 0, true);
			let mut row_two = layout.allocate_row();
			layout.set_bool(&mut row_two, 0, false);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::bool([true, false]));
		}

		#[test]
		fn test_all_defined_float4() {
			let mut test_instance = Columns::new(vec![ColumnQualified::float4("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Float4]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Float4(OrderedF32::try_from(1.0).unwrap())]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Float4(OrderedF32::try_from(2.0).unwrap())]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::float4([1.0, 2.0]));
		}

		#[test]
		fn test_all_defined_float8() {
			let mut test_instance = Columns::new(vec![ColumnQualified::float8("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Float8]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Float8(OrderedF64::try_from(1.0).unwrap())]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Float8(OrderedF64::try_from(2.0).unwrap())]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::float8([1.0, 2.0]));
		}

		#[test]
		fn test_all_defined_int1() {
			let mut test_instance = Columns::new(vec![ColumnQualified::int1("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Int1]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Int1(1)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Int1(2)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int1([1, 2]));
		}

		#[test]
		fn test_all_defined_int2() {
			let mut test_instance = Columns::new(vec![ColumnQualified::int2("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Int2]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Int2(100)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Int2(200)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int2([100, 200]));
		}

		#[test]
		fn test_all_defined_int4() {
			let mut test_instance = Columns::new(vec![ColumnQualified::int4("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Int4]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Int4(1000)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Int4(2000)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int4([1000, 2000]));
		}

		#[test]
		fn test_all_defined_int8() {
			let mut test_instance = Columns::new(vec![ColumnQualified::int8("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Int8]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Int8(10000)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Int8(20000)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int8([10000, 20000]));
		}

		#[test]
		fn test_all_defined_int16() {
			let mut test_instance = Columns::new(vec![ColumnQualified::int16("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Int16]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Int16(1000)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Int16(2000)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int16([1000, 2000]));
		}

		#[test]
		fn test_all_defined_string() {
			let mut test_instance = Columns::new(vec![ColumnQualified::utf8("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Utf8]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Utf8("a".into())]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Utf8("b".into())]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::utf8(["a".to_string(), "b".to_string()]));
		}

		#[test]
		fn test_all_defined_uint1() {
			let mut test_instance = Columns::new(vec![ColumnQualified::uint1("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Uint1]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Uint1(1)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Uint1(2)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint1([1, 2]));
		}

		#[test]
		fn test_all_defined_uint2() {
			let mut test_instance = Columns::new(vec![ColumnQualified::uint2("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Uint2]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Uint2(100)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Uint2(200)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint2([100, 200]));
		}

		#[test]
		fn test_all_defined_uint4() {
			let mut test_instance = Columns::new(vec![ColumnQualified::uint4("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Uint4]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Uint4(1000)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Uint4(2000)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint4([1000, 2000]));
		}

		#[test]
		fn test_all_defined_uint8() {
			let mut test_instance = Columns::new(vec![ColumnQualified::uint8("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Uint8]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Uint8(10000)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Uint8(20000)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint8([10000, 20000]));
		}

		#[test]
		fn test_all_defined_uint16() {
			let mut test_instance = Columns::new(vec![ColumnQualified::uint16("test_col", [])]);

			let layout = EncodedRowLayout::new(&[Type::Uint16]);
			let mut row_one = layout.allocate_row();
			layout.set_values(&mut row_one, &[Value::Uint16(1000)]);
			let mut row_two = layout.allocate_row();
			layout.set_values(&mut row_two, &[Value::Uint16(2000)]);

			test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint16([1000, 2000]));
		}

		#[test]
		fn test_row_with_undefined() {
			let mut test_instance = test_instance_with_columns();

			let layout = EncodedRowLayout::new(&[Type::Int2, Type::Boolean]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Undefined, Value::Boolean(false)]);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int2_with_bitvec(vec![1, 0], vec![true, false])
			);
			assert_eq!(*test_instance[1].data(), ColumnData::bool_with_bitvec([true, false], [true, true]));
		}

		#[test]
		fn test_row_with_type_mismatch_fails() {
			let mut test_instance = test_instance_with_columns();

			let layout = EncodedRowLayout::new(&[Type::Boolean, Type::Boolean]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Boolean(true), Value::Boolean(true)]);

			let result = test_instance.append_rows(&layout, [row]);
			assert!(result.is_err());
			assert!(result.unwrap_err().to_string().contains("type mismatch"));
		}

		#[test]
		fn test_row_wrong_length_fails() {
			let mut test_instance = test_instance_with_columns();

			let layout = EncodedRowLayout::new(&[Type::Int2]);
			let mut row = layout.allocate_row();
			layout.set_values(&mut row, &[Value::Int2(2)]);

			let result = test_instance.append_rows(&layout, [row]);
			assert!(result.is_err());
			assert!(result.unwrap_err().to_string().contains("mismatched column count"));
		}

		#[test]
		fn test_fallback_bool() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::bool("test_col", []),
				ColumnQualified::bool("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Boolean, Type::Boolean]);
			let mut row_one = layout.allocate_row();
			layout.set_bool(&mut row_one, 0, true);
			layout.set_undefined(&mut row_one, 1);

			test_instance.append_rows(&layout, [row_one]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::bool_with_bitvec([true], [true]));

			assert_eq!(*test_instance[1].data(), ColumnData::bool_with_bitvec([false], [false]));
		}

		#[test]
		fn test_fallback_float4() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::float4("test_col", []),
				ColumnQualified::float4("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Float4, Type::Float4]);
			let mut row = layout.allocate_row();
			layout.set_f32(&mut row, 0, 1.5);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::float4_with_bitvec([1.5], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::float4_with_bitvec([0.0], [false]));
		}

		#[test]
		fn test_fallback_float8() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::float8("test_col", []),
				ColumnQualified::float8("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Float8, Type::Float8]);
			let mut row = layout.allocate_row();
			layout.set_f64(&mut row, 0, 2.5);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::float8_with_bitvec([2.5], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::float8_with_bitvec([0.0], [false]));
		}

		#[test]
		fn test_fallback_int1() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::int1("test_col", []),
				ColumnQualified::int1("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Int1, Type::Int1]);
			let mut row = layout.allocate_row();
			layout.set_i8(&mut row, 0, 42);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int1_with_bitvec([42], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::int1_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_int2() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::int2("test_col", []),
				ColumnQualified::int2("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Int2, Type::Int2]);
			let mut row = layout.allocate_row();
			layout.set_i16(&mut row, 0, -1234i16);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int2_with_bitvec([-1234], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::int2_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_int4() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::int4("test_col", []),
				ColumnQualified::int4("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Int4, Type::Int4]);
			let mut row = layout.allocate_row();
			layout.set_i32(&mut row, 0, 56789);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int4_with_bitvec([56789], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::int4_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_int8() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::int8("test_col", []),
				ColumnQualified::int8("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Int8, Type::Int8]);
			let mut row = layout.allocate_row();
			layout.set_i64(&mut row, 0, -987654321);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::int8_with_bitvec([-987654321], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::int8_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_int16() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::int16("test_col", []),
				ColumnQualified::int16("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Int16, Type::Int16]);
			let mut row = layout.allocate_row();
			layout.set_i128(&mut row, 0, 123456789012345678901234567890i128);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::int16_with_bitvec([123456789012345678901234567890i128], [true])
			);
			assert_eq!(*test_instance[1].data(), ColumnData::int16_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_string() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::utf8("test_col", []),
				ColumnQualified::utf8("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Utf8, Type::Utf8]);
			let mut row = layout.allocate_row();
			layout.set_utf8(&mut row, 0, "reifydb");
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::utf8_with_bitvec(["reifydb".to_string()], [true])
			);
			assert_eq!(*test_instance[1].data(), ColumnData::utf8_with_bitvec(["".to_string()], [false]));
		}

		#[test]
		fn test_fallback_uint1() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::uint1("test_col", []),
				ColumnQualified::uint1("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Uint1, Type::Uint1]);
			let mut row = layout.allocate_row();
			layout.set_u8(&mut row, 0, 255);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint1_with_bitvec([255], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::uint1_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_uint2() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::uint2("test_col", []),
				ColumnQualified::uint2("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Uint2, Type::Uint2]);
			let mut row = layout.allocate_row();
			layout.set_u16(&mut row, 0, 65535u16);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint2_with_bitvec([65535], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::uint2_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_uint4() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::uint4("test_col", []),
				ColumnQualified::uint4("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Uint4, Type::Uint4]);
			let mut row = layout.allocate_row();
			layout.set_u32(&mut row, 0, 4294967295u32);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(*test_instance[0].data(), ColumnData::uint4_with_bitvec([4294967295], [true]));
			assert_eq!(*test_instance[1].data(), ColumnData::uint4_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_uint8() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::uint8("test_col", []),
				ColumnQualified::uint8("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Uint8, Type::Uint8]);
			let mut row = layout.allocate_row();
			layout.set_u64(&mut row, 0, 18446744073709551615u64);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint8_with_bitvec([18446744073709551615], [true])
			);
			assert_eq!(*test_instance[1].data(), ColumnData::uint8_with_bitvec([0], [false]));
		}

		#[test]
		fn test_fallback_uint16() {
			let mut test_instance = Columns::new(vec![
				ColumnQualified::uint16("test_col", []),
				ColumnQualified::uint16("undefined", []),
			]);

			let layout = EncodedRowLayout::new(&[Type::Uint16, Type::Uint16]);
			let mut row = layout.allocate_row();
			layout.set_u128(&mut row, 0, 340282366920938463463374607431768211455u128);
			layout.set_undefined(&mut row, 1);

			test_instance.append_rows(&layout, [row]).unwrap();

			assert_eq!(
				*test_instance[0].data(),
				ColumnData::uint16_with_bitvec([340282366920938463463374607431768211455u128], [true])
			);
			assert_eq!(*test_instance[1].data(), ColumnData::uint16_with_bitvec([0], [false]));
		}

		fn test_instance_with_columns() -> Columns {
			Columns::new(vec![
				Column::SourceQualified(SourceQualified {
					source: "test".into(),
					name: "int2".into(),
					data: ColumnData::int2(vec![1]),
				}),
				Column::SourceQualified(SourceQualified {
					source: "test".into(),
					name: "bool".into(),
					data: ColumnData::bool(vec![true]),
				}),
			])
		}
	}
}
