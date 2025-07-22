// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Frame};
use reifydb_core::error::diagnostic::engine;
use reifydb_core::row::{EncodedRow, Layout};
use reifydb_core::{BitVec, CowVec, Date, DateTime, Interval, Time, Type, return_error};

impl Frame {
    pub fn append_frame(&mut self, other: Frame) -> crate::Result<()> {
        if self.columns.len() != other.columns.len() {
            return_error!(engine::frame_error("mismatched column count".to_string()));
        }

        for (i, (l, r)) in self.columns.iter_mut().zip(other.columns.into_iter()).enumerate() {
            if l.name != r.name {
                return_error!(engine::frame_error(format!(
                    "column name mismatch at index {}: '{}' vs '{}'",
                    i, l.name, r.name
                )));
            }

            l.extend(r)?;
        }

        Ok(())
    }
}

impl Frame {
    pub fn append_rows(
        &mut self,
        layout: &Layout,
        rows: impl IntoIterator<Item = EncodedRow>,
    ) -> crate::Result<()> {
        if self.columns.len() != layout.fields.len() {
            return_error!(engine::frame_error(format!(
                "mismatched column count: expected {}, got {}",
                self.columns.len(),
                layout.fields.len()
            )));
        }

        let rows: Vec<EncodedRow> = rows.into_iter().collect();
        let values = layout.fields.iter().map(|f| f.value.clone()).collect::<Vec<_>>();
        let layout = Layout::new(&values);

        // if there is an undefined column and the new data contains defined data
        // convert this column into the new type and fill the undefined part
        for (index, column) in self.columns.iter_mut().enumerate() {
            if let ColumnValues::Undefined(size) = column.values {
                let new_data = match layout.value(index) {
                    Type::Bool => ColumnValues::bool_with_bitvec(
                        CowVec::new(vec![false; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Float4 => ColumnValues::float4_with_bitvec(
                        CowVec::new(vec![0.0f32; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Float8 => ColumnValues::float8_with_bitvec(
                        CowVec::new(vec![0.0f64; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Int1 => ColumnValues::int1_with_bitvec(
                        CowVec::new(vec![0i8; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Int2 => ColumnValues::int2_with_bitvec(
                        CowVec::new(vec![0i16; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Int4 => ColumnValues::int4_with_bitvec(
                        CowVec::new(vec![0i32; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Int8 => ColumnValues::int8_with_bitvec(
                        CowVec::new(vec![0i64; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Int16 => ColumnValues::int16_with_bitvec(
                        CowVec::new(vec![0i128; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Utf8 => ColumnValues::utf8_with_bitvec(
                        CowVec::new(vec![String::new(); size]),
                        BitVec::new(size, false),
                    ),
                    Type::Uint1 => ColumnValues::uint1_with_bitvec(
                        CowVec::new(vec![0u8; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Uint2 => ColumnValues::uint2_with_bitvec(
                        CowVec::new(vec![0u16; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Uint4 => ColumnValues::uint4_with_bitvec(
                        CowVec::new(vec![0u32; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Uint8 => ColumnValues::uint8_with_bitvec(
                        CowVec::new(vec![0u64; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Uint16 => ColumnValues::uint16_with_bitvec(
                        CowVec::new(vec![0u128; size]),
                        BitVec::new(size, false),
                    ),
                    Type::Date => ColumnValues::date_with_bitvec(
                        CowVec::new(vec![Date::default(); size]),
                        BitVec::new(size, false),
                    ),
                    Type::DateTime => ColumnValues::datetime_with_bitvec(
                        CowVec::new(vec![DateTime::default(); size]),
                        BitVec::new(size, false),
                    ),
                    Type::Time => ColumnValues::time_with_bitvec(
                        CowVec::new(vec![Time::default(); size]),
                        BitVec::new(size, false),
                    ),
                    Type::Interval => ColumnValues::interval_with_bitvec(
                        CowVec::new(vec![Interval::default(); size]),
                        BitVec::new(size, false),
                    ),
                    Type::Undefined => column.values.clone(),
                    Type::RowId => {
                        ColumnValues::row_id(CowVec::new(vec![Default::default(); size]))
                    }
                };

                column.values = new_data;
            }
        }

        for row in &rows {
            if layout.all_defined(&row) {
                // if all columns in the row are defined, then we can take a simpler implementation
                self.append_all_defined(&layout, &row)?;
            } else {
                // at least one column is undefined
                self.append_fallback(&layout, &row)?;
            }
        }

        Ok(())
    }

    fn append_all_defined(&mut self, layout: &Layout, row: &EncodedRow) -> crate::Result<()> {
        for (index, column) in self.columns.iter_mut().enumerate() {
            match (&mut column.values, layout.value(index)) {
                (ColumnValues::Bool(vec, bitvec), Type::Bool) => {
                    vec.push(layout.get_bool(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Float4(vec, bitvec), Type::Float4) => {
                    vec.push(layout.get_f32(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Float8(vec, bitvec), Type::Float8) => {
                    vec.push(layout.get_f64(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Int1(vec, bitvec), Type::Int1) => {
                    vec.push(layout.get_i8(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Int2(vec, bitvec), Type::Int2) => {
                    vec.push(layout.get_i16(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Int4(vec, bitvec), Type::Int4) => {
                    vec.push(layout.get_i32(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Int8(vec, bitvec), Type::Int8) => {
                    vec.push(layout.get_i64(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Int16(vec, bitvec), Type::Int16) => {
                    vec.push(layout.get_i128(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Utf8(vec, bitvec), Type::Utf8) => {
                    vec.push(layout.get_utf8(&row, index).to_string());
                    bitvec.push(true);
                }
                (ColumnValues::Uint1(vec, bitvec), Type::Uint1) => {
                    vec.push(layout.get_u8(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Uint2(vec, bitvec), Type::Uint2) => {
                    vec.push(layout.get_u16(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Uint4(vec, bitvec), Type::Uint4) => {
                    vec.push(layout.get_u32(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Uint8(vec, bitvec), Type::Uint8) => {
                    vec.push(layout.get_u64(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Uint16(vec, bitvec), Type::Uint16) => {
                    vec.push(layout.get_u128(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Date(vec, bitvec), Type::Date) => {
                    vec.push(layout.get_date(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::DateTime(vec, bitvec), Type::DateTime) => {
                    vec.push(layout.get_datetime(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Time(vec, bitvec), Type::Time) => {
                    vec.push(layout.get_time(&row, index));
                    bitvec.push(true);
                }
                (ColumnValues::Interval(vec, bitvec), Type::Interval) => {
                    vec.push(layout.get_interval(&row, index));
                    bitvec.push(true);
                }
                (_, v) => {
                    return_error!(engine::frame_error(format!(
                        "type mismatch for column '{}'({}): incompatible with value {}",
                        column.name,
                        column.values.get_type(),
                        v
                    )));
                }
            }
        }
        Ok(())
    }

    fn append_fallback(&mut self, layout: &Layout, row: &EncodedRow) -> crate::Result<()> {
        for (index, column) in self.columns.iter_mut().enumerate() {
            match (&mut column.values, layout.value(index)) {
                (ColumnValues::Bool(vec, bitvec), Type::Bool) => {
                    match layout.try_get_bool(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(false);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Float4(vec, bitvec), Type::Float4) => {
                    match layout.try_get_f32(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0.0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Float8(vec, bitvec), Type::Float8) => {
                    match layout.try_get_f64(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0.0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Int1(vec, bitvec), Type::Int1) => {
                    match layout.try_get_i8(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Int2(vec, bitvec), Type::Int2) => {
                    match layout.try_get_i16(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Int4(vec, bitvec), Type::Int4) => {
                    match layout.try_get_i32(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Int8(vec, bitvec), Type::Int8) => {
                    match layout.try_get_i64(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Int16(vec, bitvec), Type::Int16) => {
                    match layout.try_get_i128(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Utf8(vec, bitvec), Type::Utf8) => {
                    match layout.try_get_utf8(row, index) {
                        Some(v) => {
                            vec.push(v.to_string());
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(String::new());
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Uint1(vec, bitvec), Type::Uint1) => {
                    match layout.try_get_u8(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Uint2(vec, bitvec), Type::Uint2) => {
                    match layout.try_get_u16(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Uint4(vec, bitvec), Type::Uint4) => {
                    match layout.try_get_u32(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Uint8(vec, bitvec), Type::Uint8) => {
                    match layout.try_get_u64(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Uint16(vec, bitvec), Type::Uint16) => {
                    match layout.try_get_u128(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(0);
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Date(vec, bitvec), Type::Date) => {
                    match layout.try_get_date(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(Date::default());
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::DateTime(vec, bitvec), Type::DateTime) => {
                    match layout.try_get_datetime(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(DateTime::default());
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Time(vec, bitvec), Type::Time) => {
                    match layout.try_get_time(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(Time::default());
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Interval(vec, bitvec), Type::Interval) => {
                    match layout.try_get_interval(row, index) {
                        Some(v) => {
                            vec.push(v);
                            bitvec.push(true);
                        }
                        None => {
                            vec.push(Interval::default());
                            bitvec.push(false);
                        }
                    }
                }
                (ColumnValues::Undefined(size), Type::Undefined) => {
                    *size += 1;
                }
                (_, _) => unreachable!(),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    mod frame {
        use crate::frame::{Frame, FrameColumn};

        #[test]
        fn test_boolean() {
            let mut test_instance1 =
                Frame::new(vec![FrameColumn::bool_with_bitvec("id", [true], [false])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::bool_with_bitvec("id", [false], [true])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::bool_with_bitvec("id", [true, false], [false, true])
            );
        }

        #[test]
        fn test_float4() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::float4("id", [1.0f32, 2.0])]);

            let test_instance2 = Frame::new(vec![FrameColumn::float4_with_bitvec(
                "id",
                [3.0f32, 4.0],
                [true, false],
            )]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::float4_with_bitvec(
                    "id",
                    [1.0f32, 2.0, 3.0, 4.0],
                    [true, true, true, false]
                )
            );
        }

        #[test]
        fn test_float8() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::float8("id", [1.0f64, 2.0])]);

            let test_instance2 = Frame::new(vec![FrameColumn::float8_with_bitvec(
                "id",
                [3.0f64, 4.0],
                [true, false],
            )]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::float8_with_bitvec(
                    "id",
                    [1.0f64, 2.0, 3.0, 4.0],
                    [true, true, true, false]
                )
            );
        }

        #[test]
        fn test_int1() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::int1("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::int1_with_bitvec("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::int1_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_int2() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::int2("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::int2_with_bitvec("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::int2_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_int4() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::int4("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::int4_with_bitvec("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::int4_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_int8() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::int8("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::int8_with_bitvec("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::int8_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_int16() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::int16("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::int16_with_bitvec("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::int16_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_string() {
            let mut test_instance1 =
                Frame::new(vec![FrameColumn::utf8_with_bitvec("id", ["a", "b"], [true, true])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::utf8_with_bitvec("id", ["c", "d"], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::utf8_with_bitvec(
                    "id",
                    ["a", "b", "c", "d"],
                    [true, true, true, false]
                )
            );
        }

        #[test]
        fn test_uint1() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::uint1("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::uint1_with_bitvec("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::uint1_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_uint2() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::uint2("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::uint2_with_bitvec("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::uint2_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_uint4() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::uint4("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::uint4_with_bitvec("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::uint4_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_uint8() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::uint8("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::uint8_with_bitvec("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::uint8_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_uint16() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::uint16("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::uint16_with_bitvec("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::uint16_with_bitvec("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_with_undefined_lr_promotes_correctly() {
            let mut test_instance1 =
                Frame::new(vec![FrameColumn::int2_with_bitvec("id", [1, 2], [true, false])]);

            let test_instance2 = Frame::new(vec![FrameColumn::undefined("id", 2)]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::int2_with_bitvec("id", [1, 2, 0, 0], [true, false, false, false])
            );
        }

        #[test]
        fn test_with_undefined_l_promotes_correctly() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::undefined("score", 2)]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::int2_with_bitvec("score", [10, 20], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                FrameColumn::int2_with_bitvec("score", [0, 0, 10, 20], [false, false, true, false])
            );
        }

        #[test]
        fn test_fails_on_column_count_mismatch() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::int2("id", [1])]);

            let test_instance2 =
                Frame::new(vec![FrameColumn::int2("id", [2]), FrameColumn::utf8("name", ["Bob"])]);

            let result = test_instance1.append_frame(test_instance2);
            assert!(result.is_err());
        }

        #[test]
        fn test_fails_on_column_name_mismatch() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::int2("id", [1])]);

            let test_instance2 = Frame::new(vec![FrameColumn::int2("wrong", [2])]);

            let result = test_instance1.append_frame(test_instance2);
            assert!(result.is_err());
        }

        #[test]
        fn test_fails_on_type_mismatch() {
            let mut test_instance1 = Frame::new(vec![FrameColumn::int2("id", [1])]);

            let test_instance2 = Frame::new(vec![FrameColumn::utf8("id", ["A"])]);

            let result = test_instance1.append_frame(test_instance2);
            assert!(result.is_err());
        }
    }

    mod row {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use reifydb_core::row::Layout;
        use reifydb_core::{BitVec, OrderedF32, OrderedF64};
        use reifydb_core::{Type, Value};

        #[test]
        fn test_before_undefined_bool() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);

            let layout = Layout::new(&[Type::Bool]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Bool(true)]);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::bool_with_bitvec(
                    [false, false, true],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_float4() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Float4]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Float4(OrderedF32::try_from(1.5).unwrap())]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::float4_with_bitvec(
                    [0.0, 0.0, 1.5],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_float8() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Float8]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Float8(OrderedF64::try_from(2.25).unwrap())]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::float8_with_bitvec(
                    [0.0, 0.0, 2.25],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_int1() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Int1]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int1(42)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int1_with_bitvec(
                    [0, 0, 42],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_int2() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Int2]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int2(-1234)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int2_with_bitvec(
                    [0, 0, -1234],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_int4() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Int4]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int4(56789)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int4_with_bitvec(
                    [0, 0, 56789],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_int8() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Int8]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int8(-987654321)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int8_with_bitvec(
                    [0, 0, -987654321],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_int16() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Int16]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int16(123456789012345678901234567890i128)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int16_with_bitvec(
                    [0, 0, 123456789012345678901234567890i128],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_string() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Utf8]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Utf8("reifydb".into())]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::utf8_with_bitvec(
                    ["".to_string(), "".to_string(), "reifydb".to_string()],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_uint1() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Uint1]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Uint1(255)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::uint1_with_bitvec(
                    [0, 0, 255],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_uint2() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Uint2]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Uint2(65535)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::uint2_with_bitvec(
                    [0, 0, 65535],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_uint4() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Uint4]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Uint4(4294967295)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::uint4_with_bitvec(
                    [0, 0, 4294967295],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_uint8() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Uint8]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Uint8(18446744073709551615)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::uint8_with_bitvec(
                    [0, 0, 18446744073709551615],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_before_undefined_uint16() {
            let mut test_instance = Frame::new(vec![FrameColumn::undefined("test", 2)]);
            let layout = Layout::new(&[Type::Uint16]);
            let mut row = layout.allocate_row();
            layout.set_values(
                &mut row,
                &[Value::Uint16(340282366920938463463374607431768211455u128)],
            );
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::uint16_with_bitvec(
                    [0, 0, 340282366920938463463374607431768211455u128],
                    BitVec::from_slice(&[false, false, true])
                )
            );
        }

        #[test]
        fn test_mismatched_columns() {
            let mut test_instance = Frame::new(vec![]);

            let layout = Layout::new(&[Type::Int2]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int2(2)]);

            let err = test_instance.append_rows(&layout, [row]).err().unwrap();
            assert!(err.to_string().contains("mismatched column count: expected 0, got 1"));
        }

        #[test]
        fn test_ok() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[Type::Int2, Type::Bool]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int2(2), Value::Bool(true)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int2(3), Value::Bool(false)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::int2([1, 2, 3]));
            assert_eq!(test_instance.columns[1].values, ColumnValues::bool([true, true, false]));
        }

        #[test]
        fn test_all_defined_bool() {
            let mut test_instance = Frame::new(vec![FrameColumn::bool("test", [])]);

            let layout = Layout::new(&[Type::Bool]);
            let mut row_one = layout.allocate_row();
            layout.set_bool(&mut row_one, 0, true);
            let mut row_two = layout.allocate_row();
            layout.set_bool(&mut row_two, 0, false);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::bool([true, false]));
        }

        #[test]
        fn test_all_defined_float4() {
            let mut test_instance = Frame::new(vec![FrameColumn::float4("test", [])]);

            let layout = Layout::new(&[Type::Float4]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Float4(OrderedF32::try_from(1.0).unwrap())]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Float4(OrderedF32::try_from(2.0).unwrap())]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::float4([1.0, 2.0]));
        }

        #[test]
        fn test_all_defined_float8() {
            let mut test_instance = Frame::new(vec![FrameColumn::float8("test", [])]);

            let layout = Layout::new(&[Type::Float8]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Float8(OrderedF64::try_from(1.0).unwrap())]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Float8(OrderedF64::try_from(2.0).unwrap())]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::float8([1.0, 2.0]));
        }

        #[test]
        fn test_all_defined_int1() {
            let mut test_instance = Frame::new(vec![FrameColumn::int1("test", [])]);

            let layout = Layout::new(&[Type::Int1]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int1(1)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int1(2)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::int1([1, 2]));
        }

        #[test]
        fn test_all_defined_int2() {
            let mut test_instance = Frame::new(vec![FrameColumn::int2("test", [])]);

            let layout = Layout::new(&[Type::Int2]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int2(100)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int2(200)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::int2([100, 200]));
        }

        #[test]
        fn test_all_defined_int4() {
            let mut test_instance = Frame::new(vec![FrameColumn::int4("test", [])]);

            let layout = Layout::new(&[Type::Int4]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int4(1000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int4(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::int4([1000, 2000]));
        }

        #[test]
        fn test_all_defined_int8() {
            let mut test_instance = Frame::new(vec![FrameColumn::int8("test", [])]);

            let layout = Layout::new(&[Type::Int8]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int8(10000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int8(20000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::int8([10000, 20000]));
        }

        #[test]
        fn test_all_defined_int16() {
            let mut test_instance = Frame::new(vec![FrameColumn::int16("test", [])]);

            let layout = Layout::new(&[Type::Int16]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int16(1000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int16(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::int16([1000, 2000]));
        }

        #[test]
        fn test_all_defined_string() {
            let mut test_instance = Frame::new(vec![FrameColumn::utf8("test", [])]);

            let layout = Layout::new(&[Type::Utf8]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Utf8("a".into())]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Utf8("b".into())]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::utf8(["a".to_string(), "b".to_string()])
            );
        }

        #[test]
        fn test_all_defined_uint1() {
            let mut test_instance = Frame::new(vec![FrameColumn::uint1("test", [])]);

            let layout = Layout::new(&[Type::Uint1]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Uint1(1)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Uint1(2)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::uint1([1, 2]));
        }

        #[test]
        fn test_all_defined_uint2() {
            let mut test_instance = Frame::new(vec![FrameColumn::uint2("test", [])]);

            let layout = Layout::new(&[Type::Uint2]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Uint2(100)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Uint2(200)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::uint2([100, 200]));
        }

        #[test]
        fn test_all_defined_uint4() {
            let mut test_instance = Frame::new(vec![FrameColumn::uint4("test", [])]);

            let layout = Layout::new(&[Type::Uint4]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Uint4(1000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Uint4(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::uint4([1000, 2000]));
        }

        #[test]
        fn test_all_defined_uint8() {
            let mut test_instance = Frame::new(vec![FrameColumn::uint8("test", [])]);

            let layout = Layout::new(&[Type::Uint8]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Uint8(10000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Uint8(20000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::uint8([10000, 20000]));
        }

        #[test]
        fn test_all_defined_uint16() {
            let mut test_instance = Frame::new(vec![FrameColumn::uint16("test", [])]);

            let layout = Layout::new(&[Type::Uint16]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Uint16(1000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Uint16(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].values, ColumnValues::uint16([1000, 2000]));
        }

        #[test]
        fn test_row_with_undefined() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[Type::Int2, Type::Bool]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Undefined, Value::Bool(false)]);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int2_with_bitvec(vec![1, 0], vec![true, false])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::bool_with_bitvec([true, false], [true, true])
            );
        }

        #[test]
        fn test_row_with_type_mismatch_fails() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[Type::Bool, Type::Bool]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Bool(true), Value::Bool(true)]);

            let result = test_instance.append_rows(&layout, [row]);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("type mismatch"));
        }

        #[test]
        fn test_row_wrong_length_fails() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[Type::Int2]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int2(2)]);

            let result = test_instance.append_rows(&layout, [row]);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("mismatched column count"));
        }

        #[test]
        fn test_fallback_bool() {
            let mut test_instance =
                Frame::new(vec![FrameColumn::bool("test", []), FrameColumn::bool("undefined", [])]);

            let layout = Layout::new(&[Type::Bool, Type::Bool]);
            let mut row_one = layout.allocate_row();
            layout.set_bool(&mut row_one, 0, true);
            layout.set_undefined(&mut row_one, 1);

            test_instance.append_rows(&layout, [row_one]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::bool_with_bitvec([true], [true])
            );

            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::bool_with_bitvec([false], [false])
            );
        }

        #[test]
        fn test_fallback_float4() {
            let mut test_instance = Frame::new(vec![
                FrameColumn::float4("test", []),
                FrameColumn::float4("undefined", []),
            ]);

            let layout = Layout::new(&[Type::Float4, Type::Float4]);
            let mut row = layout.allocate_row();
            layout.set_f32(&mut row, 0, 1.5);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::float4_with_bitvec([1.5], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::float4_with_bitvec([0.0], [false])
            );
        }

        #[test]
        fn test_fallback_float8() {
            let mut test_instance = Frame::new(vec![
                FrameColumn::float8("test", []),
                FrameColumn::float8("undefined", []),
            ]);

            let layout = Layout::new(&[Type::Float8, Type::Float8]);
            let mut row = layout.allocate_row();
            layout.set_f64(&mut row, 0, 2.5);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::float8_with_bitvec([2.5], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::float8_with_bitvec([0.0], [false])
            );
        }

        #[test]
        fn test_fallback_int1() {
            let mut test_instance =
                Frame::new(vec![FrameColumn::int1("test", []), FrameColumn::int1("undefined", [])]);

            let layout = Layout::new(&[Type::Int1, Type::Int1]);
            let mut row = layout.allocate_row();
            layout.set_i8(&mut row, 0, 42);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int1_with_bitvec([42], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::int1_with_bitvec([0], [false])
            );
        }

        #[test]
        fn test_fallback_int2() {
            let mut test_instance =
                Frame::new(vec![FrameColumn::int2("test", []), FrameColumn::int2("undefined", [])]);

            let layout = Layout::new(&[Type::Int2, Type::Int2]);
            let mut row = layout.allocate_row();
            layout.set_i16(&mut row, 0, -1234i16);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int2_with_bitvec([-1234], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::int2_with_bitvec([0], [false])
            );
        }

        #[test]
        fn test_fallback_int4() {
            let mut test_instance =
                Frame::new(vec![FrameColumn::int4("test", []), FrameColumn::int4("undefined", [])]);

            let layout = Layout::new(&[Type::Int4, Type::Int4]);
            let mut row = layout.allocate_row();
            layout.set_i32(&mut row, 0, 56789);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int4_with_bitvec([56789], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::int4_with_bitvec([0], [false])
            );
        }

        #[test]
        fn test_fallback_int8() {
            let mut test_instance =
                Frame::new(vec![FrameColumn::int8("test", []), FrameColumn::int8("undefined", [])]);

            let layout = Layout::new(&[Type::Int8, Type::Int8]);
            let mut row = layout.allocate_row();
            layout.set_i64(&mut row, 0, -987654321);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int8_with_bitvec([-987654321], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::int8_with_bitvec([0], [false])
            );
        }

        #[test]
        fn test_fallback_int16() {
            let mut test_instance = Frame::new(vec![
                FrameColumn::int16("test", []),
                FrameColumn::int16("undefined", []),
            ]);

            let layout = Layout::new(&[Type::Int16, Type::Int16]);
            let mut row = layout.allocate_row();
            layout.set_i128(&mut row, 0, 123456789012345678901234567890i128);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::int16_with_bitvec([123456789012345678901234567890i128], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::int16_with_bitvec([0], [false])
            );
        }

        #[test]
        fn test_fallback_string() {
            let mut test_instance =
                Frame::new(vec![FrameColumn::utf8("test", []), FrameColumn::utf8("undefined", [])]);

            let layout = Layout::new(&[Type::Utf8, Type::Utf8]);
            let mut row = layout.allocate_row();
            layout.set_utf8(&mut row, 0, "reifydb");
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::utf8_with_bitvec(["reifydb".to_string()], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::utf8_with_bitvec(["".to_string()], [false])
            );
        }

        #[test]
        fn test_fallback_uint1() {
            let mut test_instance = Frame::new(vec![
                FrameColumn::uint1("test", []),
                FrameColumn::uint1("undefined", []),
            ]);

            let layout = Layout::new(&[Type::Uint1, Type::Uint1]);
            let mut row = layout.allocate_row();
            layout.set_u8(&mut row, 0, 255);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::uint1_with_bitvec([255], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::uint1_with_bitvec([0], [false])
            );
        }

        #[test]
        fn test_fallback_uint2() {
            let mut test_instance = Frame::new(vec![
                FrameColumn::uint2("test", []),
                FrameColumn::uint2("undefined", []),
            ]);

            let layout = Layout::new(&[Type::Uint2, Type::Uint2]);
            let mut row = layout.allocate_row();
            layout.set_u16(&mut row, 0, 65535u16);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::uint2_with_bitvec([65535], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::uint2_with_bitvec([0], [false])
            );
        }

        #[test]
        fn test_fallback_uint4() {
            let mut test_instance = Frame::new(vec![
                FrameColumn::uint4("test", []),
                FrameColumn::uint4("undefined", []),
            ]);

            let layout = Layout::new(&[Type::Uint4, Type::Uint4]);
            let mut row = layout.allocate_row();
            layout.set_u32(&mut row, 0, 4294967295u32);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::uint4_with_bitvec([4294967295], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::uint4_with_bitvec([0], [false])
            );
        }

        #[test]
        fn test_fallback_uint8() {
            let mut test_instance = Frame::new(vec![
                FrameColumn::uint8("test", []),
                FrameColumn::uint8("undefined", []),
            ]);

            let layout = Layout::new(&[Type::Uint8, Type::Uint8]);
            let mut row = layout.allocate_row();
            layout.set_u64(&mut row, 0, 18446744073709551615u64);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::uint8_with_bitvec([18446744073709551615], [true])
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::uint8_with_bitvec([0], [false])
            );
        }

        #[test]
        fn test_fallback_uint16() {
            let mut test_instance = Frame::new(vec![
                FrameColumn::uint16("test", []),
                FrameColumn::uint16("undefined", []),
            ]);

            let layout = Layout::new(&[Type::Uint16, Type::Uint16]);
            let mut row = layout.allocate_row();
            layout.set_u128(&mut row, 0, 340282366920938463463374607431768211455u128);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].values,
                ColumnValues::uint16_with_bitvec(
                    [340282366920938463463374607431768211455u128],
                    [true]
                )
            );
            assert_eq!(
                test_instance.columns[1].values,
                ColumnValues::uint16_with_bitvec([0], [false])
            );
        }

        fn test_instance_with_columns() -> Frame {
            Frame::new(vec![
                FrameColumn { name: "int2".into(), values: ColumnValues::int2(vec![1]) },
                FrameColumn { name: "bool".into(), values: ColumnValues::bool(vec![true]) },
            ])
        }
    }
}
