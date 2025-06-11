// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, Frame};
use reifydb_core::row::{Layout, Row};
use reifydb_core::{CowVec, ValueKind};

impl Frame {
    pub fn append_frame(&mut self, other: Frame) -> crate::Result<()> {
        if self.columns.len() != other.columns.len() {
            return Err("mismatched column count".into());
        }

        for (i, (l, r)) in self.columns.iter_mut().zip(other.columns.into_iter()).enumerate() {
            if l.name != r.name {
                return Err(format!(
                    "column name mismatch at index {}: '{}' vs '{}'",
                    i, l.name, r.name
                )
                .into());
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
        rows: impl IntoIterator<Item = Row>,
    ) -> crate::Result<()> {
        if self.columns.len() != layout.fields.len() {
            return Err(format!(
                "mismatched column count: expected {}, got {}",
                self.columns.len(),
                layout.fields.len()
            )
            .into());
        }

        let rows: Vec<Row> = rows.into_iter().collect();
        let values = layout.fields.iter().map(|f| f.value.clone()).collect::<Vec<_>>();
        let layout = Layout::new(&values);

        // if there is an undefined column and the new data contains defined data
        // convert this column into the new type and fill the undefined part
        for (index, column) in self.columns.iter_mut().enumerate() {
            if let ColumnValues::Undefined(size) = column.data {
                let new_data = match layout.value(index) {
                    ValueKind::Bool => ColumnValues::bool_with_validity(
                        CowVec::new(vec![false; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Float4 => ColumnValues::float4_with_validity(
                        CowVec::new(vec![0.0f32; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Float8 => ColumnValues::float8_with_validity(
                        CowVec::new(vec![0.0f64; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Int1 => ColumnValues::int1_with_validity(
                        CowVec::new(vec![0i8; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Int2 => ColumnValues::int2_with_validity(
                        CowVec::new(vec![0i16; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Int4 => ColumnValues::int4_with_validity(
                        CowVec::new(vec![0i32; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Int8 => ColumnValues::int8_with_validity(
                        CowVec::new(vec![0i64; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Int16 => ColumnValues::int16_with_validity(
                        CowVec::new(vec![0i128; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::String => ColumnValues::string_with_validity(
                        CowVec::new(vec![String::new(); size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Uint1 => ColumnValues::uint1_with_validity(
                        CowVec::new(vec![0u8; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Uint2 => ColumnValues::uint2_with_validity(
                        CowVec::new(vec![0u16; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Uint4 => ColumnValues::uint4_with_validity(
                        CowVec::new(vec![0u32; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Uint8 => ColumnValues::uint8_with_validity(
                        CowVec::new(vec![0u64; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Uint16 => ColumnValues::uint16_with_validity(
                        CowVec::new(vec![0u128; size]),
                        CowVec::new(vec![false; size]),
                    ),
                    ValueKind::Undefined => column.data.clone(),
                };

                column.data = new_data;
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

    fn append_all_defined(&mut self, layout: &Layout, row: &Row) -> crate::Result<()> {
        for (index, column) in self.columns.iter_mut().enumerate() {
            match (&mut column.data, layout.value(index)) {
                (ColumnValues::Bool(vec, valid), ValueKind::Bool) => {
                    vec.push(layout.get_bool(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Float4(vec, valid), ValueKind::Float4) => {
                    vec.push(layout.get_f32(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Float8(vec, valid), ValueKind::Float8) => {
                    vec.push(layout.get_f64(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Int1(vec, valid), ValueKind::Int1) => {
                    vec.push(layout.get_i8(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Int2(vec, valid), ValueKind::Int2) => {
                    vec.push(layout.get_i16(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Int4(vec, valid), ValueKind::Int4) => {
                    vec.push(layout.get_i32(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Int8(vec, valid), ValueKind::Int8) => {
                    vec.push(layout.get_i64(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Int16(vec, valid), ValueKind::Int16) => {
                    vec.push(layout.get_i128(&row, index));
                    valid.push(true);
                }
                (ColumnValues::String(vec, valid), ValueKind::String) => {
                    vec.push(layout.get_str(&row, index).to_string());
                    valid.push(true);
                }
                (ColumnValues::Uint1(vec, valid), ValueKind::Uint1) => {
                    vec.push(layout.get_u8(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Uint2(vec, valid), ValueKind::Uint2) => {
                    vec.push(layout.get_u16(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Uint4(vec, valid), ValueKind::Uint4) => {
                    vec.push(layout.get_u32(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Uint8(vec, valid), ValueKind::Uint8) => {
                    vec.push(layout.get_u64(&row, index));
                    valid.push(true);
                }
                (ColumnValues::Uint16(vec, valid), ValueKind::Uint16) => {
                    vec.push(layout.get_u128(&row, index));
                    valid.push(true);
                }
                (_, v) => {
                    return Err(format!(
                        "type mismatch for column '{}'({}): incompatible with value {}",
                        column.name,
                        column.data.value(),
                        v
                    )
                    .into());
                }
            }
        }
        Ok(())
    }

    fn append_fallback(&mut self, layout: &Layout, row: &Row) -> crate::Result<()> {
        for (index, column) in self.columns.iter_mut().enumerate() {
            match (&mut column.data, layout.value(index)) {
                (ColumnValues::Bool(vec, valid), ValueKind::Bool) => {
                    match layout.try_get_bool(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(false);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Float4(vec, valid), ValueKind::Float4) => {
                    match layout.try_get_f32(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0.0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Float8(vec, valid), ValueKind::Float8) => {
                    match layout.try_get_f64(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0.0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Int1(vec, valid), ValueKind::Int1) => {
                    match layout.try_get_i8(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Int2(vec, valid), ValueKind::Int2) => {
                    match layout.try_get_i16(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Int4(vec, valid), ValueKind::Int4) => {
                    match layout.try_get_i32(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Int8(vec, valid), ValueKind::Int8) => {
                    match layout.try_get_i64(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Int16(vec, valid), ValueKind::Int16) => {
                    match layout.try_get_i128(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::String(vec, valid), ValueKind::String) => {
                    match layout.try_get_str(row, index) {
                        Some(v) => {
                            vec.push(v.to_string());
                            valid.push(true);
                        }
                        None => {
                            vec.push(String::new());
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Uint1(vec, valid), ValueKind::Uint1) => {
                    match layout.try_get_u8(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Uint2(vec, valid), ValueKind::Uint2) => {
                    match layout.try_get_u16(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Uint4(vec, valid), ValueKind::Uint4) => {
                    match layout.try_get_u32(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Uint8(vec, valid), ValueKind::Uint8) => {
                    match layout.try_get_u64(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Uint16(vec, valid), ValueKind::Uint16) => {
                    match layout.try_get_u128(row, index) {
                        Some(v) => {
                            vec.push(v);
                            valid.push(true);
                        }
                        None => {
                            vec.push(0);
                            valid.push(false);
                        }
                    }
                }
                (ColumnValues::Undefined(size), ValueKind::Undefined) => {
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
        use crate::{Column, Frame};

        #[test]
        fn test_boolean() {
            let mut test_instance1 =
                Frame::new(vec![Column::bool_with_validity("id", [true], [false])]);

            let test_instance2 =
                Frame::new(vec![Column::bool_with_validity("id", [false], [true])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::bool_with_validity("id", [true, false], [false, true])
            );
        }

        #[test]
        fn test_float4() {
            let mut test_instance1 = Frame::new(vec![Column::float4("id", [1.0f32, 2.0])]);

            let test_instance2 =
                Frame::new(vec![Column::float4_with_validity("id", [3.0f32, 4.0], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::float4_with_validity(
                    "id",
                    [1.0f32, 2.0, 3.0, 4.0],
                    [true, true, true, false]
                )
            );
        }

        #[test]
        fn test_float8() {
            let mut test_instance1 = Frame::new(vec![Column::float8("id", [1.0f64, 2.0])]);

            let test_instance2 =
                Frame::new(vec![Column::float8_with_validity("id", [3.0f64, 4.0], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::float8_with_validity(
                    "id",
                    [1.0f64, 2.0, 3.0, 4.0],
                    [true, true, true, false]
                )
            );
        }

        #[test]
        fn test_int1() {
            let mut test_instance1 = Frame::new(vec![Column::int1("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![Column::int1_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::int1_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_int2() {
            let mut test_instance1 = Frame::new(vec![Column::int2("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![Column::int2_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::int2_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_int4() {
            let mut test_instance1 = Frame::new(vec![Column::int4("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![Column::int4_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::int4_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_int8() {
            let mut test_instance1 = Frame::new(vec![Column::int8("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![Column::int8_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::int8_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_int16() {
            let mut test_instance1 = Frame::new(vec![Column::int16("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![Column::int16_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::int16_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_string() {
            let mut test_instance1 =
                Frame::new(vec![Column::string_with_validity("id", ["a", "b"], [true, true])]);

            let test_instance2 =
                Frame::new(vec![Column::string_with_validity("id", ["c", "d"], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::string_with_validity("id", ["a", "b", "c", "d"], [true, true, true, false])
            );
        }

        #[test]
        fn test_uint1() {
            let mut test_instance1 = Frame::new(vec![Column::uint1("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![Column::uint1_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::uint1_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_uint2() {
            let mut test_instance1 = Frame::new(vec![Column::uint2("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![Column::uint2_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::uint2_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_uint4() {
            let mut test_instance1 = Frame::new(vec![Column::uint4("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![Column::uint4_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::uint4_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_uint8() {
            let mut test_instance1 = Frame::new(vec![Column::uint8("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![Column::uint8_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::uint8_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_uint16() {
            let mut test_instance1 = Frame::new(vec![Column::uint16("id", [1, 2])]);

            let test_instance2 =
                Frame::new(vec![Column::uint16_with_validity("id", [3, 4], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::uint16_with_validity("id", [1, 2, 3, 4], [true, true, true, false])
            );
        }

        #[test]
        fn test_with_undefined_lr_promotes_correctly() {
            let mut test_instance1 =
                Frame::new(vec![Column::int2_with_validity("id", [1, 2], [true, false])]);

            let test_instance2 = Frame::new(vec![Column::undefined("id", 2)]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::int2_with_validity("id", [1, 2, 0, 0], [true, false, false, false])
            );
        }

        #[test]
        fn test_with_undefined_l_promotes_correctly() {
            let mut test_instance1 = Frame::new(vec![Column::undefined("score", 2)]);

            let test_instance2 =
                Frame::new(vec![Column::int2_with_validity("score", [10, 20], [true, false])]);

            test_instance1.append_frame(test_instance2).unwrap();

            assert_eq!(
                test_instance1.columns[0],
                Column::int2_with_validity("score", [0, 0, 10, 20], [false, false, true, false])
            );
        }

        #[test]
        fn test_fails_on_column_count_mismatch() {
            let mut test_instance1 = Frame::new(vec![Column::int2("id", [1])]);

            let test_instance2 =
                Frame::new(vec![Column::int2("id", [2]), Column::string("name", ["Bob"])]);

            let result = test_instance1.append_frame(test_instance2);
            assert!(result.is_err());
        }

        #[test]
        fn test_fails_on_column_name_mismatch() {
            let mut test_instance1 = Frame::new(vec![Column::int2("id", [1])]);

            let test_instance2 = Frame::new(vec![Column::int2("wrong", [2])]);

            let result = test_instance1.append_frame(test_instance2);
            assert!(result.is_err());
        }

        #[test]
        fn test_fails_on_type_mismatch() {
            let mut test_instance1 = Frame::new(vec![Column::int2("id", [1])]);

            let test_instance2 = Frame::new(vec![Column::string("id", ["A"])]);

            let result = test_instance1.append_frame(test_instance2);
            assert!(result.is_err());
        }
    }

    mod row {
        use crate::{Column, ColumnValues, Frame};
        use reifydb_core::ordered_float::{OrderedF32, OrderedF64};
        use reifydb_core::row::Layout;
        use reifydb_core::{Value, ValueKind};

        #[test]
        fn test_before_undefined_bool() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);

            let layout = Layout::new(&[ValueKind::Bool]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Bool(true)]);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::bool_with_validity([false, false, true], [false, false, true])
            );
        }

        #[test]
        fn test_before_undefined_float4() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Float4]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Float4(OrderedF32::try_from(1.5).unwrap())]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::float4_with_validity([0.0, 0.0, 1.5], [false, false, true])
            );
        }

        #[test]
        fn test_before_undefined_float8() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Float8]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Float8(OrderedF64::try_from(2.25).unwrap())]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::float8_with_validity([0.0, 0.0, 2.25], [false, false, true])
            );
        }

        #[test]
        fn test_before_undefined_int1() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Int1]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int1(42)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int1_with_validity([0, 0, 42], [false, false, true])
            );
        }

        #[test]
        fn test_before_undefined_int2() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Int2]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int2(-1234)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int2_with_validity([0, 0, -1234], [false, false, true])
            );
        }

        #[test]
        fn test_before_undefined_int4() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Int4]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int4(56789)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int4_with_validity([0, 0, 56789], [false, false, true])
            );
        }

        #[test]
        fn test_before_undefined_int8() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Int8]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int8(-987654321)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int8_with_validity([0, 0, -987654321], [false, false, true])
            );
        }

        #[test]
        fn test_before_undefined_int16() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Int16]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int16(123456789012345678901234567890i128)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int16_with_validity(
                    [0, 0, 123456789012345678901234567890i128],
                    [false, false, true]
                )
            );
        }

        #[test]
        fn test_before_undefined_string() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::String]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::String("reifydb".into())]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::string_with_validity(
                    ["".to_string(), "".to_string(), "reifydb".to_string()],
                    [false, false, true]
                )
            );
        }

        #[test]
        fn test_before_undefined_uint1() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Uint1]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Uint1(255)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::uint1_with_validity([0, 0, 255], [false, false, true])
            );
        }

        #[test]
        fn test_before_undefined_uint2() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Uint2]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Uint2(65535)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::uint2_with_validity([0, 0, 65535], [false, false, true])
            );
        }

        #[test]
        fn test_before_undefined_uint4() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Uint4]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Uint4(4294967295)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::uint4_with_validity([0, 0, 4294967295], [false, false, true])
            );
        }

        #[test]
        fn test_before_undefined_uint8() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Uint8]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Uint8(18446744073709551615)]);
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::uint8_with_validity(
                    [0, 0, 18446744073709551615],
                    [false, false, true]
                )
            );
        }

        #[test]
        fn test_before_undefined_uint16() {
            let mut test_instance = Frame::new(vec![Column::undefined("test", 2)]);
            let layout = Layout::new(&[ValueKind::Uint16]);
            let mut row = layout.allocate_row();
            layout.set_values(
                &mut row,
                &[Value::Uint16(340282366920938463463374607431768211455u128)],
            );
            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::uint16_with_validity(
                    [0, 0, 340282366920938463463374607431768211455u128],
                    [false, false, true]
                )
            );
        }

        #[test]
        fn test_mismatched_columns() {
            let mut test_instance = Frame::new(vec![]);

            let layout = Layout::new(&[ValueKind::Int2]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int2(2)]);

            let err = test_instance.append_rows(&layout, [row]).err().unwrap();
            assert_eq!(err.to_string(), "mismatched column count: expected 0, got 1");
        }

        #[test]
        fn test_ok() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[ValueKind::Int2, ValueKind::Bool]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int2(2), Value::Bool(true)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int2(3), Value::Bool(false)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int2([1, 2, 3]));
            assert_eq!(test_instance.columns[1].data, ColumnValues::bool([true, true, false]));
        }

        #[test]
        fn test_all_defined_bool() {
            let mut test_instance = Frame::new(vec![Column::bool("test", [])]);

            let layout = Layout::new(&[ValueKind::Bool]);
            let mut row_one = layout.allocate_row();
            layout.set_bool(&mut row_one, 0, true);
            let mut row_two = layout.allocate_row();
            layout.set_bool(&mut row_two, 0, false);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::bool([true, false]));
        }

        #[test]
        fn test_all_defined_float4() {
            let mut test_instance = Frame::new(vec![Column::float4("test", [])]);

            let layout = Layout::new(&[ValueKind::Float4]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Float4(OrderedF32::try_from(1.0).unwrap())]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Float4(OrderedF32::try_from(2.0).unwrap())]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::float4([1.0, 2.0]));
        }

        #[test]
        fn test_all_defined_float8() {
            let mut test_instance = Frame::new(vec![Column::float8("test", [])]);

            let layout = Layout::new(&[ValueKind::Float8]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Float8(OrderedF64::try_from(1.0).unwrap())]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Float8(OrderedF64::try_from(2.0).unwrap())]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::float8([1.0, 2.0]));
        }

        #[test]
        fn test_all_defined_int1() {
            let mut test_instance = Frame::new(vec![Column::int1("test", [])]);

            let layout = Layout::new(&[ValueKind::Int1]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int1(1)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int1(2)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int1([1, 2]));
        }

        #[test]
        fn test_all_defined_int2() {
            let mut test_instance = Frame::new(vec![Column::int2("test", [])]);

            let layout = Layout::new(&[ValueKind::Int2]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int2(100)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int2(200)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int2([100, 200]));
        }

        #[test]
        fn test_all_defined_int4() {
            let mut test_instance = Frame::new(vec![Column::int4("test", [])]);

            let layout = Layout::new(&[ValueKind::Int4]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int4(1000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int4(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int4([1000, 2000]));
        }

        #[test]
        fn test_all_defined_int8() {
            let mut test_instance = Frame::new(vec![Column::int8("test", [])]);

            let layout = Layout::new(&[ValueKind::Int8]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int8(10000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int8(20000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int8([10000, 20000]));
        }

        #[test]
        fn test_all_defined_int16() {
            let mut test_instance = Frame::new(vec![Column::int16("test", [])]);

            let layout = Layout::new(&[ValueKind::Int16]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Int16(1000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Int16(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int16([1000, 2000]));
        }

        #[test]
        fn test_all_defined_string() {
            let mut test_instance = Frame::new(vec![Column::string("test", [])]);

            let layout = Layout::new(&[ValueKind::String]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::String("a".into())]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::String("b".into())]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::string(["a".to_string(), "b".to_string()])
            );
        }

        #[test]
        fn test_all_defined_uint1() {
            let mut test_instance = Frame::new(vec![Column::uint1("test", [])]);

            let layout = Layout::new(&[ValueKind::Uint1]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Uint1(1)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Uint1(2)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::uint1([1, 2]));
        }

        #[test]
        fn test_all_defined_uint2() {
            let mut test_instance = Frame::new(vec![Column::uint2("test", [])]);

            let layout = Layout::new(&[ValueKind::Uint2]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Uint2(100)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Uint2(200)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::uint2([100, 200]));
        }

        #[test]
        fn test_all_defined_uint4() {
            let mut test_instance = Frame::new(vec![Column::uint4("test", [])]);

            let layout = Layout::new(&[ValueKind::Uint4]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Uint4(1000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Uint4(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::uint4([1000, 2000]));
        }

        #[test]
        fn test_all_defined_uint8() {
            let mut test_instance = Frame::new(vec![Column::uint8("test", [])]);

            let layout = Layout::new(&[ValueKind::Uint8]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Uint8(10000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Uint8(20000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::uint8([10000, 20000]));
        }

        #[test]
        fn test_all_defined_uint16() {
            let mut test_instance = Frame::new(vec![Column::uint16("test", [])]);

            let layout = Layout::new(&[ValueKind::Uint16]);
            let mut row_one = layout.allocate_row();
            layout.set_values(&mut row_one, &[Value::Uint16(1000)]);
            let mut row_two = layout.allocate_row();
            layout.set_values(&mut row_two, &[Value::Uint16(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::uint16([1000, 2000]));
        }

        #[test]
        fn test_row_with_undefined() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[ValueKind::Int2, ValueKind::Bool]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Undefined, Value::Bool(false)]);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int2_with_validity(vec![1, 0], vec![true, false])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::bool_with_validity([true, false], [true, true])
            );
        }

        #[test]
        fn test_row_with_type_mismatch_fails() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[ValueKind::Bool, ValueKind::Bool]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Bool(true), Value::Bool(true)]);

            let result = test_instance.append_rows(&layout, [row]);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("type mismatch"));
        }

        #[test]
        fn test_row_wrong_length_fails() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[ValueKind::Int2]);
            let mut row = layout.allocate_row();
            layout.set_values(&mut row, &[Value::Int2(2)]);

            let result = test_instance.append_rows(&layout, [row]);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("mismatched column count"));
        }

        #[test]
        fn test_fallback_bool() {
            let mut test_instance =
                Frame::new(vec![Column::bool("test", []), Column::bool("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Bool, ValueKind::Bool]);
            let mut row_one = layout.allocate_row();
            layout.set_bool(&mut row_one, 0, true);
            layout.set_undefined(&mut row_one, 1);

            test_instance.append_rows(&layout, [row_one]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::bool_with_validity([true], [true])
            );

            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::bool_with_validity([false], [false])
            );
        }

        #[test]
        fn test_fallback_float4() {
            let mut test_instance =
                Frame::new(vec![Column::float4("test", []), Column::float4("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Float4, ValueKind::Float4]);
            let mut row = layout.allocate_row();
            layout.set_f32(&mut row, 0, 1.5);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::float4_with_validity([1.5], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::float4_with_validity([0.0], [false])
            );
        }

        #[test]
        fn test_fallback_float8() {
            let mut test_instance =
                Frame::new(vec![Column::float8("test", []), Column::float8("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Float8, ValueKind::Float8]);
            let mut row = layout.allocate_row();
            layout.set_f64(&mut row, 0, 2.5);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::float8_with_validity([2.5], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::float8_with_validity([0.0], [false])
            );
        }

        #[test]
        fn test_fallback_int1() {
            let mut test_instance =
                Frame::new(vec![Column::int1("test", []), Column::int1("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Int1, ValueKind::Int1]);
            let mut row = layout.allocate_row();
            layout.set_i8(&mut row, 0, 42);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int1_with_validity([42], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::int1_with_validity([0], [false])
            );
        }

        #[test]
        fn test_fallback_int2() {
            let mut test_instance =
                Frame::new(vec![Column::int2("test", []), Column::int2("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Int2, ValueKind::Int2]);
            let mut row = layout.allocate_row();
            layout.set_i16(&mut row, 0, -1234);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int2_with_validity([-1234], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::int2_with_validity([0], [false])
            );
        }

        #[test]
        fn test_fallback_int4() {
            let mut test_instance =
                Frame::new(vec![Column::int4("test", []), Column::int4("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Int4, ValueKind::Int4]);
            let mut row = layout.allocate_row();
            layout.set_i32(&mut row, 0, 56789);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int4_with_validity([56789], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::int4_with_validity([0], [false])
            );
        }

        #[test]
        fn test_fallback_int8() {
            let mut test_instance =
                Frame::new(vec![Column::int8("test", []), Column::int8("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Int8, ValueKind::Int8]);
            let mut row = layout.allocate_row();
            layout.set_i64(&mut row, 0, -987654321);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int8_with_validity([-987654321], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::int8_with_validity([0], [false])
            );
        }

        #[test]
        fn test_fallback_int16() {
            let mut test_instance =
                Frame::new(vec![Column::int16("test", []), Column::int16("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Int16, ValueKind::Int16]);
            let mut row = layout.allocate_row();
            layout.set_i128(&mut row, 0, 123456789012345678901234567890i128);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int16_with_validity([123456789012345678901234567890i128], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::int16_with_validity([0], [false])
            );
        }

        #[test]
        fn test_fallback_string() {
            let mut test_instance =
                Frame::new(vec![Column::string("test", []), Column::string("undefined", [])]);

            let layout = Layout::new(&[ValueKind::String, ValueKind::String]);
            let mut row = layout.allocate_row();
            layout.set_str(&mut row, 0, "reifydb");
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::string_with_validity(["reifydb".to_string()], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::string_with_validity(["".to_string()], [false])
            );
        }

        #[test]
        fn test_fallback_uint1() {
            let mut test_instance =
                Frame::new(vec![Column::uint1("test", []), Column::uint1("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Uint1, ValueKind::Uint1]);
            let mut row = layout.allocate_row();
            layout.set_u8(&mut row, 0, 255);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::uint1_with_validity([255], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::uint1_with_validity([0], [false])
            );
        }

        #[test]
        fn test_fallback_uint2() {
            let mut test_instance =
                Frame::new(vec![Column::uint2("test", []), Column::uint2("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Uint2, ValueKind::Uint2]);
            let mut row = layout.allocate_row();
            layout.set_u16(&mut row, 0, 65535);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::uint2_with_validity([65535], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::uint2_with_validity([0], [false])
            );
        }

        #[test]
        fn test_fallback_uint4() {
            let mut test_instance =
                Frame::new(vec![Column::uint4("test", []), Column::uint4("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Uint4, ValueKind::Uint4]);
            let mut row = layout.allocate_row();
            layout.set_u32(&mut row, 0, 4294967295);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::uint4_with_validity([4294967295], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::uint4_with_validity([0], [false])
            );
        }

        #[test]
        fn test_fallback_uint8() {
            let mut test_instance =
                Frame::new(vec![Column::uint8("test", []), Column::uint8("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Uint8, ValueKind::Uint8]);
            let mut row = layout.allocate_row();
            layout.set_u64(&mut row, 0, 18446744073709551615u64);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::uint8_with_validity([18446744073709551615], [true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::uint8_with_validity([0], [false])
            );
        }

        #[test]
        fn test_fallback_uint16() {
            let mut test_instance =
                Frame::new(vec![Column::uint16("test", []), Column::uint16("undefined", [])]);

            let layout = Layout::new(&[ValueKind::Uint16, ValueKind::Uint16]);
            let mut row = layout.allocate_row();
            layout.set_u128(&mut row, 0, 340282366920938463463374607431768211455u128);
            layout.set_undefined(&mut row, 1);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::uint16_with_validity(
                    [340282366920938463463374607431768211455u128],
                    [true]
                )
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::uint16_with_validity([0], [false])
            );
        }

        fn test_instance_with_columns() -> Frame {
            Frame::new(vec![
                Column { name: "int2".into(), data: ColumnValues::int2(vec![1]) },
                Column { name: "bool".into(), data: ColumnValues::bool(vec![true]) },
            ])
        }
    }
}
