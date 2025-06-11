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

        for (index, (col, value)) in self.columns.iter_mut().zip(values).enumerate() {
            match (&mut col.data, value) {
                (ColumnValues::Bool(vec, valid), ValueKind::Bool) => {
                    for row in &rows {
                        vec.push(layout.get_bool(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Float4(vec, valid), ValueKind::Float4) => {
                    for row in &rows {
                        vec.push(layout.get_f32(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Float8(vec, valid), ValueKind::Float8) => {
                    for row in &rows {
                        vec.push(layout.get_f64(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Int1(vec, valid), ValueKind::Int1) => {
                    for row in &rows {
                        vec.push(layout.get_i8(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Int2(vec, valid), ValueKind::Int2) => {
                    for row in &rows {
                        vec.push(layout.get_i16(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Int4(vec, valid), ValueKind::Int4) => {
                    for row in &rows {
                        vec.push(layout.get_i32(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Int8(vec, valid), ValueKind::Int8) => {
                    for row in &rows {
                        vec.push(layout.get_i64(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Int16(vec, valid), ValueKind::Int16) => {
                    for row in &rows {
                        vec.push(layout.get_i128(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Uint1(vec, valid), ValueKind::Uint1) => {
                    for row in &rows {
                        vec.push(layout.get_u8(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Uint2(vec, valid), ValueKind::Uint2) => {
                    for row in &rows {
                        vec.push(layout.get_u16(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Uint4(vec, valid), ValueKind::Uint4) => {
                    for row in &rows {
                        vec.push(layout.get_u32(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Uint8(vec, valid), ValueKind::Uint8) => {
                    for row in &rows {
                        vec.push(layout.get_u64(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::Uint16(vec, valid), ValueKind::Uint16) => {
                    for row in &rows {
                        vec.push(layout.get_u128(&row, index));
                        valid.push(true);
                    }
                }
                (ColumnValues::String(vec, valid), ValueKind::String) => {
                    for row in &rows {
                        vec.push(layout.get_str(&row, index).to_string());
                        valid.push(true);
                    }
                }
                (ColumnValues::Undefined(n), ValueKind::Undefined) => {
                    *n += rows.len();
                }

                (ColumnValues::Undefined(n), v) => {
                    let mut new_column = match v {
                        ValueKind::Bool => {
                            let mut values = CowVec::new(vec![false; *n]);
                            for row in &rows {
                                values.push(layout.get_bool(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::bool_with_validity(values, validity)
                        }
                        ValueKind::Float4 => {
                            let mut values = CowVec::new(vec![0.0f32; *n]);
                            for row in &rows {
                                values.push(layout.get_f32(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::float4_with_validity(values, validity)
                        }
                        ValueKind::Float8 => {
                            let mut values = CowVec::new(vec![0.0f64; *n]);
                            for row in &rows {
                                values.push(layout.get_f64(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::float8_with_validity(values, validity)
                        }
                        ValueKind::Int1 => {
                            let mut values = CowVec::new(vec![0i8; *n]);
                            for row in &rows {
                                values.push(layout.get_i8(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::int1_with_validity(values, validity)
                        }
                        ValueKind::Int2 => {
                            let mut values = CowVec::new(vec![0i16; *n]);
                            for row in &rows {
                                values.push(layout.get_i16(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::int2_with_validity(values, validity)
                        }
                        ValueKind::Int4 => {
                            let mut values = CowVec::new(vec![0i32; *n]);
                            for row in &rows {
                                values.push(layout.get_i32(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::int4_with_validity(values, validity)
                        }
                        ValueKind::Int8 => {
                            let mut values = CowVec::new(vec![0i64; *n]);
                            for row in &rows {
                                values.push(layout.get_i64(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::int8_with_validity(values, validity)
                        }
                        ValueKind::Int16 => {
                            let mut values = CowVec::new(vec![0i128; *n]);
                            for row in &rows {
                                values.push(layout.get_i128(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::int16_with_validity(values, validity)
                        }
                        ValueKind::Uint1 => {
                            let mut values = CowVec::new(vec![0u8; *n]);
                            for row in &rows {
                                values.push(layout.get_u8(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::uint1_with_validity(values, validity)
                        }
                        ValueKind::Uint2 => {
                            let mut values = CowVec::new(vec![0u16; *n]);
                            for row in &rows {
                                values.push(layout.get_u16(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::uint2_with_validity(values, validity)
                        }
                        ValueKind::Uint4 => {
                            let mut values = CowVec::new(vec![0u32; *n]);
                            for row in &rows {
                                values.push(layout.get_u32(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::uint4_with_validity(values, validity)
                        }
                        ValueKind::Uint8 => {
                            let mut values = CowVec::new(vec![0u64; *n]);
                            for row in &rows {
                                values.push(layout.get_u64(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::uint8_with_validity(values, validity)
                        }
                        ValueKind::Uint16 => {
                            let mut values = CowVec::new(vec![0u128; *n]);
                            for row in &rows {
                                values.push(layout.get_u128(&row, index));
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::uint16_with_validity(values, validity)
                        }
                        ValueKind::String => {
                            let mut values = CowVec::new(vec!["".to_string(); *n]);
                            for row in &rows {
                                values.push(layout.get_str(&row, index).to_string());
                            }

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::string_with_validity(values, validity)
                        }
                        ValueKind::Undefined => unreachable!(),
                    };

                    std::mem::swap(&mut col.data, &mut new_column);
                }
                (_, _) => {
                    return Err(format!(
                        "type mismatch for column '{}'({}): incompatible with {}",
                        col.name,
                        col.data.value(),
                        value
                    )
                    .into());
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    mod frame {
        use crate::*;

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
        fn test_mismatched_columns() {
            let mut test_instance = Frame::new(vec![]);

            let layout = Layout::new(&[ValueKind::Int2]);
            let mut row = layout.allocate_row();
            layout.write_values(row.make_mut(), &[Value::Int2(2)]);

            let err = test_instance.append_rows(&layout, [row]).err().unwrap();
            assert_eq!(err.to_string(), "mismatched column count: expected 0, got 1");
        }

        #[test]
        fn test_ok() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[ValueKind::Int2, ValueKind::Bool]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Int2(2), Value::Bool(true)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Int2(3), Value::Bool(false)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int2([1, 2, 3]));
            assert_eq!(test_instance.columns[1].data, ColumnValues::bool([true, true, false]));
        }

        #[test]
        fn test_bool() {
            let mut test_instance = Frame::new(vec![Column::bool("test", [])]);

            let layout = Layout::new(&[ValueKind::Bool]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Bool(true)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Bool(false)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::bool([true, false]));
        }

        #[test]
        fn test_float4() {
            let mut test_instance = Frame::new(vec![Column::float4("test", [])]);

            let layout = Layout::new(&[ValueKind::Float4]);
            let mut row_one = layout.allocate_row();
            layout.write_values(
                row_one.make_mut(),
                &[Value::Float4(OrderedF32::try_from(1.0).unwrap())],
            );
            let mut row_two = layout.allocate_row();
            layout.write_values(
                row_two.make_mut(),
                &[Value::Float4(OrderedF32::try_from(2.0).unwrap())],
            );

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::float4([1.0, 2.0]));
        }

        #[test]
        fn test_float8() {
            let mut test_instance = Frame::new(vec![Column::float8("test", [])]);

            let layout = Layout::new(&[ValueKind::Float8]);
            let mut row_one = layout.allocate_row();
            layout.write_values(
                row_one.make_mut(),
                &[Value::Float8(OrderedF64::try_from(1.0).unwrap())],
            );
            let mut row_two = layout.allocate_row();
            layout.write_values(
                row_two.make_mut(),
                &[Value::Float8(OrderedF64::try_from(2.0).unwrap())],
            );

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::float8([1.0, 2.0]));
        }

        #[test]
        fn test_int1() {
            let mut test_instance = Frame::new(vec![Column::int1("test", [])]);

            let layout = Layout::new(&[ValueKind::Int1]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Int1(1)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Int1(2)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int1([1, 2]));
        }

        #[test]
        fn test_int2() {
            let mut test_instance = Frame::new(vec![Column::int2("test", [])]);

            let layout = Layout::new(&[ValueKind::Int2]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Int2(100)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Int2(200)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int2([100, 200]));
        }

        #[test]
        fn test_int4() {
            let mut test_instance = Frame::new(vec![Column::int4("test", [])]);

            let layout = Layout::new(&[ValueKind::Int4]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Int4(1000)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Int4(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int4([1000, 2000]));
        }

        #[test]
        fn test_int8() {
            let mut test_instance = Frame::new(vec![Column::int8("test", [])]);

            let layout = Layout::new(&[ValueKind::Int8]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Int8(10000)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Int8(20000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int8([10000, 20000]));
        }

        #[test]
        fn test_int16() {
            let mut test_instance = Frame::new(vec![Column::int16("test", [])]);

            let layout = Layout::new(&[ValueKind::Int16]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Int16(1000)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Int16(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int16([1000, 2000]));
        }

        #[test]
        fn test_string() {
            let mut test_instance = Frame::new(vec![Column::string("test", [])]);

            let layout = Layout::new(&[ValueKind::String]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::String("a".into())]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::String("b".into())]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::string(["a".to_string(), "b".to_string()])
            );
        }

        #[test]
        fn test_uint1() {
            let mut test_instance = Frame::new(vec![Column::uint1("test", [])]);

            let layout = Layout::new(&[ValueKind::Uint1]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Uint1(1)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Uint1(2)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::uint1([1, 2]));
        }

        #[test]
        fn test_uint2() {
            let mut test_instance = Frame::new(vec![Column::uint2("test", [])]);

            let layout = Layout::new(&[ValueKind::Uint2]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Uint2(100)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Uint2(200)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::uint2([100, 200]));
        }

        #[test]
        fn test_uint4() {
            let mut test_instance = Frame::new(vec![Column::uint4("test", [])]);

            let layout = Layout::new(&[ValueKind::Uint4]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Uint4(1000)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Uint4(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::uint4([1000, 2000]));
        }

        #[test]
        fn test_uint8() {
            let mut test_instance = Frame::new(vec![Column::uint8("test", [])]);

            let layout = Layout::new(&[ValueKind::Uint8]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Uint8(10000)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Uint8(20000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::uint8([10000, 20000]));
        }

        #[test]
        fn test_uint16() {
            let mut test_instance = Frame::new(vec![Column::uint16("test", [])]);

            let layout = Layout::new(&[ValueKind::Uint16]);
            let mut row_one = layout.allocate_row();
            layout.write_values(row_one.make_mut(), &[Value::Uint16(1000)]);
            let mut row_two = layout.allocate_row();
            layout.write_values(row_two.make_mut(), &[Value::Uint16(2000)]);

            test_instance.append_rows(&layout, [row_one, row_two]).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::uint16([1000, 2000]));
        }

        #[test]
        #[ignore]
        fn test_row_with_undefined() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[ValueKind::Undefined, ValueKind::Bool]);
            let mut row = layout.allocate_row();
            layout.write_values(row.make_mut(), &[Value::Undefined, Value::Bool(true)]);

            test_instance.append_rows(&layout, [row]).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int2_with_validity(vec![1, 0], vec![true, false])
            );
            assert_eq!(
                test_instance.columns[2].data,
                ColumnValues::bool_with_validity([true, false], [true, true])
            );
        }

        #[test]
        fn test_row_with_type_mismatch_fails() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[ValueKind::Bool, ValueKind::Bool]);
            let mut row = layout.allocate_row();
            layout.write_values(row.make_mut(), &[Value::Bool(true), Value::Bool(true)]);

            let result = test_instance.append_rows(&layout, [row]);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("type mismatch"));
        }

        #[test]
        fn test_row_wrong_length_fails() {
            let mut test_instance = test_instance_with_columns();

            let layout = Layout::new(&[ValueKind::Int2]);
            let mut row = layout.allocate_row();
            layout.write_values(row.make_mut(), &[Value::Int2(2)]);

            let result = test_instance.append_rows(&layout, [row]);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("mismatched column count"));
        }

        fn test_instance_with_columns() -> Frame {
            Frame::new(vec![
                Column { name: "int2".into(), data: ColumnValues::int2(vec![1]) },
                Column { name: "bool".into(), data: ColumnValues::bool(vec![true]) },
            ])
        }
    }
}
