// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{ColumnValues, Frame};
use reifydb_core::{CowVec, Row, Value};

pub trait Append<T> {
    fn append(&mut self, other: T) -> crate::Result<()>;
}

impl Append<Frame> for Frame {
    fn append(&mut self, other: Frame) -> crate::Result<()> {
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

impl Append<Row> for Frame {
    fn append(&mut self, other: Row) -> crate::Result<()> {
        if self.columns.len() != other.len() {
            return Err(format!(
                "mismatched column count: expected {}, got {}",
                self.columns.len(),
                other.len()
            )
            .into());
        }

        for (col, value) in self.columns.iter_mut().zip(other.into_iter()) {
            match (&mut col.data, value) {
                (ColumnValues::Bool(vec, valid), Value::Bool(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Bool(vec, valid), Value::Undefined) => {
                    vec.push(false);
                    valid.push(false);
                }

                (ColumnValues::Float4(vec, valid), Value::Float4(v)) => {
                    vec.push(v.value());
                    valid.push(true);
                }
                (ColumnValues::Float4(vec, valid), Value::Undefined) => {
                    vec.push(0.0f32);
                    valid.push(false);
                }

                (ColumnValues::Float8(vec, valid), Value::Float8(v)) => {
                    vec.push(v.value());
                    valid.push(true);
                }
                (ColumnValues::Float8(vec, valid), Value::Undefined) => {
                    vec.push(0.0f64);
                    valid.push(false);
                }

                (ColumnValues::Int1(vec, valid), Value::Int1(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Int1(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::Int2(vec, valid), Value::Int2(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Int2(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::Int4(vec, valid), Value::Int4(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Int4(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::Int8(vec, valid), Value::Int8(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Int8(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::Int16(vec, valid), Value::Int16(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Int16(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::String(vec, valid), Value::String(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::String(vec, valid), Value::Undefined) => {
                    vec.push(String::new());
                    valid.push(false);
                }

                (ColumnValues::Uint1(vec, valid), Value::Uint1(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Uint1(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::Uint2(vec, valid), Value::Uint2(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Uint2(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::Uint4(vec, valid), Value::Uint4(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Uint4(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::Uint8(vec, valid), Value::Uint8(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Uint8(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::Uint16(vec, valid), Value::Uint16(v)) => {
                    vec.push(v);
                    valid.push(true);
                }
                (ColumnValues::Uint16(vec, valid), Value::Undefined) => {
                    vec.push(0);
                    valid.push(false);
                }

                (ColumnValues::Undefined(n), Value::Undefined) => {
                    *n += 1;
                }

                (ColumnValues::Undefined(n), v) => {
                    let mut new_column = match v {
                        Value::Bool(b) => {
                            let mut values = CowVec::new(vec![false; *n]);
                            values.extend([b]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::bool_with_validity(values, validity)
                        }

                        Value::Float4(f) => {
                            let mut values = CowVec::new(vec![0.0f32; *n]);
                            values.extend([f.value()]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::float4_with_validity(values, validity)
                        }
                        Value::Float8(f) => {
                            let mut values = CowVec::new(vec![0.0f64; *n]);
                            values.extend([f.value()]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::float8_with_validity(values, validity)
                        }

                        Value::Int1(i) => {
                            let mut values = CowVec::new(vec![0i8; *n]);
                            values.extend([i]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::int1_with_validity(values, validity)
                        }
                        Value::Int2(i) => {
                            let mut values = CowVec::new(vec![0i16; *n]);
                            values.extend([i]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::int2_with_validity(values, validity)
                        }
                        Value::Int4(i) => {
                            let mut values = CowVec::new(vec![0i32; *n]);
                            values.extend([i]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::int4_with_validity(values, validity)
                        }
                        Value::Int8(i) => {
                            let mut values = CowVec::new(vec![0i64; *n]);
                            values.extend([i]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::int8_with_validity(values, validity)
                        }
                        Value::Int16(i) => {
                            let mut values = CowVec::new(vec![0i128; *n]);
                            values.extend([i]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::int16_with_validity(values, validity)
                        }

                        Value::Uint1(u) => {
                            let mut values = CowVec::new(vec![0u8; *n]);
                            values.extend([u]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::uint1_with_validity(values, validity)
                        }
                        Value::Uint2(u) => {
                            let mut values = CowVec::new(vec![0u16; *n]);
                            values.extend([u]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::uint2_with_validity(values, validity)
                        }
                        Value::Uint4(u) => {
                            let mut values = CowVec::new(vec![0u32; *n]);
                            values.extend([u]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::uint4_with_validity(values, validity)
                        }
                        Value::Uint8(u) => {
                            let mut values = CowVec::new(vec![0u64; *n]);
                            values.extend([u]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::uint8_with_validity(values, validity)
                        }
                        Value::Uint16(u) => {
                            let mut values = CowVec::new(vec![0u128; *n]);
                            values.extend([u]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::uint16_with_validity(values, validity)
                        }

                        Value::String(s) => {
                            let mut values = CowVec::new(vec![String::new(); *n]);
                            values.extend([s]);

                            let mut validity = CowVec::new(vec![false; *n]);
                            validity.extend([true]);

                            ColumnValues::string_with_validity(values, validity)
                        }

                        Value::Undefined => unreachable!(),
                    };

                    std::mem::swap(&mut col.data, &mut new_column);
                }

                (_, v) => {
                    return Err(format!(
						"type mismatch for column '{}'({}): incompatible with value {}",
						col.name,
						col.data.value(),
						v.kind()
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
        use crate::transform::append::Append;
        use crate::*;

        #[test]
        fn test_boolean() {
            let mut test_instance1 =
                Frame::new(vec![Column::bool_with_validity("id", [true], [false])]);

            let test_instance2 =
                Frame::new(vec![Column::bool_with_validity("id", [false], [true])]);

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            test_instance1.append(test_instance2).unwrap();

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

            let result = test_instance1.append(test_instance2);
            assert!(result.is_err());
        }

        #[test]
        fn test_fails_on_column_name_mismatch() {
            let mut test_instance1 = Frame::new(vec![Column::int2("id", [1])]);

            let test_instance2 = Frame::new(vec![Column::int2("wrong", [2])]);

            let result = test_instance1.append(test_instance2);
            assert!(result.is_err());
        }

        #[test]
        fn test_fails_on_type_mismatch() {
            let mut test_instance1 = Frame::new(vec![Column::int2("id", [1])]);

            let test_instance2 = Frame::new(vec![Column::string("id", ["A"])]);

            let result = test_instance1.append(test_instance2);
            assert!(result.is_err());
        }
    }

    mod row {
        use crate::{Append, Column, ColumnValues, Frame};
        use reifydb_core::Value;

        #[test]
        fn test_to_empty() {
            let mut test_instance = Frame::new(vec![]);

            let row = vec![Value::Int2(2), Value::String("Bob".into()), Value::Bool(false)];

            let err = test_instance.append(row).err().unwrap();
            assert_eq!(err.to_string(), "mismatched column count: expected 0, got 3");
        }

        #[test]
        fn test_row_matching_types() {
            let mut test_instance = test_instance_with_columns();

            let row = vec![Value::Int2(2), Value::String("Bob".into()), Value::Bool(false)];

            test_instance.append(row).unwrap();

            assert_eq!(test_instance.columns[0].data, ColumnValues::int2([1, 2]));
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::string(["Alice".to_string(), "Bob".to_string()])
            );
            assert_eq!(test_instance.columns[2].data, ColumnValues::bool([true, false]));
        }

        #[test]
        fn test_row_with_undefined() {
            let mut test_instance = test_instance_with_columns();

            let row = vec![Value::Undefined, Value::String("Karen".into()), Value::Undefined];

            test_instance.append(row).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int2_with_validity(vec![1, 0], vec![true, false])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::string_with_validity(
                    ["Alice".to_string(), "Karen".to_string()],
                    [true, true]
                )
            );
            assert_eq!(
                test_instance.columns[2].data,
                ColumnValues::bool_with_validity([true, false], [true, false])
            );
        }

        #[test]
        fn test_row_with_type_mismatch_fails() {
            let mut test_instance = test_instance_with_columns();

            let row = vec![
                Value::Bool(true), // should be Int2
                Value::String("Eve".into()),
                Value::Bool(false),
            ];

            let result = test_instance.append(row);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("type mismatch"));
        }

        #[test]
        fn test_row_wrong_length_fails() {
            let mut test_instance = test_instance_with_columns();

            let row = vec![
                Value::Int2(42),
                Value::String("X".into()),
                Value::Bool(true),
                Value::Bool(false),
            ];

            let result = test_instance.append(row);
            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("mismatched column count"));
        }

        #[test]
        fn test_row_to_undefined_columns_promotes() {
            let mut test_instance = Frame::new(vec![
                Column { name: "age".into(), data: ColumnValues::Undefined(1) },
                Column { name: "name".into(), data: ColumnValues::Undefined(1) },
            ]);

            let row = vec![Value::Int2(30), Value::String("Zoe".into())];
            test_instance.append(row).unwrap();

            assert_eq!(
                test_instance.columns[0].data,
                ColumnValues::int2_with_validity(vec![0, 30], vec![false, true])
            );
            assert_eq!(
                test_instance.columns[1].data,
                ColumnValues::string_with_validity(
                    vec!["".to_string(), "Zoe".to_string()],
                    vec![false, true]
                )
            );
        }

        fn test_instance_with_columns() -> Frame {
            Frame::new(vec![
                Column { name: "int2".into(), data: ColumnValues::int2(vec![1]) },
                Column {
                    name: "text".into(),
                    data: ColumnValues::string(vec!["Alice".to_string()]),
                },
                Column { name: "bool".into(), data: ColumnValues::bool(vec![true]) },
            ])
        }
    }
}
