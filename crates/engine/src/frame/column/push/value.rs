// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use reifydb_core::{BitVec, Date, DateTime, Interval, RowId, Time, Value};

impl ColumnValues {
    pub fn push_value(&mut self, value: Value) {
        match value {
            Value::Bool(v) => match self {
                ColumnValues::Bool(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![false; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::bool_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Float4(v) => match self {
                ColumnValues::Float4(_, _) => self.push(v.value()),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0.0f32; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v.value());
                    bitvec.push(true);
                    *self = ColumnValues::float4_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Float8(v) => match self {
                ColumnValues::Float8(_, _) => self.push(v.value()),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0.0f64; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v.value());
                    bitvec.push(true);
                    *self = ColumnValues::float8_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Int1(v) => match self {
                ColumnValues::Int1(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::int1_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Int2(v) => match self {
                ColumnValues::Int2(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::int2_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Int4(v) => match self {
                ColumnValues::Int4(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::int4_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Int8(v) => match self {
                ColumnValues::Int8(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::int8_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Int16(v) => match self {
                ColumnValues::Int16(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::int16_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Utf8(v) => match self {
                ColumnValues::Utf8(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec!["".to_string(); *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::utf8_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Uint1(v) => match self {
                ColumnValues::Uint1(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::uint1_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Uint2(v) => match self {
                ColumnValues::Uint2(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::uint2_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Uint4(v) => match self {
                ColumnValues::Uint4(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::uint4_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Uint8(v) => match self {
                ColumnValues::Uint8(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::uint8_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Uint16(v) => match self {
                ColumnValues::Uint16(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![0; *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::uint16_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Date(v) => match self {
                ColumnValues::Date(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![Date::default(); *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::date_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::DateTime(v) => match self {
                ColumnValues::DateTime(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![DateTime::default(); *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::datetime_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Time(v) => match self {
                ColumnValues::Time(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![Time::default(); *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::time_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Interval(v) => match self {
                ColumnValues::Interval(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![Interval::default(); *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::interval_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Undefined => self.push_undefined(),
            Value::RowId(row_id) => match self {
                ColumnValues::RowId(values, bitvec) => {
                    values.push(row_id);
                    bitvec.push(true);
                }
                ColumnValues::Undefined(len) => {
                    let mut values = vec![RowId::default(); *len];
                    let mut bitvec = BitVec::new(*len, false);
                    values.push(row_id);
                    bitvec.push(true);
                    *self = ColumnValues::row_id_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::frame::ColumnValues;
    use reifydb_core::Value;
    use reifydb_core::{OrderedF32, OrderedF64};

    #[test]
    fn test_bool() {
        let mut col = ColumnValues::bool(vec![true]);
        col.push_value(Value::Bool(false));
        if let ColumnValues::Bool(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![true, false]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_bool() {
        let mut col = ColumnValues::bool(vec![true]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Bool(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![true, false]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_bool() {
        let mut col = ColumnValues::Undefined(2);
        col.push_value(Value::Bool(true));
        if let ColumnValues::Bool(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![false, false, true]);
            assert_eq!(bitvec.to_vec(), vec![false, false, true]);
        }
    }

    #[test]
    fn test_float4() {
        let mut col = ColumnValues::float4(vec![1.0]);
        col.push_value(Value::Float4(OrderedF32::try_from(2.0).unwrap()));
        if let ColumnValues::Float4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1.0, 2.0]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_float4() {
        let mut col = ColumnValues::float4(vec![1.0]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Float4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1.0, 0.0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_float4() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Float4(OrderedF32::try_from(3.14).unwrap()));
        if let ColumnValues::Float4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0.0, 3.14]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_float8() {
        let mut col = ColumnValues::float8(vec![1.0]);
        col.push_value(Value::Float8(OrderedF64::try_from(2.0).unwrap()));
        if let ColumnValues::Float8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1.0, 2.0]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_float8() {
        let mut col = ColumnValues::float8(vec![1.0]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Float8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1.0, 0.0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_float8() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Float8(OrderedF64::try_from(2.718).unwrap()));
        if let ColumnValues::Float8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0.0, 2.718]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_int1() {
        let mut col = ColumnValues::int1(vec![1]);
        col.push_value(Value::Int1(2));
        if let ColumnValues::Int1(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1, 2]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_int1() {
        let mut col = ColumnValues::int1(vec![1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Int1(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_int1() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Int1(5));
        if let ColumnValues::Int1(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0, 5]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_int2() {
        let mut col = ColumnValues::int2(vec![1]);
        col.push_value(Value::Int2(3));
        if let ColumnValues::Int2(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1, 3]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_int2() {
        let mut col = ColumnValues::int2(vec![1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Int2(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_int2() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Int2(10));
        if let ColumnValues::Int2(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0, 10]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_int4() {
        let mut col = ColumnValues::int4(vec![10]);
        col.push_value(Value::Int4(20));
        if let ColumnValues::Int4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![10, 20]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_int4() {
        let mut col = ColumnValues::int4(vec![10]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Int4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![10, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_int4() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Int4(20));
        if let ColumnValues::Int4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0, 20]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_int8() {
        let mut col = ColumnValues::int8(vec![100]);
        col.push_value(Value::Int8(200));
        if let ColumnValues::Int8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![100, 200]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_int8() {
        let mut col = ColumnValues::int8(vec![100]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Int8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![100, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_int8() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Int8(30));
        if let ColumnValues::Int8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0, 30]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_int16() {
        let mut col = ColumnValues::int16(vec![1000]);
        col.push_value(Value::Int16(2000));
        if let ColumnValues::Int16(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1000, 2000]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_int16() {
        let mut col = ColumnValues::int16(vec![1000]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Int16(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1000, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_int16() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Int16(40));
        if let ColumnValues::Int16(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0, 40]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_uint1() {
        let mut col = ColumnValues::uint1(vec![1]);
        col.push_value(Value::Uint1(2));
        if let ColumnValues::Uint1(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1, 2]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_uint1() {
        let mut col = ColumnValues::uint1(vec![1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Uint1(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_uint1() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Uint1(1));
        if let ColumnValues::Uint1(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0, 1]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_uint2() {
        let mut col = ColumnValues::uint2(vec![10]);
        col.push_value(Value::Uint2(20));
        if let ColumnValues::Uint2(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![10, 20]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_uint2() {
        let mut col = ColumnValues::uint2(vec![10]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Uint2(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![10, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_uint2() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Uint2(2));
        if let ColumnValues::Uint2(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0, 2]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_uint4() {
        let mut col = ColumnValues::uint4(vec![100]);
        col.push_value(Value::Uint4(200));
        if let ColumnValues::Uint4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![100, 200]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_uint4() {
        let mut col = ColumnValues::uint4(vec![100]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Uint4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![100, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_uint4() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Uint4(3));
        if let ColumnValues::Uint4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0, 3]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_uint8() {
        let mut col = ColumnValues::uint8(vec![1000]);
        col.push_value(Value::Uint8(2000));
        if let ColumnValues::Uint8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1000, 2000]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_uint8() {
        let mut col = ColumnValues::uint8(vec![1000]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Uint8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1000, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_uint8() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Uint8(4));
        if let ColumnValues::Uint8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0, 4]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_uint16() {
        let mut col = ColumnValues::uint16(vec![10000]);
        col.push_value(Value::Uint16(20000));
        if let ColumnValues::Uint16(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![10000, 20000]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_uint16() {
        let mut col = ColumnValues::uint16(vec![10000]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Uint16(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![10000, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_uint16() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Uint16(5));
        if let ColumnValues::Uint16(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![0, 5]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_utf8() {
        let mut col = ColumnValues::utf8(vec!["hello".to_string()]);
        col.push_value(Value::Utf8("world".to_string()));
        if let ColumnValues::Utf8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec!["hello", "world"]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_utf8() {
        let mut col = ColumnValues::utf8(vec!["hello".to_string()]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Utf8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec!["hello", ""]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_utf8() {
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Utf8("ok".to_string()));
        if let ColumnValues::Utf8(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec!["", "ok"]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_undefined() {
        let mut col = ColumnValues::int2(vec![1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Int2(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![1, 0]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }
}
