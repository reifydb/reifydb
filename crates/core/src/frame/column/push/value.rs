// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use crate::value::uuid::{Uuid4, Uuid7};
use crate::value::Blob;
use crate::{BitVec, Date, DateTime, Interval, RowId, Time, Value};

impl ColumnValues {
    pub fn push_value(&mut self, value: Value) {
        match value {
            Value::Bool(v) => match self {
                ColumnValues::Bool(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![false; *len];
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
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
                    let mut bitvec = BitVec::repeat(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::interval_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Uuid4(v) => match self {
                ColumnValues::Uuid4(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![Uuid4::default(); *len];
                    let mut bitvec = BitVec::repeat(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::uuid4_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },

            Value::Uuid7(v) => match self {
                ColumnValues::Uuid7(_, _) => self.push(v),
                ColumnValues::Undefined(len) => {
                    let mut values = vec![Uuid7::default(); *len];
                    let mut bitvec = BitVec::repeat(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::uuid7_with_bitvec(values, bitvec);
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
                    let mut bitvec = BitVec::repeat(*len, false);
                    values.push(row_id);
                    bitvec.push(true);
                    *self = ColumnValues::row_id_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },
            Value::Blob(v) => match self {
                ColumnValues::Blob(values, bitvec) => {
                    values.push(v);
                    bitvec.push(true);
                }
                ColumnValues::Undefined(len) => {
                    let mut values = vec![Blob::new(vec![]); *len];
                    let mut bitvec = BitVec::repeat(*len, false);
                    values.push(v);
                    bitvec.push(true);
                    *self = ColumnValues::blob_with_bitvec(values, bitvec);
                }
                _ => unimplemented!(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Value;
    use crate::frame::ColumnValues;
    use crate::value::uuid::{Uuid4, Uuid7};
    use crate::{OrderedF32, OrderedF64};
    use uuid::Uuid;

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

    #[test]
    fn test_date() {
        use crate::Date;
        let date1 = Date::from_ymd(2023, 1, 1).unwrap();
        let date2 = Date::from_ymd(2023, 12, 31).unwrap();
        let mut col = ColumnValues::date(vec![date1]);
        col.push_value(Value::Date(date2));
        if let ColumnValues::Date(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![date1, date2]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_date() {
        use crate::Date;
        let date1 = Date::from_ymd(2023, 1, 1).unwrap();
        let mut col = ColumnValues::date(vec![date1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Date(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![date1, Date::default()]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_date() {
        use crate::Date;
        let date = Date::from_ymd(2023, 6, 15).unwrap();
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Date(date));
        if let ColumnValues::Date(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![Date::default(), date]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_datetime() {
        use crate::DateTime;
        let dt1 = DateTime::from_timestamp(1672531200).unwrap(); // 2023-01-01 00:00:00 UTC
        let dt2 = DateTime::from_timestamp(1704067200).unwrap(); // 2024-01-01 00:00:00 UTC
        let mut col = ColumnValues::datetime(vec![dt1]);
        col.push_value(Value::DateTime(dt2));
        if let ColumnValues::DateTime(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![dt1, dt2]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_datetime() {
        use crate::DateTime;
        let dt1 = DateTime::from_timestamp(1672531200).unwrap();
        let mut col = ColumnValues::datetime(vec![dt1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::DateTime(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![dt1, DateTime::default()]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_datetime() {
        use crate::DateTime;
        let dt = DateTime::from_timestamp(1672531200).unwrap();
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::DateTime(dt));
        if let ColumnValues::DateTime(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![DateTime::default(), dt]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_time() {
        use crate::Time;
        let time1 = Time::from_hms(12, 30, 0).unwrap();
        let time2 = Time::from_hms(18, 45, 30).unwrap();
        let mut col = ColumnValues::time(vec![time1]);
        col.push_value(Value::Time(time2));
        if let ColumnValues::Time(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![time1, time2]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_time() {
        use crate::Time;
        let time1 = Time::from_hms(12, 30, 0).unwrap();
        let mut col = ColumnValues::time(vec![time1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Time(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![time1, Time::default()]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_time() {
        use crate::Time;
        let time = Time::from_hms(15, 20, 10).unwrap();
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Time(time));
        if let ColumnValues::Time(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![Time::default(), time]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_interval() {
        use crate::Interval;
        let interval1 = Interval::from_days(30);
        let interval2 = Interval::from_hours(24);
        let mut col = ColumnValues::interval(vec![interval1]);
        col.push_value(Value::Interval(interval2));
        if let ColumnValues::Interval(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![interval1, interval2]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_interval() {
        use crate::Interval;
        let interval1 = Interval::from_days(30);
        let mut col = ColumnValues::interval(vec![interval1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Interval(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![interval1, Interval::default()]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_interval() {
        use crate::Interval;
        let interval = Interval::from_minutes(90);
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Interval(interval));
        if let ColumnValues::Interval(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![Interval::default(), interval]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_row_id() {
        use crate::RowId;
        let row_id1 = RowId::new(1);
        let row_id2 = RowId::new(2);
        let mut col = ColumnValues::row_id(vec![row_id1]);
        col.push_value(Value::RowId(row_id2));
        if let ColumnValues::RowId(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![row_id1, row_id2]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_row_id() {
        use crate::RowId;
        let row_id1 = RowId::new(1);
        let mut col = ColumnValues::row_id(vec![row_id1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::RowId(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![row_id1, RowId::default()]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_row_id() {
        use crate::RowId;
        let row_id = RowId::new(42);
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::RowId(row_id));
        if let ColumnValues::RowId(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![RowId::default(), row_id]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_uuid4() {
        let uuid1 = Uuid4::generate();
        let uuid2 = Uuid4::generate();
        let mut col = ColumnValues::uuid4(vec![uuid1]);
        col.push_value(Value::Uuid4(uuid2));
        if let ColumnValues::Uuid4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![uuid1, uuid2]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_uuid4() {
        let uuid1 = Uuid4::generate();
        let mut col = ColumnValues::uuid4(vec![uuid1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Uuid4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![uuid1, Uuid4::from(Uuid::nil())]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_uuid4() {
        let uuid = Uuid4::generate();
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Uuid4(uuid));
        if let ColumnValues::Uuid4(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![Uuid4::from(Uuid::nil()), uuid]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }

    #[test]
    fn test_uuid7() {
        let uuid1 = Uuid7::generate();
        let uuid2 = Uuid7::generate();
        let mut col = ColumnValues::uuid7(vec![uuid1]);
        col.push_value(Value::Uuid7(uuid2));
        if let ColumnValues::Uuid7(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![uuid1, uuid2]);
            assert_eq!(bitvec.to_vec(), vec![true, true]);
        }
    }

    #[test]
    fn test_undefined_uuid7() {
        let uuid1 = Uuid7::generate();
        let mut col = ColumnValues::uuid7(vec![uuid1]);
        col.push_value(Value::Undefined);
        if let ColumnValues::Uuid7(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![uuid1, Uuid7::from(Uuid::nil())]);
            assert_eq!(bitvec.to_vec(), vec![true, false]);
        }
    }

    #[test]
    fn test_push_value_to_undefined_uuid7() {
        let uuid = Uuid7::generate();
        let mut col = ColumnValues::Undefined(1);
        col.push_value(Value::Uuid7(uuid));
        if let ColumnValues::Uuid7(v, bitvec) = col {
            assert_eq!(v.to_vec(), vec![Uuid7::from(Uuid::nil()), uuid]);
            assert_eq!(bitvec.to_vec(), vec![false, true]);
        }
    }
}
