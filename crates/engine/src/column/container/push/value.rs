// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::EngineColumnData;
use reifydb_core::Value;

impl EngineColumnData {
    pub fn push_value(&mut self, value: Value) {
        match value {
            Value::Bool(v) => match self {
                EngineColumnData::Bool(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::bool(vec![]);
                    if let EngineColumnData::Bool(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Float4(v) => match self {
                EngineColumnData::Float4(_) => self.push(v.value()),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::float4(vec![]);
                    if let EngineColumnData::Float4(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v.value());
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Float8(v) => match self {
                EngineColumnData::Float8(_) => self.push(v.value()),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::float8(vec![]);
                    if let EngineColumnData::Float8(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v.value());
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Int1(v) => match self {
                EngineColumnData::Int1(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::int1(vec![]);
                    if let EngineColumnData::Int1(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Int2(v) => match self {
                EngineColumnData::Int2(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::int2(vec![]);
                    if let EngineColumnData::Int2(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Int4(v) => match self {
                EngineColumnData::Int4(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::int4(vec![]);
                    if let EngineColumnData::Int4(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Int8(v) => match self {
                EngineColumnData::Int8(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::int8(vec![]);
                    if let EngineColumnData::Int8(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Int16(v) => match self {
                EngineColumnData::Int16(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::int16(vec![]);
                    if let EngineColumnData::Int16(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Utf8(v) => match self {
                EngineColumnData::Utf8(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::utf8(Vec::<String>::new());
                    if let EngineColumnData::Utf8(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Uint1(v) => match self {
                EngineColumnData::Uint1(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::uint1(vec![]);
                    if let EngineColumnData::Uint1(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Uint2(v) => match self {
                EngineColumnData::Uint2(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::uint2(vec![]);
                    if let EngineColumnData::Uint2(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Uint4(v) => match self {
                EngineColumnData::Uint4(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::uint4(vec![]);
                    if let EngineColumnData::Uint4(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Uint8(v) => match self {
                EngineColumnData::Uint8(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::uint8(vec![]);
                    if let EngineColumnData::Uint8(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Uint16(v) => match self {
                EngineColumnData::Uint16(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::uint16(vec![]);
                    if let EngineColumnData::Uint16(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Date(v) => match self {
                EngineColumnData::Date(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::date(vec![]);
                    if let EngineColumnData::Date(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::DateTime(v) => match self {
                EngineColumnData::DateTime(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::datetime(vec![]);
                    if let EngineColumnData::DateTime(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Time(v) => match self {
                EngineColumnData::Time(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::time(vec![]);
                    if let EngineColumnData::Time(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Interval(v) => match self {
                EngineColumnData::Interval(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::interval(vec![]);
                    if let EngineColumnData::Interval(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Uuid4(v) => match self {
                EngineColumnData::Uuid4(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::uuid4(vec![]);
                    if let EngineColumnData::Uuid4(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Uuid7(v) => match self {
                EngineColumnData::Uuid7(_) => self.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::uuid7(vec![]);
                    if let EngineColumnData::Uuid7(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },

            Value::Undefined => self.push_undefined(),
            Value::RowId(row_id) => match self {
                EngineColumnData::RowId(container) => container.push(row_id),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::row_id(vec![]);
                    if let EngineColumnData::RowId(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(row_id);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },
            Value::Blob(v) => match self {
                EngineColumnData::Blob(container) => container.push(v),
                EngineColumnData::Undefined(container) => {
                    let mut new_container = EngineColumnData::blob(vec![]);
                    if let EngineColumnData::Blob(new_container) = &mut new_container {
                        for _ in 0..container.len() {
                            new_container.push_undefined();
                        }
                        new_container.push(v);
                    }
                    *self = new_container;
                }
                _ => unimplemented!(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::column::EngineColumnData;
    use reifydb_core::value::uuid::{Uuid4, Uuid7};
    use reifydb_core::{Date, DateTime, Interval, OrderedF32, OrderedF64, RowId, Time, Value};
    use uuid::Uuid;

    #[test]
    fn test_bool() {
        let mut col = EngineColumnData::bool(vec![true]);
        col.push_value(Value::Bool(false));
        let EngineColumnData::Bool(container) = col else {
            panic!("Expected Bool");
        };
        assert_eq!(container.values().to_vec(), vec![true, false]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_bool() {
        let mut col = EngineColumnData::bool(vec![true]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Bool(container) = col else {
            panic!("Expected Bool");
        };
        assert_eq!(container.values().to_vec(), vec![true, false]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_bool() {
        let mut col = EngineColumnData::undefined(2);
        col.push_value(Value::Bool(true));
        let EngineColumnData::Bool(container) = col else {
            panic!("Expected Bool");
        };
        assert_eq!(container.values().to_vec(), vec![false, false, true]);
        assert_eq!(container.bitvec().to_vec(), vec![false, false, true]);
    }

    #[test]
    fn test_float4() {
        let mut col = EngineColumnData::float4(vec![1.0]);
        col.push_value(Value::Float4(OrderedF32::try_from(2.0).unwrap()));
        let EngineColumnData::Float4(container) = col else {
            panic!("Expected Float4");
        };
        assert_eq!(container.values().as_slice(), &[1.0, 2.0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_float4() {
        let mut col = EngineColumnData::float4(vec![1.0]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Float4(container) = col else {
            panic!("Expected Float4");
        };
        assert_eq!(container.values().as_slice(), &[1.0, 0.0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_float4() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Float4(OrderedF32::try_from(3.14).unwrap()));
        let EngineColumnData::Float4(container) = col else {
            panic!("Expected Float4");
        };
        assert_eq!(container.values().as_slice(), &[0.0, 3.14]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_float8() {
        let mut col = EngineColumnData::float8(vec![1.0]);
        col.push_value(Value::Float8(OrderedF64::try_from(2.0).unwrap()));
        let EngineColumnData::Float8(container) = col else {
            panic!("Expected Float8");
        };
        assert_eq!(container.values().as_slice(), &[1.0, 2.0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_float8() {
        let mut col = EngineColumnData::float8(vec![1.0]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Float8(container) = col else {
            panic!("Expected Float8");
        };
        assert_eq!(container.values().as_slice(), &[1.0, 0.0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_float8() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Float8(OrderedF64::try_from(2.718).unwrap()));
        let EngineColumnData::Float8(container) = col else {
            panic!("Expected Float8");
        };
        assert_eq!(container.values().as_slice(), &[0.0, 2.718]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_int1() {
        let mut col = EngineColumnData::int1(vec![1]);
        col.push_value(Value::Int1(2));
        let EngineColumnData::Int1(container) = col else {
            panic!("Expected Int1");
        };
        assert_eq!(container.values().as_slice(), &[1, 2]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_int1() {
        let mut col = EngineColumnData::int1(vec![1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Int1(container) = col else {
            panic!("Expected Int1");
        };
        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_int1() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Int1(5));
        let EngineColumnData::Int1(container) = col else {
            panic!("Expected Int1");
        };
        assert_eq!(container.values().as_slice(), &[0, 5]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_int2() {
        let mut col = EngineColumnData::int2(vec![1]);
        col.push_value(Value::Int2(3));
        let EngineColumnData::Int2(container) = col else {
            panic!("Expected Int2");
        };
        assert_eq!(container.values().as_slice(), &[1, 3]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_int2() {
        let mut col = EngineColumnData::int2(vec![1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Int2(container) = col else {
            panic!("Expected Int2");
        };
        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_int2() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Int2(10));
        let EngineColumnData::Int2(container) = col else {
            panic!("Expected Int2");
        };
        assert_eq!(container.values().as_slice(), &[0, 10]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_int4() {
        let mut col = EngineColumnData::int4(vec![10]);
        col.push_value(Value::Int4(20));
        let EngineColumnData::Int4(container) = col else {
            panic!("Expected Int4");
        };
        assert_eq!(container.values().as_slice(), &[10, 20]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_int4() {
        let mut col = EngineColumnData::int4(vec![10]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Int4(container) = col else {
            panic!("Expected Int4");
        };
        assert_eq!(container.values().as_slice(), &[10, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_int4() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Int4(20));
        let EngineColumnData::Int4(container) = col else {
            panic!("Expected Int4");
        };
        assert_eq!(container.values().as_slice(), &[0, 20]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_int8() {
        let mut col = EngineColumnData::int8(vec![100]);
        col.push_value(Value::Int8(200));
        let EngineColumnData::Int8(container) = col else {
            panic!("Expected Int8");
        };
        assert_eq!(container.values().as_slice(), &[100, 200]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_int8() {
        let mut col = EngineColumnData::int8(vec![100]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Int8(container) = col else {
            panic!("Expected Int8");
        };
        assert_eq!(container.values().as_slice(), &[100, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_int8() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Int8(30));
        let EngineColumnData::Int8(container) = col else {
            panic!("Expected Int8");
        };
        assert_eq!(container.values().as_slice(), &[0, 30]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_int16() {
        let mut col = EngineColumnData::int16(vec![1000]);
        col.push_value(Value::Int16(2000));
        let EngineColumnData::Int16(container) = col else {
            panic!("Expected Int16");
        };
        assert_eq!(container.values().as_slice(), &[1000, 2000]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_int16() {
        let mut col = EngineColumnData::int16(vec![1000]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Int16(container) = col else {
            panic!("Expected Int16");
        };
        assert_eq!(container.values().as_slice(), &[1000, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_int16() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Int16(40));
        let EngineColumnData::Int16(container) = col else {
            panic!("Expected Int16");
        };
        assert_eq!(container.values().as_slice(), &[0, 40]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_uint1() {
        let mut col = EngineColumnData::uint1(vec![1]);
        col.push_value(Value::Uint1(2));
        let EngineColumnData::Uint1(container) = col else {
            panic!("Expected Uint1");
        };
        assert_eq!(container.values().as_slice(), &[1, 2]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_uint1() {
        let mut col = EngineColumnData::uint1(vec![1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Uint1(container) = col else {
            panic!("Expected Uint1");
        };
        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_uint1() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Uint1(1));
        let EngineColumnData::Uint1(container) = col else {
            panic!("Expected Uint1");
        };
        assert_eq!(container.values().as_slice(), &[0, 1]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_uint2() {
        let mut col = EngineColumnData::uint2(vec![10]);
        col.push_value(Value::Uint2(20));
        let EngineColumnData::Uint2(container) = col else {
            panic!("Expected Uint2");
        };
        assert_eq!(container.values().as_slice(), &[10, 20]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_uint2() {
        let mut col = EngineColumnData::uint2(vec![10]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Uint2(container) = col else {
            panic!("Expected Uint2");
        };
        assert_eq!(container.values().as_slice(), &[10, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_uint2() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Uint2(2));
        let EngineColumnData::Uint2(container) = col else {
            panic!("Expected Uint2");
        };
        assert_eq!(container.values().as_slice(), &[0, 2]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_uint4() {
        let mut col = EngineColumnData::uint4(vec![100]);
        col.push_value(Value::Uint4(200));
        let EngineColumnData::Uint4(container) = col else {
            panic!("Expected Uint4");
        };
        assert_eq!(container.values().as_slice(), &[100, 200]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_uint4() {
        let mut col = EngineColumnData::uint4(vec![100]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Uint4(container) = col else {
            panic!("Expected Uint4");
        };
        assert_eq!(container.values().as_slice(), &[100, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_uint4() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Uint4(3));
        let EngineColumnData::Uint4(container) = col else {
            panic!("Expected Uint4");
        };
        assert_eq!(container.values().as_slice(), &[0, 3]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_uint8() {
        let mut col = EngineColumnData::uint8(vec![1000]);
        col.push_value(Value::Uint8(2000));
        let EngineColumnData::Uint8(container) = col else {
            panic!("Expected Uint8");
        };
        assert_eq!(container.values().as_slice(), &[1000, 2000]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_uint8() {
        let mut col = EngineColumnData::uint8(vec![1000]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Uint8(container) = col else {
            panic!("Expected Uint8");
        };
        assert_eq!(container.values().as_slice(), &[1000, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_uint8() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Uint8(4));
        let EngineColumnData::Uint8(container) = col else {
            panic!("Expected Uint8");
        };
        assert_eq!(container.values().as_slice(), &[0, 4]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_uint16() {
        let mut col = EngineColumnData::uint16(vec![10000]);
        col.push_value(Value::Uint16(20000));
        let EngineColumnData::Uint16(container) = col else {
            panic!("Expected Uint16");
        };
        assert_eq!(container.values().as_slice(), &[10000, 20000]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_uint16() {
        let mut col = EngineColumnData::uint16(vec![10000]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Uint16(container) = col else {
            panic!("Expected Uint16");
        };
        assert_eq!(container.values().as_slice(), &[10000, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_uint16() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Uint16(5));
        let EngineColumnData::Uint16(container) = col else {
            panic!("Expected Uint16");
        };
        assert_eq!(container.values().as_slice(), &[0, 5]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_utf8() {
        let mut col = EngineColumnData::utf8(vec!["hello".to_string()]);
        col.push_value(Value::Utf8("world".to_string()));
        let EngineColumnData::Utf8(container) = col else {
            panic!("Expected Utf8");
        };
        assert_eq!(container.values().as_slice(), &["hello".to_string(), "world".to_string()]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_utf8() {
        let mut col = EngineColumnData::utf8(vec!["hello".to_string()]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Utf8(container) = col else {
            panic!("Expected Utf8");
        };
        assert_eq!(container.values().as_slice(), &["hello".to_string(), "".to_string()]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_utf8() {
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Utf8("ok".to_string()));
        let EngineColumnData::Utf8(container) = col else {
            panic!("Expected Utf8");
        };
        assert_eq!(container.values().as_slice(), &["".to_string(), "ok".to_string()]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_undefined() {
        let mut col = EngineColumnData::int2(vec![1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Int2(container) = col else {
            panic!("Expected Int2");
        };
        assert_eq!(container.values().as_slice(), &[1, 0]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_date() {
        let date1 = Date::from_ymd(2023, 1, 1).unwrap();
        let date2 = Date::from_ymd(2023, 12, 31).unwrap();
        let mut col = EngineColumnData::date(vec![date1]);
        col.push_value(Value::Date(date2));
        let EngineColumnData::Date(container) = col else {
            panic!("Expected Date");
        };
        assert_eq!(container.values().as_slice(), &[date1, date2]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_date() {
        use Date;
        let date1 = Date::from_ymd(2023, 1, 1).unwrap();
        let mut col = EngineColumnData::date(vec![date1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Date(container) = col else {
            panic!("Expected Date");
        };
        assert_eq!(container.values().as_slice(), &[date1, Date::default()]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_date() {
        use Date;
        let date = Date::from_ymd(2023, 6, 15).unwrap();
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Date(date));
        let EngineColumnData::Date(container) = col else {
            panic!("Expected Date");
        };
        assert_eq!(container.values().as_slice(), &[Date::default(), date]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_datetime() {
        let dt1 = DateTime::from_timestamp(1672531200).unwrap(); // 2023-01-01 00:00:00 UTC
        let dt2 = DateTime::from_timestamp(1704067200).unwrap(); // 2024-01-01 00:00:00 UTC
        let mut col = EngineColumnData::datetime(vec![dt1]);
        col.push_value(Value::DateTime(dt2));
        let EngineColumnData::DateTime(container) = col else {
            panic!("Expected DateTime");
        };
        assert_eq!(container.values().as_slice(), &[dt1, dt2]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_datetime() {
        use DateTime;
        let dt1 = DateTime::from_timestamp(1672531200).unwrap();
        let mut col = EngineColumnData::datetime(vec![dt1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::DateTime(container) = col else {
            panic!("Expected DateTime");
        };
        assert_eq!(container.values().as_slice(), &[dt1, DateTime::default()]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_datetime() {
        use DateTime;
        let dt = DateTime::from_timestamp(1672531200).unwrap();
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::DateTime(dt));
        let EngineColumnData::DateTime(container) = col else {
            panic!("Expected DateTime");
        };
        assert_eq!(container.values().as_slice(), &[DateTime::default(), dt]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_time() {
        let time1 = Time::from_hms(12, 30, 0).unwrap();
        let time2 = Time::from_hms(18, 45, 30).unwrap();
        let mut col = EngineColumnData::time(vec![time1]);
        col.push_value(Value::Time(time2));
        let EngineColumnData::Time(container) = col else {
            panic!("Expected Time");
        };
        assert_eq!(container.values().as_slice(), &[time1, time2]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_time() {
        use Time;
        let time1 = Time::from_hms(12, 30, 0).unwrap();
        let mut col = EngineColumnData::time(vec![time1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Time(container) = col else {
            panic!("Expected Time");
        };
        assert_eq!(container.values().as_slice(), &[time1, Time::default()]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_time() {
        use Time;
        let time = Time::from_hms(15, 20, 10).unwrap();
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Time(time));
        let EngineColumnData::Time(container) = col else {
            panic!("Expected Time");
        };
        assert_eq!(container.values().as_slice(), &[Time::default(), time]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_interval() {
        let interval1 = Interval::from_days(30);
        let interval2 = Interval::from_hours(24);
        let mut col = EngineColumnData::interval(vec![interval1]);
        col.push_value(Value::Interval(interval2));
        let EngineColumnData::Interval(container) = col else {
            panic!("Expected Interval");
        };
        assert_eq!(container.values().as_slice(), &[interval1, interval2]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_interval() {
        let interval1 = Interval::from_days(30);
        let mut col = EngineColumnData::interval(vec![interval1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Interval(container) = col else {
            panic!("Expected Interval");
        };
        assert_eq!(container.values().as_slice(), &[interval1, Interval::default()]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_interval() {
        let interval = Interval::from_minutes(90);
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Interval(interval));
        let EngineColumnData::Interval(container) = col else {
            panic!("Expected Interval");
        };
        assert_eq!(container.values().as_slice(), &[Interval::default(), interval]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_row_id() {
        let row_id1 = RowId::new(1);
        let row_id2 = RowId::new(2);
        let mut col = EngineColumnData::row_id(vec![row_id1]);
        col.push_value(Value::RowId(row_id2));
        let EngineColumnData::RowId(container) = col else {
            panic!("Expected RowId");
        };
        assert_eq!(container.values().as_slice(), &[row_id1, row_id2]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_row_id() {
        let row_id1 = RowId::new(1);
        let mut col = EngineColumnData::row_id(vec![row_id1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::RowId(container) = col else {
            panic!("Expected RowId");
        };
        assert_eq!(container.values().as_slice(), &[row_id1, RowId::default()]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_row_id() {
        let row_id = RowId::new(42);
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::RowId(row_id));
        let EngineColumnData::RowId(container) = col else {
            panic!("Expected RowId");
        };
        assert_eq!(container.values().as_slice(), &[RowId::default(), row_id]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_uuid4() {
        let uuid1 = Uuid4::generate();
        let uuid2 = Uuid4::generate();
        let mut col = EngineColumnData::uuid4(vec![uuid1]);
        col.push_value(Value::Uuid4(uuid2));
        let EngineColumnData::Uuid4(container) = col else {
            panic!("Expected Uuid4");
        };
        assert_eq!(container.values().as_slice(), &[uuid1, uuid2]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_uuid4() {
        let uuid1 = Uuid4::generate();
        let mut col = EngineColumnData::uuid4(vec![uuid1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Uuid4(container) = col else {
            panic!("Expected Uuid4");
        };
        assert_eq!(container.values().as_slice(), &[uuid1, Uuid4::from(Uuid::nil())]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_uuid4() {
        let uuid = Uuid4::generate();
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Uuid4(uuid));
        let EngineColumnData::Uuid4(container) = col else {
            panic!("Expected Uuid4");
        };
        assert_eq!(container.values().as_slice(), &[Uuid4::from(Uuid::nil()), uuid]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }

    #[test]
    fn test_uuid7() {
        let uuid1 = Uuid7::generate();
        let uuid2 = Uuid7::generate();
        let mut col = EngineColumnData::uuid7(vec![uuid1]);
        col.push_value(Value::Uuid7(uuid2));
        let EngineColumnData::Uuid7(container) = col else {
            panic!("Expected Uuid7");
        };
        assert_eq!(container.values().as_slice(), &[uuid1, uuid2]);
        assert_eq!(container.bitvec().to_vec(), vec![true, true]);
    }

    #[test]
    fn test_undefined_uuid7() {
        let uuid1 = Uuid7::generate();
        let mut col = EngineColumnData::uuid7(vec![uuid1]);
        col.push_value(Value::Undefined);
        let EngineColumnData::Uuid7(container) = col else {
            panic!("Expected Uuid7");
        };
        assert_eq!(container.values().as_slice(), &[uuid1, Uuid7::from(Uuid::nil())]);
        assert_eq!(container.bitvec().to_vec(), vec![true, false]);
    }

    #[test]
    fn test_push_value_to_undefined_uuid7() {
        let uuid = Uuid7::generate();
        let mut col = EngineColumnData::undefined(1);
        col.push_value(Value::Uuid7(uuid));
        let EngineColumnData::Uuid7(container) = col else {
            panic!("Expected Uuid7");
        };
        assert_eq!(container.values().as_slice(), &[Uuid7::from(Uuid::nil()), uuid]);
        assert_eq!(container.bitvec().to_vec(), vec![false, true]);
    }
}
