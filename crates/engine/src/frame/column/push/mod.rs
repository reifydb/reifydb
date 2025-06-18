// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::ColumnValues;
use reifydb_core::CowVec;

mod undefined;
mod value;

pub trait Push<T> {
    fn push(&mut self, value: T);
}

impl ColumnValues {
    pub fn push<T>(&mut self, value: T)
    where
        Self: Push<T>,
    {
        <Self as Push<T>>::push(self, value)
    }
}

macro_rules! impl_push {
    ($t:ty, $variant:ident) => {
        impl Push<$t> for ColumnValues {
            fn push(&mut self, value: $t) {
                match self {
                    ColumnValues::$variant(values, validity) => {
                        values.push(value);
                        validity.push(true);
                    }
                    ColumnValues::Undefined(len) => {
                        let mut values = vec![Default::default(); *len];
                        let mut validity = vec![false; *len];
                        values.push(value);
                        validity.push(true);

                        *self = ColumnValues::$variant(CowVec::new(values), CowVec::new(validity));
                    }
                    other => panic!(
                        "called `push::<{}>()` on ColumnValues::{:?}",
                        stringify!($t),
                        other.value()
                    ),
                }
            }
        }
    };
}

impl_push!(bool, Bool);
impl_push!(f32, Float4);
impl_push!(f64, Float8);
impl_push!(i8, Int1);
impl_push!(i16, Int2);
impl_push!(i32, Int4);
impl_push!(i64, Int8);
impl_push!(i128, Int16);
impl_push!(u8, Uint1);
impl_push!(u16, Uint2);
impl_push!(u32, Uint4);
impl_push!(u64, Uint8);
impl_push!(u128, Uint16);

impl Push<String> for ColumnValues {
    fn push(&mut self, value: String) {
        match self {
            ColumnValues::String(values, validity) => {
                values.push(value);
                validity.push(true);
            }
            ColumnValues::Undefined(len) => {
                let mut values = vec![String::default(); *len];
                let mut validity = vec![false; *len];
                values.push(value);
                validity.push(true);

                *self = ColumnValues::String(CowVec::new(values), CowVec::new(validity));
            }
            other => panic!("called `push::<String>()` on ColumnValues::{:?}", other.value()),
        }
    }
}

#[cfg(test)]
mod tests {

    mod bool {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_bool_column() {
            let mut col = ColumnValues::bool(vec![true, false]);
            col.push(true);

            if let ColumnValues::Bool(values, validity) = col {
                assert_eq!(values.as_slice(), [true, false, true]);
                assert_eq!(validity.as_slice(), [true, true, true]);
            } else {
                panic!("Expected ColumnValues::Bool");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(false);

            if let ColumnValues::Bool(values, validity) = col {
                assert_eq!(values.as_slice(), [false, false, false]);
                assert_eq!(validity.as_slice(), [false, false, true]);
            } else {
                panic!("Expected ColumnValues::Bool after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<bool>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::int1(vec![1, 2]);
            col.push(true);
        }
    }

    mod float4 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_float4_column() {
            let mut col = ColumnValues::float4(vec![1.0, 2.0]);
            col.push(3.0f32);

            if let ColumnValues::Float4(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &3.0);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Float4");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(3.0f32);

            if let ColumnValues::Float4(values, validity) = col {
                assert_eq!(values.as_slice(), &[0.0, 0.0, 3.0]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Float4 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<f32>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(3.0f32);
        }
    }

    mod float8 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_float8_column() {
            let mut col = ColumnValues::float8(vec![1.0, 2.0]);
            col.push(3.0f64);

            if let ColumnValues::Float8(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &3.0);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Float8");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(3.0f64);

            if let ColumnValues::Float8(values, validity) = col {
                assert_eq!(values.as_slice(), &[0.0, 0.0, 3.0]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Float8 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<f64>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(3.0f64);
        }
    }

    mod int1 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_int1_column() {
            let mut col = ColumnValues::int1(vec![1, 2]);
            col.push(3i8);

            if let ColumnValues::Int1(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &3);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Int1");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(3i8);

            if let ColumnValues::Int1(values, validity) = col {
                assert_eq!(values.as_slice(), &[0, 0, 3]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Int1 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<i8>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(3i8);
        }
    }

    mod int2 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_int2_column() {
            let mut col = ColumnValues::int2(vec![10, 20]);
            col.push(30i16);

            if let ColumnValues::Int2(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &30);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Int2");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(30i16);

            if let ColumnValues::Int2(values, validity) = col {
                assert_eq!(values.as_slice(), &[0, 0, 30]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Int2 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<i16>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(30i16);
        }
    }

    mod int4 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_int4_column() {
            let mut col = ColumnValues::int4(vec![100, 200]);
            col.push(300i32);

            if let ColumnValues::Int4(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &300);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Int4");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(300i32);

            if let ColumnValues::Int4(values, validity) = col {
                assert_eq!(values.as_slice(), &[0, 0, 300]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Int4 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<i32>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(300i32);
        }
    }

    mod int8 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_int8_column() {
            let mut col = ColumnValues::int8(vec![1000, 2000]);
            col.push(3000i64);

            if let ColumnValues::Int8(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &3000);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Int8");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(3000i64);

            if let ColumnValues::Int8(values, validity) = col {
                assert_eq!(values.as_slice(), &[0, 0, 3000]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Int8 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<i64>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(3000i64);
        }
    }

    mod int16 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_int16_column() {
            let mut col = ColumnValues::int16(vec![10000, 20000]);
            col.push(30000i128);

            if let ColumnValues::Int16(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &30000);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Int16");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(30000i128);

            if let ColumnValues::Int16(values, validity) = col {
                assert_eq!(values.as_slice(), &[0, 0, 30000]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Int16 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<i128>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(30000i128);
        }
    }

    mod string {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_string_column() {
            let mut col = ColumnValues::string(vec!["reifydb".to_string(), "db".to_string()]);
            col.push("new_value".to_string());

            if let ColumnValues::String(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &"new_value".to_string());
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::String");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push("new_value".to_string());

            if let ColumnValues::String(values, validity) = col {
                assert_eq!(
                    values.as_slice(),
                    &["".to_string(), "".to_string(), "new_value".to_string()]
                );
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::String after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<String>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::int1(vec![1]);
            col.push("fail".to_string());
        }
    }

    mod uint1 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_uint1_column() {
            let mut col = ColumnValues::uint1(vec![1, 2]);
            col.push(3u8);

            if let ColumnValues::Uint1(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &3);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Uint1");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(3u8);

            if let ColumnValues::Uint1(values, validity) = col {
                assert_eq!(values.as_slice(), &[0, 0, 3]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Uint1 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<u8>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(3u8);
        }
    }

    mod uint2 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_uint2_column() {
            let mut col = ColumnValues::uint2(vec![10, 20]);
            col.push(30u16);

            if let ColumnValues::Uint2(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &30);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Uint2");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(30u16);

            if let ColumnValues::Uint2(values, validity) = col {
                assert_eq!(values.as_slice(), &[0, 0, 30]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Uint2 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<u16>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(30u16);
        }
    }

    mod uint4 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_uint4_column() {
            let mut col = ColumnValues::uint4(vec![100, 200]);
            col.push(300u32);

            if let ColumnValues::Uint4(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &300);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Uint4");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(300u32);

            if let ColumnValues::Uint4(values, validity) = col {
                assert_eq!(values.as_slice(), &[0, 0, 300]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Uint4 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<u32>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(300u32);
        }
    }

    mod uint8 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_uint8_column() {
            let mut col = ColumnValues::uint8(vec![1000, 2000]);
            col.push(3000u64);

            if let ColumnValues::Uint8(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &3000);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Uint8");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(3000u64);

            if let ColumnValues::Uint8(values, validity) = col {
                assert_eq!(values.as_slice(), &[0, 0, 3000]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Uint8 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<u64>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(3000u64);
        }
    }

    mod uint16 {
        use crate::frame::ColumnValues;

        #[test]
        fn test_into_uint16_column() {
            let mut col = ColumnValues::uint16(vec![10000, 20000]);
            col.push(30000u128);

            if let ColumnValues::Uint16(values, validity) = col {
                assert_eq!(values.as_slice().last().unwrap(), &30000);
                assert_eq!(validity.as_slice().last().unwrap(), &true);
            } else {
                panic!("Expected ColumnValues::Uint16");
            }
        }

        #[test]
        fn test_into_undefined_column() {
            let mut col = ColumnValues::Undefined(2);
            col.push(30000u128);

            if let ColumnValues::Uint16(values, validity) = col {
                assert_eq!(values.as_slice(), &[0, 0, 30000]);
                assert_eq!(validity.as_slice(), &[false, false, true]);
            } else {
                panic!("Expected ColumnValues::Uint16 after push into Undefined");
            }
        }

        #[test]
        #[should_panic(expected = "called `push::<u128>()` on ColumnValues")]
        fn test_into_wrong_column_type() {
            let mut col = ColumnValues::bool(vec![true]);
            col.push(30000u128);
        }
    }
}
