// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Frame};

impl Frame {
    pub fn get_bool(&self, name: &str, idx: usize) -> Option<bool> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Bool(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_float4(&self, name: &str, idx: usize) -> Option<f32> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Float4(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_float8(&self, name: &str, idx: usize) -> Option<f64> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Float8(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_int1(&self, name: &str, idx: usize) -> Option<i8> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Int1(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_int2(&self, name: &str, idx: usize) -> Option<i16> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Int2(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_int4(&self, name: &str, idx: usize) -> Option<i32> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Int4(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_int8(&self, name: &str, idx: usize) -> Option<i64> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Int8(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_int16(&self, name: &str, idx: usize) -> Option<i128> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Int16(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_uint1(&self, name: &str, idx: usize) -> Option<u8> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Uint1(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_uint2(&self, name: &str, idx: usize) -> Option<u16> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Uint2(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_uint4(&self, name: &str, idx: usize) -> Option<u32> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Uint4(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_uint8(&self, name: &str, idx: usize) -> Option<u64> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Uint8(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_uint16(&self, name: &str, idx: usize) -> Option<u128> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Uint16(v, b) => {
                if b.get(idx) {
                    Some(v[idx])
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_str(&self, name: &str, idx: usize) -> Option<&str> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Utf8(v, b) => {
                if b.get(idx) {
                    Some(v[idx].as_str())
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn is_defined(&self, name: &str, idx: usize) -> Option<bool> {
        match &self.columns[*self.index.get(name)?].values {
            ColumnValues::Undefined(len) => Some(*len > idx),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    mod bool {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::bool_with_bitvec([true], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_bool("col", 0), Some(true));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_bool("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::bool_with_bitvec([true], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_bool("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_bool("col", 0), None);
        }
    }
    mod float4 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::float4_with_bitvec([3.14], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_float4("col", 0), Some(3.14));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_float4("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::float4_with_bitvec([3.14], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_float4("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_float4("col", 0), None);
        }
    }
    mod float8 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::float8_with_bitvec([2.718], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_float8("col", 0), Some(2.718));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_float8("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::float8_with_bitvec([2.718], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_float8("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_float8("col", 0), None);
        }
    }

    mod int1 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int1_with_bitvec([1], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_int1("col", 0), Some(1));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_int1("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int1_with_bitvec([1], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_int1("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_int1("col", 0), None);
        }
    }
    mod int2 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int2_with_bitvec([2], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_int2("col", 0), Some(2));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_int2("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int2_with_bitvec([2], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_int2("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_int2("col", 0), None);
        }
    }
    mod int4 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([42], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_int4("col", 0), Some(42));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_int4("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([42], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_int4("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::float4_with_bitvec([3.14], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_int4("col", 0), None);
        }
    }
    mod int8 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int8_with_bitvec([8], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_int8("col", 0), Some(8));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_int8("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int8_with_bitvec([8], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_int8("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_int8("col", 0), None);
        }
    }
    mod int16 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int16_with_bitvec([16], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_int16("col", 0), Some(16));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_int16("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int16_with_bitvec([16], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_int16("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_int16("col", 0), None);
        }
    }

    mod uint1 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::uint1_with_bitvec([1], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_uint1("col", 0), Some(1));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_uint1("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::uint1_with_bitvec([1], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_uint1("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_uint1("col", 0), None);
        }
    }
    mod uint2 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::uint2_with_bitvec([2], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_uint2("col", 0), Some(2));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_uint2("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::uint2_with_bitvec([2], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_uint2("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_uint2("col", 0), None);
        }
    }
    mod uint4 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::uint4_with_bitvec([4], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_uint4("col", 0), Some(4));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_uint4("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::uint4_with_bitvec([4], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_uint4("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_uint4("col", 0), None);
        }
    }
    mod uint8 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::uint8_with_bitvec([8], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_uint8("col", 0), Some(8));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_uint8("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::uint8_with_bitvec([8], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_uint8("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_uint8("col", 0), None);
        }
    }
    mod uint16 {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::uint16_with_bitvec([16], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_uint16("col", 0), Some(16));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_uint16("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::uint16_with_bitvec([16], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_uint16("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([123], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_uint16("col", 0), None);
        }
    }

    mod string {
        use crate::frame::{ColumnValues, Frame, FrameColumn};
        use std::collections::HashMap;

        #[test]
        fn test_happy_path() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::utf8_with_bitvec(["hello".to_string()], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_str("col", 0), Some("hello"));
        }

        #[test]
        fn test_value_not_found() {
            let frame = Frame { name: "frame".to_string(), columns: vec![], index: HashMap::new() };
            assert_eq!(frame.get_str("col", 0), None);
        }

        #[test]
        fn test_value_invalid() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::utf8_with_bitvec(["hello".to_string()], [false]),
                }],
                index,
            };
            assert_eq!(frame.get_str("col", 0), None);
        }

        #[test]
        fn test_wrong_type() {
            let mut index = HashMap::new();
            index.insert("col".into(), 0);
            let frame = Frame {
                name: "frame".to_string(),
                columns: vec![FrameColumn {
                    name: "col".into(),
                    values: ColumnValues::int4_with_bitvec([1], [true]),
                }],
                index,
            };
            assert_eq!(frame.get_str("col", 0), None);
        }
    }
}
