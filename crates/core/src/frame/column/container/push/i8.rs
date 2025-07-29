// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::ColumnValues;
use crate::frame::column::container::Push;
use crate::value::number::{SafeConvert, SafePromote};

impl Push<i8> for ColumnValues {
    fn push(&mut self, value: i8) {
        match self {
            ColumnValues::Float4(container) => {
                match <i8 as SafeConvert<f32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Float8(container) => {
                match <i8 as SafeConvert<f64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int1(container) => {
                container.push(value);
            }
            ColumnValues::Int2(container) => {
                match <i8 as SafePromote<i16>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int4(container) => {
                match <i8 as SafePromote<i32>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int8(container) => {
                match <i8 as SafePromote<i64>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int16(container) => {
                match <i8 as SafePromote<i128>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint1(container) => {
                match <i8 as SafeConvert<u8>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint2(container) => {
                match <i8 as SafeConvert<u16>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint4(container) => {
                match <i8 as SafeConvert<u32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint8(container) => {
                match <i8 as SafeConvert<u64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint16(container) => {
                match <i8 as SafeConvert<u128>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Undefined(container) => {
                let mut new_container = ColumnValues::int1(vec![0i8; container.len()]);
                if let ColumnValues::Int1(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!(
                    "called `push::<i8>()` on incompatible ColumnValues::{:?}",
                    other.get_type()
                );
            }
        }
    }
}
