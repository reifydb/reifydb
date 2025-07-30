// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::ColumnValues;
use crate::column::container::Push;
use reifydb_core::value::number::{SafeConvert, SafeDemote, SafePromote};

impl Push<i128> for ColumnValues {
    fn push(&mut self, value: i128) {
        match self {
            ColumnValues::Float4(container) => {
                match <i128 as SafeConvert<f32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Float8(container) => {
                match <i128 as SafeConvert<f64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int1(container) => {
                match <i128 as SafeDemote<i8>>::checked_demote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int2(container) => {
                match <i128 as SafeDemote<i16>>::checked_demote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int4(container) => {
                match <i128 as SafeDemote<i32>>::checked_demote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int8(container) => {
                match <i128 as SafeDemote<i64>>::checked_demote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int16(container) => container.push(value),
            ColumnValues::Uint1(container) => {
                match <i128 as SafeConvert<u8>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint2(container) => {
                match <i128 as SafeConvert<u16>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint4(container) => {
                match <i128 as SafeConvert<u32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint8(container) => {
                match <i128 as SafeConvert<u64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint16(container) => {
                match <i128 as SafeConvert<u128>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Undefined(container) => {
                let mut new_container = ColumnValues::int16(vec![0i128; container.len()]);
                if let ColumnValues::Int16(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!("called `push::<i128>()` on incompatible ColumnValues::{:?}", other.get_type());
            }
        }
    }
}
