// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::ColumnValues;
use crate::column::container::Push;
use reifydb_core::value::number::{SafeConvert, SafeDemote, SafePromote};

impl Push<u16> for ColumnValues {
    fn push(&mut self, value: u16) {
        match self {
            ColumnValues::Float4(container) => {
                match <u16 as SafeConvert<f32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Float8(container) => {
                match <u16 as SafeConvert<f64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint1(container) => {
                match <u16 as SafeDemote<u8>>::checked_demote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint2(container) => container.push(value),
            ColumnValues::Uint4(container) => {
                match <u16 as SafePromote<u32>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint8(container) => {
                match <u16 as SafePromote<u64>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Uint16(container) => {
                match <u16 as SafePromote<u128>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int1(container) => {
                match <u16 as SafeConvert<i8>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int2(container) => {
                match <u16 as SafeConvert<i16>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int4(container) => {
                match <u16 as SafeConvert<i32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int8(container) => {
                match <u16 as SafeConvert<i64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Int16(container) => {
                match <u16 as SafeConvert<i128>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnValues::Undefined(container) => {
                let mut new_container = ColumnValues::uint2(vec![0u16; container.len()]);
                if let ColumnValues::Uint2(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!("called `push::<u16>()` on incompatible ColumnValues::{:?}", other.get_type());
            }
        }
    }
}
