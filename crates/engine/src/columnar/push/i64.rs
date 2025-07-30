// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::data::ColumnData;
use crate::columnar::push::Push;
use reifydb_core::value::number::{SafeConvert, SafeDemote, SafePromote};

impl Push<i64> for ColumnData {
    fn push(&mut self, value: i64) {
        match self {
            ColumnData::Float4(container) => {
                match <i64 as SafeConvert<f32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Float8(container) => {
                match <i64 as SafeConvert<f64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Int1(container) => {
                match <i64 as SafeDemote<i8>>::checked_demote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Int2(container) => {
                match <i64 as SafeDemote<i16>>::checked_demote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Int4(container) => {
                match <i64 as SafeDemote<i32>>::checked_demote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Int8(container) => container.push(value),
            ColumnData::Int16(container) => {
                match <i64 as SafePromote<i128>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Uint1(container) => {
                match <i64 as SafeConvert<u8>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Uint2(container) => {
                match <i64 as SafeConvert<u16>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Uint4(container) => {
                match <i64 as SafeConvert<u32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Uint8(container) => {
                match <i64 as SafeConvert<u64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Uint16(container) => {
                match <i64 as SafeConvert<u128>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Undefined(container) => {
                let mut new_container = ColumnData::int8(vec![0i64; container.len()]);
                if let ColumnData::Int8(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!("called `push::<i64>()` on incompatible EngineColumnData::{:?}", other.get_type());
            }
        }
    }
}
