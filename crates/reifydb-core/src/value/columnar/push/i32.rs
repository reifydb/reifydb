// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::{SafeConvert, SafeDemote, SafePromote};
use crate::value::{
	columnar::{data::ColumnData, push::Push},
};

impl Push<i32> for ColumnData {
	fn push(&mut self, value: i32) {
		match self {
            ColumnData::Float4(container) => {
                match <i32 as SafeConvert<f32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Float8(container) => {
                match <i32 as SafeConvert<f64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Int1(container) => match <i32 as SafeDemote<i8>>::checked_demote(value) {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnData::Int2(container) => match <i32 as SafeDemote<i16>>::checked_demote(value) {
                Some(v) => container.push(v),
                None => container.push_undefined(),
            },
            ColumnData::Int4(container) => container.push(value),
            ColumnData::Int8(container) => {
                match <i32 as SafePromote<i64>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Int16(container) => {
                match <i32 as SafePromote<i128>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Uint1(container) => {
                match <i32 as SafeConvert<u8>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Uint2(container) => {
                match <i32 as SafeConvert<u16>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Uint4(container) => {
                match <i32 as SafeConvert<u32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Uint8(container) => {
                match <i32 as SafeConvert<u64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Uint16(container) => {
                match <i32 as SafeConvert<u128>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            ColumnData::Undefined(container) => {
                let mut new_container = ColumnData::int4(vec![0i32; container.len()]);
                if let ColumnData::Int4(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!(
                    "called `push::<i32>()` on incompatible EngineColumnData::{:?}",
                    other.get_type()
                );
            }
        }
	}
}
