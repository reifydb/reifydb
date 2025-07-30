// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::column::data::EngineColumnData;
use crate::column::push::Push;
use reifydb_core::value::number::{SafeConvert, SafeDemote, SafePromote};

impl Push<i16> for EngineColumnData {
    fn push(&mut self, value: i16) {
        match self {
            EngineColumnData::Float4(container) => {
                match <i16 as SafeConvert<f32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Float8(container) => {
                match <i16 as SafeConvert<f64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Int1(container) => {
                match <i16 as SafeDemote<i8>>::checked_demote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Int2(container) => container.push(value),
            EngineColumnData::Int4(container) => {
                match <i16 as SafePromote<i32>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Int8(container) => {
                match <i16 as SafePromote<i64>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Int16(container) => {
                match <i16 as SafePromote<i128>>::checked_promote(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Uint1(container) => {
                match <i16 as SafeConvert<u8>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Uint2(container) => {
                match <i16 as SafeConvert<u16>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Uint4(container) => {
                match <i16 as SafeConvert<u32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Uint8(container) => {
                match <i16 as SafeConvert<u64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Uint16(container) => {
                match <i16 as SafeConvert<u128>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined(),
                }
            }
            EngineColumnData::Undefined(container) => {
                let mut new_container = EngineColumnData::int2(vec![0i16; container.len()]);
                if let EngineColumnData::Int2(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!(
                    "called `push::<i16>()` on incompatible EngineColumnData::{:?}",
                    other.get_type()
                );
            }
        }
    }
}
