// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::SafeConvert;

use crate::value::columnar::{data::ColumnData, push::Push};

impl Push<u64> for ColumnData {
	fn push(&mut self, value: u64) {
		match self {
            ColumnData::Float4(container) => {
                match <u64 as SafeConvert<f32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined()}
            }
            ColumnData::Float8(container) => {
                match <u64 as SafeConvert<f64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined()}
            }
            ColumnData::Uint1(container) => match <u64 as SafeConvert<u8>>::checked_convert(value) {
                Some(v) => container.push(v),
                None => container.push_undefined()},
            ColumnData::Uint2(container) => match <u64 as SafeConvert<u16>>::checked_convert(value) {
                Some(v) => container.push(v),
                None => container.push_undefined()},
            ColumnData::Uint4(container) => match <u64 as SafeConvert<u32>>::checked_convert(value) {
                Some(v) => container.push(v),
                None => container.push_undefined()},
            ColumnData::Uint8(container) => container.push(value),
            ColumnData::Uint16(container) => {
                match <u64 as SafeConvert<u128>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined()}
            }
            ColumnData::Int1(container) => match <u64 as SafeConvert<i8>>::checked_convert(value) {
                Some(v) => container.push(v),
                None => container.push_undefined()},
            ColumnData::Int2(container) => {
                match <u64 as SafeConvert<i16>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined()}
            }
            ColumnData::Int4(container) => {
                match <u64 as SafeConvert<i32>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined()}
            }
            ColumnData::Int8(container) => {
                match <u64 as SafeConvert<i64>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined()}
            }
            ColumnData::Int16(container) => {
                match <u64 as SafeConvert<i128>>::checked_convert(value) {
                    Some(v) => container.push(v),
                    None => container.push_undefined()}
            }
            ColumnData::Undefined(container) => {
                let mut new_container = ColumnData::uint8(vec![0u64; container.len()]);
                if let ColumnData::Uint8(new_container) = &mut new_container {
                    new_container.push(value);
                }
                *self = new_container;
            }
            other => {
                panic!(
                    "called `push::<u64>()` on incompatible EngineColumnData::{:?}",
                    other.get_type()
                );
            }
        }
	}
}
