// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::number::safe::convert::SafeConvert;

use crate::value::column::{data::ColumnData, push::Push};

impl Push<u8> for ColumnData {
	fn push(&mut self, value: u8) {
		match self {
			ColumnData::Float4(container) => match <u8 as SafeConvert<f32>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Float8(container) => match <u8 as SafeConvert<f64>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Uint1(container) => container.push(value),
			ColumnData::Uint2(container) => match <u8 as SafeConvert<u16>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Uint4(container) => match <u8 as SafeConvert<u32>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Uint8(container) => match <u8 as SafeConvert<u64>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Uint16(container) => match <u8 as SafeConvert<u128>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Int1(container) => match <u8 as SafeConvert<i8>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Int2(container) => match <u8 as SafeConvert<i16>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Int4(container) => match <u8 as SafeConvert<i32>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Int8(container) => match <u8 as SafeConvert<i64>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Int16(container) => match <u8 as SafeConvert<i128>>::checked_convert(value) {
				Some(v) => container.push(v),
				None => container.push_undefined(),
			},
			ColumnData::Undefined(container) => {
				let mut new_container = ColumnData::uint1(vec![0u8; container.len()]);
				if let ColumnData::Uint1(new_container) = &mut new_container {
					new_container.push(value);
				}
				*self = new_container;
			}
			other => {
				panic!(
					"called `push::<u8>()` on incompatible EngineColumnData::{:?}",
					other.get_type()
				);
			}
		}
	}
}
