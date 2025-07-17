// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Push};
use reifydb_core::CowVec;
use reifydb_core::value::number::{SafeConvert, SafeDemote, SafePromote};

impl Push<u32> for ColumnValues {
	fn push(&mut self, value: u32) {
		match self {
			ColumnValues::Float4(values, validity) => match value.checked_convert() {
				Some(v) => {
					values.push(v);
					validity.push(true);
				}
				None => {
					values.push(0.0);
					validity.push(false);
				}
			},
			ColumnValues::Float8(values, validity) => match value.checked_convert() {
				Some(v) => {
					values.push(v);
					validity.push(true);
				}
				None => {
					values.push(0.0);
					validity.push(false);
				}
			},
			ColumnValues::Uint1(values, validity) => match value.checked_demote() {
				Some(v) => {
					values.push(v);
					validity.push(true);
				}
				None => {
					values.push(0);
					validity.push(false);
				}
			},
			ColumnValues::Uint2(values, validity) => match value.checked_demote() {
				Some(v) => {
					values.push(v);
					validity.push(true);
				}
				None => {
					values.push(0);
					validity.push(false);
				}
			},
			ColumnValues::Uint4(values, validity) => {
				values.push(value);
				validity.push(true);
			}
			ColumnValues::Uint8(values, validity) => match value.checked_promote() {
				Some(v) => {
					values.push(v);
					validity.push(true);
				}
				None => {
					values.push(0);
					validity.push(false);
				}
			},
			ColumnValues::Uint16(values, validity) => match value.checked_promote() {
				Some(v) => {
					values.push(v);
					validity.push(true);
				}
				None => {
					values.push(0);
					validity.push(false);
				}
			},
			ColumnValues::Int4(values, validity) => match value.checked_convert() {
				Some(v) => {
					values.push(v);
					validity.push(true);
				}
				None => {
					values.push(0);
					validity.push(false);
				}
			},
			ColumnValues::Int8(values, validity) => match value.checked_convert() {
				Some(v) => {
					values.push(v);
					validity.push(true);
				}
				None => {
					values.push(0);
					validity.push(false);
				}
			},
			ColumnValues::Int16(values, validity) => match value.checked_convert() {
				Some(v) => {
					values.push(v);
					validity.push(true);
				}
				None => {
					values.push(0);
					validity.push(false);
				}
			},
			ColumnValues::Undefined(len) => {
				let mut values = vec![0u32; *len];
				let mut validity = vec![false; *len];
				values.push(value);
				validity.push(true);
				*self = ColumnValues::Uint4(CowVec::new(values), CowVec::new(validity));
			}
			other => {
				panic!("called `push::<u32>()` on incompatible ColumnValues::{:?}", other.ty());
			}
		}
	}
}
