// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Push};
use reifydb_core::value::number::{SafeConvert, SafeDemote, SafePromote};
use reifydb_core::{BitVec, CowVec};

impl Push<i64> for ColumnValues {
    fn push(&mut self, value: i64) {
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
            ColumnValues::Int1(values, validity) => match value.checked_demote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int2(values, validity) => match value.checked_demote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int4(values, validity) => match value.checked_demote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Int8(values, validity) => {
                values.push(value);
                validity.push(true);
            }
            ColumnValues::Int16(values, validity) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Uint8(values, validity) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    validity.push(true);
                }
                None => {
                    values.push(0);
                    validity.push(false);
                }
            },
            ColumnValues::Uint16(values, validity) => match value.checked_convert() {
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
                let mut values = vec![0i64; *len];
                let mut validity = BitVec::new(*len, false);
                values.push(value);
                validity.push(true);
                *self = ColumnValues::Int8(CowVec::new(values), validity);
            }
            other => {
                panic!("called `push::<i64>()` on incompatible ColumnValues::{:?}", other.ty());
            }
        }
    }
}
