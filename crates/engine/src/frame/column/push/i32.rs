// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Push};
use reifydb_core::value::number::{SafeConvert, SafeDemote, SafePromote};
use reifydb_core::{BitVec, CowVec};

impl Push<i32> for ColumnValues {
    fn push(&mut self, value: i32) {
        match self {
            ColumnValues::Float4(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0.0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Float8(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0.0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int1(values, bitvec) => match value.checked_demote() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int2(values, bitvec) => match value.checked_demote() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int4(values, bitvec) => {
                values.push(value);
                bitvec.push(true);
            }
            ColumnValues::Int8(values, bitvec) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int16(values, bitvec) => match value.checked_promote() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Uint4(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Uint8(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Uint16(values, bitvec) => match value.checked_convert() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Undefined(len) => {
                let mut values = vec![0i32; *len];
                let mut bitvec = BitVec::new(*len, false);
                values.push(value);
                bitvec.push(true);
                *self = ColumnValues::Int4(CowVec::new(values), bitvec);
            }
            other => {
                panic!("called `push::<i32>()` on incompatible ColumnValues::{:?}", other.get_type());
            }
        }
    }
}
