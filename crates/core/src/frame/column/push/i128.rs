// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, Push};
use crate::value::number::{SafeConvert, SafeDemote};
use crate::{BitVec, CowVec};

impl Push<i128> for ColumnValues {
    fn push(&mut self, value: i128) {
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
            ColumnValues::Int4(values, bitvec) => match value.checked_demote() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int8(values, bitvec) => match value.checked_demote() {
                Some(v) => {
                    values.push(v);
                    bitvec.push(true);
                }
                None => {
                    values.push(0);
                    bitvec.push(false);
                }
            },
            ColumnValues::Int16(values, bitvec) => {
                values.push(value);
                bitvec.push(true);
            }
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
                let mut values = vec![0i128; *len];
                let mut bitvec = BitVec::new(*len, false);
                values.push(value);
                bitvec.push(true);
                *self = ColumnValues::Int16(CowVec::new(values), bitvec);
            }
            other => {
                panic!("called `push::<i128>()` on incompatible ColumnValues::{:?}", other.get_type());
            }
        }
    }
}
