// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::frame::{ColumnValues, FrameColumn};
use crate::{BitVec, CowVec};

impl FrameColumn {
    pub fn filter(&mut self, mask: &BitVec) -> crate::Result<()> {
        self.values_mut().filter(mask)
    }
}

impl ColumnValues {
    pub fn filter(&mut self, mask: &BitVec) -> crate::Result<()> {
        match self {
            ColumnValues::Bool(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i].clone());
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Float4(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Float8(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Int1(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Int2(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Int4(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Int8(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Int16(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Uint1(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Uint2(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Uint4(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Uint8(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Uint16(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Utf8(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i].clone());
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Date(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i].clone());
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::DateTime(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i].clone());
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Time(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i].clone());
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Interval(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i].clone());
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Undefined(len) => {
                *len = mask.count_ones();
            }

            ColumnValues::RowId(values, bitvec) => {
                let mut new_values = Vec::new();
                let mut new_valid = Vec::new();
                for (i, _) in values.iter().enumerate() {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }
                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Uuid4(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Uuid7(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }

            ColumnValues::Blob(values, bitvec) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i].clone());
                        new_valid.push(bitvec.get(i));
                    }
                }

                *values = CowVec::new(new_values);
                *bitvec = new_valid.into();
            }
        }

        Ok(())
    }
}
