// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues};
use reifydb_core::{BitVec, CowVec};

impl Column {
    pub fn filter(&mut self, mask: &BitVec) -> crate::frame::Result<()> {
        self.data.filter(mask)
    }
}

impl ColumnValues {
    pub fn filter(&mut self, mask: &BitVec) -> crate::frame::Result<()> {
        match self {
            ColumnValues::Bool(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i].clone());
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Float4(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Float8(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Int1(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Int2(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Int4(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Int8(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Int16(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Uint1(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Uint2(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Uint4(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Uint8(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Uint16(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i]);
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::String(values, valid) => {
                let mut new_values = Vec::with_capacity(mask.count_ones());
                let mut new_valid = Vec::with_capacity(mask.count_ones());

                for i in 0..values.len().min(mask.len()) {
                    if mask.get(i) {
                        new_values.push(values[i].clone());
                        new_valid.push(valid[i]);
                    }
                }

                *values = CowVec::new(new_values);
                *valid = CowVec::new(new_valid);
            }

            ColumnValues::Undefined(len) => {
                *len = mask.count_ones();
            }
        }

        Ok(())
    }
}
