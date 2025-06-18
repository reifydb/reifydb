// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::ColumnValues;
use reifydb_rql::expression::GreaterThanExpression;

impl Evaluator {
    pub(crate) fn greater_than(
        &mut self,
        gt: &GreaterThanExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<ColumnValues> {
        let left = self.evaluate(&gt.left, ctx)?;
        let right = self.evaluate(&gt.right, ctx)?;

        match (&left, &right) {
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                for i in 0..lv.len() {
                    if lv_valid[i] && rv_valid[i] {
                        values.push(lv[i] > rv[i] as i16);
                        valid.push(true);
                    } else {
                        values.push(false);
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::bool_with_validity(values, valid))
            }
            (ColumnValues::Int1(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                for i in 0..lv.len() {
                    if lv_valid[i] && rv_valid[i] {
                        values.push(lv[i] > rv[i]);
                        valid.push(true);
                    } else {
                        values.push(false);
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::bool_with_validity(values, valid))
            }
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int2(rv, rv_valid)) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                for i in 0..lv.len() {
                    if lv_valid[i] && rv_valid[i] {
                        values.push(lv[i] > rv[i]);
                        valid.push(true);
                    } else {
                        values.push(false);
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::bool_with_validity(values, valid))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int4(rv, rv_valid)) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                for i in 0..lv.len() {
                    if lv_valid[i] && rv_valid[i] {
                        values.push(lv[i] > rv[i]);
                        valid.push(true);
                    } else {
                        values.push(false);
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::bool_with_validity(values, valid))
            }
            _ => panic!("GT only supports Int2"),
        }
    }
}
