// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, Evaluator};
use crate::frame::ColumnValues;
use reifydb_rql::expression::LessThanExpression;

macro_rules! compare {
    ($lv:expr, $rv:expr, $lv_valid:expr, $rv_valid:expr, $cast:expr) => {{
        let mut values = Vec::with_capacity($lv.len());
        let mut valid = Vec::with_capacity($lv.len());
        for i in 0..$lv.len() {
            if $lv_valid[i] && $rv_valid[i] {
                values.push($cast($lv[i]) < $cast($rv[i]));
                valid.push(true);
            } else {
                values.push(false);
                valid.push(false);
            }
        }
        Ok(ColumnValues::bool_with_validity(values, valid))
    }};
}


impl Evaluator {
    pub(crate) fn less_than(
        &mut self,
        lt: &LessThanExpression,
        ctx: &Context,
    ) -> crate::evaluate::Result<ColumnValues> {
        let left = self.evaluate(&lt.left, ctx)?;
        let right = self.evaluate(&lt.right, ctx)?;

        match (&left, &right) {
            (ColumnValues::Int2(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                let mut values = Vec::new();
                let mut valid = Vec::new();
                for i in 0..lv.len() {
                    if lv_valid[i] && rv_valid[i] {
                        values.push(lv[i] < rv[i] as i16);
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
                        values.push(lv[i] < rv[i]);
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
                        values.push(lv[i] < rv[i]);
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
                        values.push(lv[i] < rv[i]);
                        valid.push(true);
                    } else {
                        values.push(false);
                        valid.push(false);
                    }
                }
                Ok(ColumnValues::bool_with_validity(values, valid))
            }
            (ColumnValues::Int4(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                compare!(lv, rv, lv_valid, rv_valid, |x| x as i32)
            }
            (ColumnValues::Int8(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                compare!(lv, rv, lv_valid, rv_valid, |x| x as i64)
            }
            (ColumnValues::Int16(lv, lv_valid), ColumnValues::Int1(rv, rv_valid)) => {
                compare!(lv, rv, lv_valid, rv_valid, |x| x as i128)
            }
            (left, right) => unimplemented!("{left:?} {right:?}"),
        }
    }
}
