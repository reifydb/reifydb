// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, evaluate};
use crate::frame::lazy::Source;
use crate::frame::{Column, Frame, LazyFrame};
use reifydb_core::BitVec;
use reifydb_transaction::Rx;

impl LazyFrame {
    pub fn evaluate(mut self, rx: &mut impl Rx) -> crate::frame::Result<Frame> {
        // FIXME refactor this - comes from calling SELECT directly
        if self.source == Source::None {
            let mut columns = vec![];

            for (idx, expr) in self.expressions.clone().into_iter().enumerate() {
                let expr = expr.expression;

                let value = evaluate(
                    &expr,
                    &Context {
                        column: None,
                        mask: &BitVec::empty(),
                        columns: &[],
                        row_count: 1,
                        limit: None,
                    },
                )
                .unwrap();
                columns.push(Column { name: format!("{}", idx + 1), data: value });
            }

            self.frame = Frame::new(columns);
            return Ok(self.frame);
        }

        self.populate_frame(rx)?;

        let mask = self.compute_mask();

        if self.expressions.is_empty() {
            self.frame.filter(&mask)?;
            if let Some(limit) = self.limit {
                self.frame.limit(limit)?;
            }
            return Ok(self.frame);
        }

        let columns = self
            .expressions
            .iter()
            .map(|alias_expr| {
                let expr = &alias_expr.expression;
                let alias = alias_expr.alias.clone().unwrap_or(expr.span().fragment);

                let values = evaluate(
                    expr,
                    &Context {
                        column: None,
                        mask: &mask,
                        columns: self.frame.columns.as_slice(),
                        row_count: self.frame.row_count(),
                        limit: self.limit,
                    },
                )
                .unwrap();

                Column { name: alias.clone(), data: values }
            })
            .collect();

        Ok(Frame::new(columns))
    }
}
