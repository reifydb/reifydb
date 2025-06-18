// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

mod compile;
mod evaluate;
mod mask;
mod populate;

use crate::frame::Frame;
use reifydb_rql::expression::{AliasExpression, Expression};

#[derive(Debug, PartialEq)]
pub enum Source {
    None,
    Table { schema: String, table: String },
}

#[derive(Debug)]
pub struct LazyFrame {
    source: Source,
    frame: Frame,
    expressions: Vec<AliasExpression>,
    filter: Vec<Expression>,
    limit: Option<usize>,
}

impl LazyFrame {
    fn row_count(&self) -> usize {
        self.frame.row_count()
    }
}
