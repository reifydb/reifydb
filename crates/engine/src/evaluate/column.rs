// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, Evaluator};
use reifydb_frame::{Column, ColumnValues};
use reifydb_rql::expression::ColumnExpression;

impl Evaluator {
    pub(crate) fn column(
        &mut self,
        column: ColumnExpression,
        ctx: &Context,
        columns: &[&Column],
        row_count: usize,
    ) -> evaluate::Result<ColumnValues> {
        Ok(columns
            .iter()
            .find(|c| c.name == *column.0.fragment)
            .cloned()
            .cloned()
            .map(|c| c.data)
            .unwrap_or(ColumnValues::undefined(row_count)))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
