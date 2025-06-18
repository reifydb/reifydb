// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate;
use crate::evaluate::{Context, Evaluator};
use crate::frame::ColumnValues;
use reifydb_rql::expression::ColumnExpression;

impl Evaluator {
    pub(crate) fn column(
        &mut self,
        column: &ColumnExpression,
        ctx: &Context,
    ) -> evaluate::Result<ColumnValues> {
        let columns = ctx.columns;
        let row_count = ctx.row_count;
        Ok(columns
            .iter()
            .find(|c| c.name == *column.0.fragment)
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
