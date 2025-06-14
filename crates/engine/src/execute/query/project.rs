// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::{Context, evaluate};
use crate::execute::Executor;
use reifydb_frame::{Column, Frame};
use reifydb_rql::expression::AliasExpression;
use reifydb_storage::VersionedStorage;

impl<VS: VersionedStorage> Executor<VS> {
    pub(crate) fn project(&mut self, expressions: Vec<AliasExpression>) -> crate::Result<()> {
        if self.frame.is_empty() {
            let mut columns = vec![];

            for (idx, expr) in expressions.into_iter().enumerate() {
                let expr = expr.expression;

                let value = evaluate(&expr, &Context { column: None, frame: None }, &[], 1)?;
                columns.push(Column { name: format!("{}", idx + 1), data: value });
            }

            self.frame = Frame::new(columns);
            return Ok(());
        }

        let row_count = self.frame.columns.first().map_or(0, |col| col.data.len());
        let columns: Vec<&Column> = self.frame.columns.iter().map(|c| c).collect();

        let mut new_columns = Vec::with_capacity(expressions.len());

        for expression in expressions {
            let expr = expression.expression;
            let name = expression.alias.unwrap_or(expr.to_string());

            let evaluated_column = evaluate(
                &expr,
                &Context { column: None, frame: Some(self.frame.clone()) },
                &columns,
                row_count,
            )?;

            new_columns.push(Column { name: name.into(), data: evaluated_column });
        }

        self.frame.columns = new_columns;

        // self.frame.project(|columns, row_count| {
        //     let mut new_columns = Vec::with_capacity(expressions.len());
        //
        //     for expression in expressions {
        //         let expr = expression.expression;
        //         let name = expression.alias.unwrap_or(expr.to_string());
        //
        //         let evaluated_column =
        //             evaluate(expr, &Context { column: None, frame: Some(self.frame)}, &columns, row_count)?;
        //         new_columns.push(Column { name: name.into(), data: evaluated_column });
        //     }
        //
        //     Ok(new_columns)
        // })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn implement() {
        todo!()
    }
}
