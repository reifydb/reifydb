// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::evaluate::evaluate;
use crate::execute::Executor;
use crate::old_execute;
use base::expression::AliasExpression;
use dataframe::{Column, DataFrame};
use transaction::NopStore;

impl Executor {
    pub(crate) fn project(&mut self, expressions: Vec<AliasExpression>) -> crate::Result<()> {
        if self.frame.is_empty() {
            let mut columns = vec![];

            for (idx, expr) in expressions.into_iter().enumerate() {
                let value = old_execute::evaluate::<NopStore>(expr.expression, None, None).unwrap();
                columns.push(Column { name: format!("{}", idx + 1), data: value.into() });
            }

            self.frame = DataFrame::new(columns);
            return Ok(());
        }

        self.frame.project(|columns, row_count| {
            let mut new_columns = Vec::with_capacity(expressions.len());

            for expression in expressions {
                let expr = expression.expression;
                let name = expression.alias.unwrap_or(expr.to_string());

                let evaluated_column = evaluate(&expr, &columns, row_count)?;
                new_columns.push(Column { name: name.into(), data: evaluated_column });
            }

            Ok(new_columns)
        })?;

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
