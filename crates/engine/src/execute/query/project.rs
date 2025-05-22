// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::execute::Executor;
use crate::old_execute::evaluate;
use base::expression::AliasExpression;
use dataframe::{Column, DataFrame};
use transaction::NopStore;

impl Executor {
    pub(crate) fn project(&mut self, expressions: Vec<AliasExpression>) -> crate::Result<()> {
        if self.frame.is_empty() {
            let mut columns = vec![];

            for (idx, expr) in expressions.into_iter().enumerate() {
                let value = evaluate::<NopStore>(expr.expression, None, None).unwrap();
                columns.push(Column { name: format!("{}", idx + 1), data: value.into() });
            }

            self.frame = DataFrame::new(columns);
            return Ok(());
        }

        self.frame.project(expressions)?;
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
