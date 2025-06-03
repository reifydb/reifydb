// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_frame::{Column, ColumnValues};
use reifydb_rql::expression::Expression;

use crate::function::{FunctionRegistry, math};
pub use error::Error;

mod call;
mod column;
mod constant;
mod error;
mod operator;
mod prefix;

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub(crate) struct Evaluator {
    functions: FunctionRegistry,
}

impl Default for Evaluator {
    fn default() -> Self {
        Self { functions: FunctionRegistry::new() }
    }
}

impl Evaluator {
    pub(crate) fn evaluate(
        &mut self,
        expr: Expression,
        columns: &[&Column],
        row_count: usize,
    ) -> Result<ColumnValues> {
        match expr {
            Expression::Add(expr) => self.add(expr, columns, row_count),
            Expression::Divide(expr) => self.divide(expr, columns, row_count),
            Expression::Call(expr) => self.call(expr, columns, row_count),
            Expression::Column(expr) => self.column(expr, columns, row_count),
            Expression::Constant(v) => self.constant(v, row_count),
            Expression::Modulo(expr) => self.modulo(expr, columns, row_count),
            Expression::Multiply(expr) => self.multiply(expr, columns, row_count),
            Expression::Prefix(expr) => self.prefix(expr, columns, row_count),
            Expression::Subtract(expr) => self.subtract(expr, columns, row_count),
            expr => unimplemented!("{expr:?}"),
        }
    }
}

pub fn evaluate(expr: Expression, columns: &[&Column], row_count: usize) -> Result<ColumnValues> {
    let mut evaluator = Evaluator { functions: FunctionRegistry::new() };

    evaluator.functions.register(math::AbsFunction {});
    evaluator.functions.register(math::AvgFunction {});

    evaluator.evaluate(expr, columns, row_count)
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
