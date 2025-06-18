// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::{Column, ColumnValues};
use reifydb_rql::expression::Expression;

use crate::function::{FunctionRegistry, math};
pub use error::Error;

pub(crate) use context::{Context, Demote, EvaluationColumn, Promote};

mod call;
mod column;
mod constant;
mod context;
mod error;
mod filter;
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
        expr: &Expression,
        ctx: &Context,
        columns: &[&Column],
        row_count: usize,
    ) -> Result<ColumnValues> {
        match expr {
            Expression::Add(expr) => self.add(expr, ctx, columns, row_count),
            Expression::Divide(expr) => self.divide(expr, ctx, columns, row_count),
            Expression::Call(expr) => self.call(expr, ctx, columns, row_count),
            Expression::Column(expr) => self.column(expr, ctx, columns, row_count),
            Expression::Constant(expr) => self.constant(expr, ctx),
            // Expression::GreaterThan(expr) => self.greater_than(expr, ctx, columns, row_count),
            Expression::GreaterThan(expr) => unimplemented!(),
            Expression::Modulo(expr) => self.modulo(expr, ctx, columns, row_count),
            Expression::Multiply(expr) => self.multiply(expr, ctx, columns, row_count),
            Expression::Prefix(expr) => self.prefix(expr, ctx, columns, row_count),
            Expression::Subtract(expr) => self.subtract(expr, ctx, columns, row_count),
            expr => unimplemented!("{expr:?}"),
        }
    }
}

pub fn evaluate(
    expr: &Expression,
    ctx: &Context,
    columns: &[&Column],
    row_count: usize,
) -> Result<ColumnValues> {
    let mut evaluator = Evaluator { functions: FunctionRegistry::new() };

    evaluator.functions.register(math::AbsFunction {});
    evaluator.functions.register(math::AvgFunction {});

    evaluator.evaluate(expr, ctx, columns, row_count)
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
