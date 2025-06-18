// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::ColumnValues;
use reifydb_rql::expression::Expression;

use crate::function::{FunctionRegistry, math};
pub use error::Error;

pub(crate) use context::{Context, Demote, EvaluationColumn, Promote};

mod call;
mod column;
mod constant;
mod context;
mod error;
mod compare;
mod arith;
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
    pub(crate) fn evaluate(&mut self, expr: &Expression, ctx: &Context) -> Result<ColumnValues> {
        match expr {
            Expression::Add(expr) => self.add(expr, ctx),
            Expression::Divide(expr) => self.divide(expr, ctx),
            Expression::Call(expr) => self.call(expr, ctx),
            Expression::Column(expr) => self.column(expr, ctx),
            Expression::Constant(expr) => self.constant(expr, ctx),
            Expression::GreaterThan(expr) => self.greater_than(expr, ctx),
            Expression::GreaterThanEqual(expr) => self.greater_than_equal(expr, ctx),
            Expression::LessThan(expr) => self.less_than(expr, ctx),
            Expression::LessThanEqual(expr) => self.less_than_equal(expr, ctx),
            Expression::Equal(expr) => self.equal(expr, ctx),
            Expression::NotEqual(expr) => self.not_equal(expr, ctx),
            Expression::Modulo(expr) => self.modulo(expr, ctx),
            Expression::Multiply(expr) => self.multiply(expr, ctx),
            Expression::Prefix(expr) => self.prefix(expr, ctx),
            Expression::Subtract(expr) => self.subtract(expr, ctx),
            expr => unimplemented!("{expr:?}"),
        }
    }
}

pub fn evaluate(expr: &Expression, ctx: &Context) -> Result<ColumnValues> {
    let mut evaluator = Evaluator { functions: FunctionRegistry::new() };

    evaluator.functions.register(math::AbsFunction {});
    evaluator.functions.register(math::AvgFunction {});

    evaluator.evaluate(expr, ctx)
}

#[cfg(test)]
mod tests {
    #[test]
    #[ignore]
    fn test() {
        todo!()
    }
}
