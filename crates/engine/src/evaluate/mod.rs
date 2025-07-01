// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::frame::Column;
use reifydb_rql::expression::Expression;

use crate::function::{Functions, math};
pub use error::Error;

pub(crate) use context::{Context, Convert, Demote, EvaluationColumn, Promote};

mod access;
mod alias;
mod arith;
mod call;
mod cast;
mod column;
mod compare;
mod constant;
mod context;
mod error;
mod prefix;

pub(crate) type Result<T> = std::result::Result<T, Error>;

pub(crate) struct Evaluator {
    functions: Functions,
}

impl Default for Evaluator {
    fn default() -> Self {
        Self { functions: Functions::builder().build() }
    }
}

impl Evaluator {
    pub(crate) fn evaluate(&mut self, expr: &Expression, ctx: &Context) -> Result<Column> {
        match expr {
            Expression::AccessTable(expr) => self.access(expr, ctx),
            Expression::Alias(expr) => self.alias(expr, ctx),
            Expression::Add(expr) => self.add(expr, ctx),
            Expression::Divide(expr) => self.divide(expr, ctx),
            Expression::Call(expr) => self.call(expr, ctx),
            Expression::Cast(expr) => self.cast(expr, ctx),
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
            Expression::Subtract(expr) => self.sub(expr, ctx),
            expr => unimplemented!("{expr:?}"),
        }
    }
}

pub fn evaluate(expr: &Expression, ctx: &Context) -> Result<Column> {
    let mut evaluator = Evaluator {
        functions: Functions::builder()
            .register_scalar("abs", math::scalar::Abs::new)
            .register_scalar("avg", math::scalar::Avg::new)
            .build(),
    };

    evaluator.evaluate(expr, ctx)
}
