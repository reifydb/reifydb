// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::columnar::{ColumnQualified, TableQualified};
use reifydb_rql::expression::Expression;

use crate::columnar::Column;
use crate::function::{Functions, blob, math};
pub(crate) use context::{Convert, Demote, EvaluationContext, Promote};

mod access;
mod alias;
mod arith;
mod call;
pub(crate) mod cast;
mod column;
mod compare;
pub(crate) mod constant;
mod context;
mod logic;
mod prefix;
mod tuple;

pub(crate) struct Evaluator {
    functions: Functions,
}

impl Default for Evaluator {
    fn default() -> Self {
        Self {
            functions: Functions::builder()
                .register_scalar("abs", math::scalar::Abs::new)
                .register_scalar("avg", math::scalar::Avg::new)
                .register_scalar("blob::hex", blob::BlobHex::new)
                .register_scalar("blob::b64", blob::BlobB64::new)
                .register_scalar("blob::b64url", blob::BlobB64url::new)
                .register_scalar("blob::utf8", blob::BlobUtf8::new)
                .build(),
        }
    }
}

impl Evaluator {
    pub(crate) fn evaluate(
        &mut self,
        expr: &Expression,
        ctx: &EvaluationContext,
    ) -> crate::Result<Column> {
        match expr {
            Expression::AccessTable(expr) => self.access(expr, ctx),
            Expression::Alias(expr) => self.alias(expr, ctx),
            Expression::Add(expr) => self.add(expr, ctx),
            Expression::Div(expr) => self.div(expr, ctx),
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
            Expression::Between(expr) => self.between(expr, ctx),
            Expression::And(expr) => self.and(expr, ctx),
            Expression::Or(expr) => self.or(expr, ctx),
            Expression::Xor(expr) => self.xor(expr, ctx),
            Expression::Rem(expr) => self.rem(expr, ctx),
            Expression::Mul(expr) => self.mul(expr, ctx),
            Expression::Prefix(expr) => self.prefix(expr, ctx),
            Expression::Sub(expr) => self.sub(expr, ctx),
            Expression::Tuple(expr) => self.tuple(expr, ctx),
            expr => unimplemented!("{expr:?}"),
        }
    }
}

pub fn evaluate(expr: &Expression, ctx: &EvaluationContext) -> crate::Result<Column> {
    let mut evaluator = Evaluator {
        functions: Functions::builder()
            .register_scalar("abs", math::scalar::Abs::new)
            .register_scalar("avg", math::scalar::Avg::new)
            .register_scalar("blob::hex", blob::BlobHex::new)
            .register_scalar("blob::b64", blob::BlobB64::new)
            .register_scalar("blob::b64url", blob::BlobB64url::new)
            .register_scalar("blob::utf8", blob::BlobUtf8::new)
            .build(),
    };

    // Ensures that result column data type matches the expected target column type
    if let Some(ty) = ctx.target_column.as_ref().and_then(|c| c.column_type) {
        let mut column = evaluator.evaluate(expr, ctx)?;
        let data = cast::cast_column_data(&column.data(), ty, ctx, expr.lazy_span())?;
        column = match column.table() {
            Some(table) => Column::TableQualified(TableQualified {
                table: table.to_string(),
                name: column.name().to_string(),
                data,
            }),
            None => {
                Column::ColumnQualified(ColumnQualified { name: column.name().to_string(), data })
            }
        };
        Ok(column)
    } else {
        evaluator.evaluate(expr, ctx)
    }
}
