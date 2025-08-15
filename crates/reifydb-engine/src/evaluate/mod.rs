// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub(crate) use reifydb_core::interface::{
	Convert, Demote, EvaluationContext, Promote,
};
use reifydb_core::interface::{Evaluate, evaluate::expression::Expression};

use crate::{
	columnar::{Column, ColumnQualified, SourceQualified},
	function::{Functions, blob, math},
};

mod access;
mod alias;
mod arith;
mod call;
pub(crate) mod cast;
mod column;
mod compare;
pub(crate) mod constant;
mod logic;
mod parameter;
mod prefix;
mod tuple;

pub struct Evaluator {
	functions: Functions,
}

impl Default for Evaluator {
	fn default() -> Self {
		Self {
			functions: Functions::builder()
				.register_scalar("abs", math::scalar::Abs::new)
				.register_scalar("avg", math::scalar::Avg::new)
				.register_scalar(
					"blob::hex",
					blob::BlobHex::new,
				)
				.register_scalar(
					"blob::b64",
					blob::BlobB64::new,
				)
				.register_scalar(
					"blob::b64url",
					blob::BlobB64url::new,
				)
				.register_scalar(
					"blob::utf8",
					blob::BlobUtf8::new,
				)
				.build(),
		}
	}
}

impl Evaluate for Evaluator {
	fn evaluate(
		&self,
		ctx: &EvaluationContext,
		expr: &Expression,
	) -> reifydb_core::Result<Column> {
		match expr {
			Expression::AccessSource(expr) => {
				self.access(ctx, expr)
			}
			Expression::Alias(expr) => self.alias(ctx, expr),
			Expression::Add(expr) => self.add(ctx, expr),
			Expression::Div(expr) => self.div(ctx, expr),
			Expression::Call(expr) => self.call(ctx, expr),
			Expression::Cast(expr) => self.cast(ctx, expr),
			Expression::Column(expr) => self.column(ctx, expr),
			Expression::Constant(expr) => self.constant(ctx, expr),
			Expression::GreaterThan(expr) => {
				self.greater_than(ctx, expr)
			}
			Expression::GreaterThanEqual(expr) => {
				self.greater_than_equal(ctx, expr)
			}
			Expression::LessThan(expr) => self.less_than(ctx, expr),
			Expression::LessThanEqual(expr) => {
				self.less_than_equal(ctx, expr)
			}
			Expression::Equal(expr) => self.equal(ctx, expr),
			Expression::NotEqual(expr) => self.not_equal(ctx, expr),
			Expression::Between(expr) => self.between(ctx, expr),
			Expression::And(expr) => self.and(ctx, expr),
			Expression::Or(expr) => self.or(ctx, expr),
			Expression::Xor(expr) => self.xor(ctx, expr),
			Expression::Rem(expr) => self.rem(ctx, expr),
			Expression::Mul(expr) => self.mul(ctx, expr),
			Expression::Prefix(expr) => self.prefix(ctx, expr),
			Expression::Sub(expr) => self.sub(ctx, expr),
			Expression::Tuple(expr) => self.tuple(ctx, expr),
			Expression::Parameter(expr) => {
				self.parameter(ctx, expr)
			}
			expr => unimplemented!("{expr:?}"),
		}
	}
}

pub fn evaluate(
	ctx: &EvaluationContext,
	expr: &Expression,
) -> crate::Result<Column> {
	let evaluator = Evaluator {
		functions: Functions::builder()
			.register_scalar("abs", math::scalar::Abs::new)
			.register_scalar("avg", math::scalar::Avg::new)
			.register_scalar("blob::hex", blob::BlobHex::new)
			.register_scalar("blob::b64", blob::BlobB64::new)
			.register_scalar("blob::b64url", blob::BlobB64url::new)
			.register_scalar("blob::utf8", blob::BlobUtf8::new)
			.build(),
	};

	// Ensures that result column data type matches the expected target
	// column type
	if let Some(ty) = ctx.target_column.as_ref().and_then(|c| c.column_type)
	{
		let mut column = evaluator.evaluate(ctx, expr)?;
		let data = cast::cast_column_data(
			ctx,
			&column.data(),
			ty,
			expr.lazy_span(),
		)?;
		column = match column.table() {
			Some(source) => {
				Column::SourceQualified(SourceQualified {
					source: source.to_string(),
					name: column.name().to_string(),
					data,
				})
			}
			None => Column::ColumnQualified(ColumnQualified {
				name: column.name().to_string(),
				data,
			}),
		};
		Ok(column)
	} else {
		evaluator.evaluate(ctx, expr)
	}
}
