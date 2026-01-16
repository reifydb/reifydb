// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::Column;
use reifydb_function::{
	blob::{b58::BlobB58, b64::BlobB64, b64url::BlobB64url, hex::BlobHex, utf8::BlobUtf8},
	flow::to_json::FlowNodeToJson,
	math::scalar::{abs::Abs, avg::Avg, max::Max, min::Min, power::Power, round::Round},
	registry::Functions,
	text::{
		format_bytes::{FormatBytes, FormatBytesSi},
		length::TextLength,
		substring::TextSubstring,
		trim::TextTrim,
		upper::TextUpper,
	},
};
use reifydb_rql::expression::Expression;

use crate::evaluate::ColumnEvaluationContext;

pub mod access;
pub mod alias;
pub mod arith;
pub mod call;
pub mod cast;
pub mod column;
pub mod compare;
pub(crate) mod constant;
pub mod extend_expr;
pub mod if_expr;
pub mod logic;
pub mod map_expr;
pub mod parameter;
pub mod prefix;
pub mod tuple;
pub mod variable;

#[derive(Clone)]
pub struct StandardColumnEvaluator {
	functions: Functions,
}

impl Default for StandardColumnEvaluator {
	fn default() -> Self {
		Self {
			functions: Functions::builder()
				.register_scalar("math::abs", Abs::new)
				.register_scalar("math::avg", Avg::new)
				.register_scalar("math::max", Max::new)
				.register_scalar("math::min", Min::new)
				.register_scalar("math::power", Power::new)
				.register_scalar("math::round", Round::new)
				.register_scalar("blob::hex", BlobHex::new)
				.register_scalar("blob::b58", BlobB58::new)
				.register_scalar("blob::b64", BlobB64::new)
				.register_scalar("blob::b64url", BlobB64url::new)
				.register_scalar("blob::utf8", BlobUtf8::new)
				.register_scalar("flow_node::to_json", FlowNodeToJson::new)
				.register_scalar("text::trim", TextTrim::new)
				.register_scalar("text::upper", TextUpper::new)
				.register_scalar("text::substring", TextSubstring::new)
				.register_scalar("text::length", TextLength::new)
				.register_scalar("text::format_bytes", FormatBytes::new)
				.register_scalar("text::format_bytes_si", FormatBytesSi::new)
				.build(),
		}
	}
}

impl StandardColumnEvaluator {
	pub fn evaluate(&self, ctx: &ColumnEvaluationContext, expr: &Expression) -> crate::Result<Column> {
		match expr {
			Expression::AccessSource(expr) => self.access(ctx, expr),
			Expression::Alias(expr) => self.alias(ctx, expr),
			Expression::Add(expr) => self.add(ctx, expr),
			Expression::Div(expr) => self.div(ctx, expr),
			Expression::Call(expr) => self.call(ctx, expr),
			Expression::Cast(expr) => self.cast(ctx, expr),
			Expression::Column(expr) => self.column(ctx, expr),
			Expression::Constant(expr) => self.constant(ctx, expr),
			Expression::GreaterThan(expr) => self.greater_than(ctx, expr),
			Expression::GreaterThanEqual(expr) => self.greater_than_equal(ctx, expr),
			Expression::LessThan(expr) => self.less_than(ctx, expr),
			Expression::LessThanEqual(expr) => self.less_than_equal(ctx, expr),
			Expression::Equal(expr) => self.equal(ctx, expr),
			Expression::NotEqual(expr) => self.not_equal(ctx, expr),
			Expression::Between(expr) => self.between(ctx, expr),
			Expression::In(expr) => self.in_expr(ctx, expr),
			Expression::And(expr) => self.and(ctx, expr),
			Expression::Or(expr) => self.or(ctx, expr),
			Expression::Xor(expr) => self.xor(ctx, expr),
			Expression::Rem(expr) => self.rem(ctx, expr),
			Expression::Mul(expr) => self.mul(ctx, expr),
			Expression::Prefix(expr) => self.prefix(ctx, expr),
			Expression::Sub(expr) => self.sub(ctx, expr),
			Expression::Tuple(expr) => self.tuple(ctx, expr),
			Expression::Parameter(expr) => self.parameter(ctx, expr),
			Expression::Variable(expr) => self.variable(ctx, expr),
			Expression::If(expr) => self.if_expr(ctx, expr),
			Expression::Map(expr) => self.map_expr(ctx, expr),
			Expression::Extend(expr) => self.extend_expr(ctx, expr),
			expr => unimplemented!("{expr:?}"),
		}
	}
}

pub fn evaluate(ctx: &ColumnEvaluationContext, expr: &Expression) -> crate::Result<Column> {
	let evaluator = StandardColumnEvaluator {
		functions: Functions::builder()
			.register_scalar("math::abs", Abs::new)
			.register_scalar("math::avg", Avg::new)
			.register_scalar("math::max", Max::new)
			.register_scalar("math::min", Min::new)
			.register_scalar("math::power", Power::new)
			.register_scalar("math::round", Round::new)
			.register_scalar("blob::hex", BlobHex::new)
			.register_scalar("blob::b58", BlobB58::new)
			.register_scalar("blob::b64", BlobB64::new)
			.register_scalar("blob::b64url", BlobB64url::new)
			.register_scalar("blob::utf8", BlobUtf8::new)
			.register_scalar("flow_node::to_json", FlowNodeToJson::new)
			.register_scalar("text::trim", TextTrim::new)
			.register_scalar("text::upper", TextUpper::new)
			.register_scalar("text::substring", TextSubstring::new)
			.register_scalar("text::length", TextLength::new)
			.register_scalar("text::format_bytes", FormatBytes::new)
			.register_scalar("text::format_bytes_si", FormatBytesSi::new)
			.build(),
	};

	// Ensures that result column data type matches the expected target
	// column type
	if let Some(ty) = ctx.target.as_ref().map(|c| c.column_type()) {
		let mut column = evaluator.evaluate(ctx, expr)?;
		let data = cast::cast_column_data(ctx, &column.data(), ty, &expr.lazy_fragment())?;
		column = Column {
			name: column.name,
			data,
		};
		Ok(column)
	} else {
		evaluator.evaluate(ctx, expr)
	}
}
