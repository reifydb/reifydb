// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::FieldRef;
use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	error::diagnostic::catalog::{enum_value_expected, enum_value_with_fields, sumtype_variant_not_found},
	expression::{
		ConstantExpression, EqExpression, Expression, IsVariantExpression, NotEqExpression, PrefixExpression,
		PrefixOperator,
	},
	interface::resolved::ResolvedObject,
	value::{batch::filter, column::headers::ColumnHeaders},
};
use reifydb_evaluate::{expression::context::EvalContext, lower::LoweredExpr};
use reifydb_extension::transform::{Transform, context::TransformContext};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions, return_error,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
	},
};
use tracing::instrument;

use super::NoopNode;
use crate::{
	Result,
	vm::volcano::{
		inline::{
			declared_sumtype_id, enum_name, param_symbol, resolve_is_variants, resolve_sumtype_ctor,
			symbol_scalar, variant_by_name,
		},
		query::{QueryContext, QueryNode, eval_context_from_transform},
		udf::{UdfEvalNode, strip_udf_columns},
	},
};

pub(crate) struct FilterNode {
	input: Box<dyn QueryNode>,
	expressions: Vec<Expression>,
	source: Option<ResolvedObject>,
	operator: &'static str,
	udf_names: Vec<String>,
	context: Option<(Arc<QueryContext>, Vec<LoweredExpr>)>,
	emitted: bool,

	empty: Option<RecordBatch>,
}

impl FilterNode {
	pub fn new(input: Box<dyn QueryNode>, expressions: Vec<Expression>, operator: &'static str) -> Self {
		Self::with_source(input, expressions, None, operator)
	}

	pub fn with_source(
		input: Box<dyn QueryNode>,
		expressions: Vec<Expression>,
		source: Option<ResolvedObject>,
		operator: &'static str,
	) -> Self {
		Self {
			input,
			expressions,
			source,
			operator,
			udf_names: Vec::new(),
			context: None,
			emitted: false,
			empty: None,
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::filter::eval")]
	fn eval_predicate(
		session: &EvalContext,
		lowered: &LoweredExpr,
		columns: &RecordBatch,
		row_count: usize,
	) -> Result<(FieldRef, ArrayRef)> {
		let exec_ctx = session.with_eval(columns.clone(), row_count);
		lowered.evaluate(&exec_ctx)
	}

	#[instrument(level = "trace", skip_all, name = "volcano::filter::mask")]
	fn build_mask(result: &ColumnView<'_>, row_count: usize) -> BooleanBuffer {
		match &result.data {
			ViewData::Bool(container) => {
				BooleanBuffer::collect_bool(row_count, |i| result.is_defined(i) && container.value(i))
			}
			_ => panic!("filter expression must evaluate to a boolean column"),
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::filter::compact")]
	fn compact(columns: &RecordBatch, mask: &BooleanBuffer) -> Result<RecordBatch> {
		filter(columns, mask)
	}
}

impl QueryNode for FilterNode {
	#[instrument(level = "trace", skip_all, name = "volcano::filter::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &QueryContext) -> Result<()> {
		if let Some(source) = self.source.as_ref() {
			for expr in &mut self.expressions {
				resolve_is_variants(&ctx.services.catalog, rx, source, expr)?;
				resolve_variant_equality(&ctx.services.catalog, rx, source, expr)?;
				resolve_enum_param_equality(ctx, rx, source, expr)?;
			}
		}
		let (input, expressions, udf_names) = UdfEvalNode::wrap_if_needed(
			mem::replace(&mut self.input, Box::new(NoopNode)),
			&self.expressions,
			&ctx.symbols,
		);
		self.input = input;
		self.expressions = expressions;
		self.udf_names = udf_names;

		let lowered = self.expressions.iter().map(|e| LoweredExpr::new(e.clone(), self.operator)).collect();
		self.context = Some((Arc::new(ctx.clone()), lowered));
		self.input.initialize(rx, ctx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::filter::next")]
	fn next<'a>(&mut self, rx: &mut Transaction<'a>, ctx: &mut QueryContext) -> Result<Option<RecordBatch>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "FilterNode::next() called before initialize()");
		}
		let (stored_ctx, _) = self.context.as_ref().unwrap();
		let stored_ctx = stored_ctx.clone();

		loop {
			match self.input.next(rx, ctx)? {
				Some(columns) => {
					let transform_ctx = TransformContext {
						routines: &ctx.services.routines,
						runtime_context: &stored_ctx.services.runtime_context,
						params: &stored_ctx.params,
					};
					let columns = self.apply(&transform_ctx, columns)?;
					let columns = strip_udf_columns(columns, &self.udf_names)?;
					if columns.num_rows() > 0 {
						self.emitted = true;
						return Ok(Some(columns));
					}
					if !self.emitted && self.empty.is_none() {
						self.empty = Some(columns);
					}
				}
				None => {
					if self.emitted {
						return Ok(None);
					}
					self.emitted = true;
					return Ok(self.empty.take());
				}
			}
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.input.headers()
	}
}

impl Transform for FilterNode {
	fn apply(&self, ctx: &TransformContext, input: RecordBatch) -> Result<RecordBatch> {
		let (stored_ctx, lowered) =
			self.context.as_ref().expect("FilterNode::apply() called before initialize()");

		let session = eval_context_from_transform(ctx, stored_ctx);
		let mut columns = input;
		let mut row_count = columns.num_rows();

		for lowered_expr in lowered {
			if row_count == 0 {
				break;
			}

			let result = Self::eval_predicate(&session, lowered_expr, &columns, row_count)?;
			let filter_mask = Self::build_mask(&ColumnView::try_from(&result)?, row_count);

			columns = Self::compact(&columns, &filter_mask)?;
			row_count = columns.num_rows();
		}

		Ok(columns)
	}
}

pub(crate) fn resolve_variant_equality(
	catalog: &Catalog,
	rx: &mut Transaction<'_>,
	source: &ResolvedObject,
	expr: &mut Expression,
) -> Result<()> {
	match expr {
		Expression::And(e) => {
			resolve_variant_equality(catalog, rx, source, &mut e.left)?;
			resolve_variant_equality(catalog, rx, source, &mut e.right)
		}
		Expression::Or(e) => {
			resolve_variant_equality(catalog, rx, source, &mut e.left)?;
			resolve_variant_equality(catalog, rx, source, &mut e.right)
		}
		Expression::Prefix(e) => resolve_variant_equality(catalog, rx, source, &mut e.expression),
		Expression::Tuple(e) if e.expressions.len() == 1 => {
			resolve_variant_equality(catalog, rx, source, &mut e.expressions[0])
		}
		Expression::Equal(e) => {
			if let Some(is_variant) = variant_test(catalog, rx, source, &e.left, &e.right, &e.fragment)? {
				*expr = is_variant;
			}
			Ok(())
		}
		Expression::NotEqual(e) => {
			if let Some(is_variant) = variant_test(catalog, rx, source, &e.left, &e.right, &e.fragment)? {
				*expr = Expression::Prefix(PrefixExpression {
					operator: PrefixOperator::Not(e.fragment.clone()),
					expression: Box::new(is_variant),
					fragment: e.fragment.clone(),
				});
			}
			Ok(())
		}
		_ => Ok(()),
	}
}

fn variant_test(
	catalog: &Catalog,
	rx: &mut Transaction<'_>,
	source: &ResolvedObject,
	left: &Expression,
	right: &Expression,
	fragment: &Fragment,
) -> Result<Option<Expression>> {
	let (Expression::Column(column), Expression::SumTypeConstructor(ctor)) = (left, right) else {
		return Ok(None);
	};
	if !ctor.columns.is_empty() {
		return Ok(None);
	}
	let (sumtype, variant) = resolve_sumtype_ctor(catalog, rx, Some(source), &column.0.name, ctor)?;
	Ok(Some(Expression::IsVariant(IsVariantExpression {
		expression: Box::new(left.clone()),
		namespace: Some(ctor.namespace.clone()),
		sumtype_name: ctor.sumtype_name.clone(),
		variant_name: ctor.variant_name.clone(),
		tag: Some(sumtype.variants[variant].tag),
		fragment: fragment.clone(),
	})))
}

pub(crate) fn resolve_enum_param_equality(
	ctx: &QueryContext,
	rx: &mut Transaction<'_>,
	source: &ResolvedObject,
	expr: &mut Expression,
) -> Result<()> {
	let (left, right, fragment, negated) = match expr {
		Expression::And(e) => {
			resolve_enum_param_equality(ctx, rx, source, &mut e.left)?;
			return resolve_enum_param_equality(ctx, rx, source, &mut e.right);
		}
		Expression::Or(e) => {
			resolve_enum_param_equality(ctx, rx, source, &mut e.left)?;
			return resolve_enum_param_equality(ctx, rx, source, &mut e.right);
		}
		Expression::Prefix(e) => return resolve_enum_param_equality(ctx, rx, source, &mut e.expression),
		Expression::Tuple(e) if e.expressions.len() == 1 => {
			return resolve_enum_param_equality(ctx, rx, source, &mut e.expressions[0]);
		}
		Expression::Equal(e) => (e.left.clone(), e.right.clone(), e.fragment.clone(), false),
		Expression::NotEqual(e) => (e.left.clone(), e.right.clone(), e.fragment.clone(), true),
		_ => return Ok(()),
	};
	let Expression::Column(column) = left.as_ref() else {
		return Ok(());
	};
	let col_name = column.0.name.text();
	let Some(declared) = declared_sumtype_id(source, col_name) else {
		return Ok(());
	};
	if let Some(name) = enum_name(ctx, right.as_ref())? {
		let sumtype = ctx.services.catalog.get_sumtype(rx, declared)?;
		let variant = &sumtype.variants[variant_by_name(&sumtype, &name)?];
		*expr = variant_test_expr(left, &sumtype.name, &variant.name, variant.tag, fragment, negated);
		return Ok(());
	}
	let Some(symbol) = param_symbol(right.as_ref()) else {
		return Ok(());
	};
	let Some(value) = symbol_scalar(ctx, symbol)? else {
		return Ok(());
	};
	let sumtype = ctx.services.catalog.get_sumtype(rx, declared)?;
	let param_fragment = right.full_fragment_owned();
	let tag = match value {
		Value::None {
			..
		} => {
			let none = Box::new(Expression::Constant(ConstantExpression::None {
				fragment: param_fragment,
			}));
			*expr = match negated {
				false => Expression::Equal(EqExpression {
					left,
					right: none,
					fragment,
				}),
				true => Expression::NotEqual(NotEqExpression {
					left,
					right: none,
					fragment,
				}),
			};
			return Ok(());
		}
		Value::Enum {
			id,
			tag,
			fields,
		} if id == declared => {
			if !fields.is_empty() {
				return_error!(enum_value_with_fields(param_fragment, col_name));
			}
			tag
		}
		other => return_error!(enum_value_expected(param_fragment, col_name, &sumtype.name, other.get_type())),
	};
	let Some(variant) = sumtype.variants.iter().find(|variant| variant.tag == tag) else {
		return_error!(sumtype_variant_not_found(param_fragment, &sumtype.name));
	};
	*expr = variant_test_expr(left, &sumtype.name, &variant.name, tag, fragment, negated);
	Ok(())
}

fn variant_test_expr(
	left: Box<Expression>,
	sumtype: &str,
	variant: &str,
	tag: u8,
	fragment: Fragment,
	negated: bool,
) -> Expression {
	let is_variant = Expression::IsVariant(IsVariantExpression {
		expression: left,
		namespace: None,
		sumtype_name: Fragment::internal(sumtype),
		variant_name: Fragment::internal(variant),
		tag: Some(tag),
		fragment: fragment.clone(),
	});
	match negated {
		false => is_variant,
		true => Expression::Prefix(PrefixExpression {
			operator: PrefixOperator::Not(fragment.clone()),
			expression: Box::new(is_variant),
			fragment,
		}),
	}
}
