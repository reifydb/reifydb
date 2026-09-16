// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, HashMap},
	mem,
	sync::Arc,
};

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	error::diagnostic::{
		catalog::{column_not_sumtype, namespace_not_found, sumtype_not_found, sumtype_variant_not_found},
		query::{column_not_found, duplicate_field},
	},
	interface::{catalog::sumtype::SumType, evaluate::TargetColumn, resolved::ResolvedObject},
	value::column::{
		ColumnWithName, buffer::ColumnBuffer, cast::cast_column_data, columns::Columns, headers::ColumnHeaders,
	},
};
use reifydb_evaluate::expression::{context::EvalContext, eval::evaluate};
use reifydb_rql::expression::{
	AliasExpression, CastExpression, ConstantExpression, Expression, IdentExpression, SumTypeConstructorExpression,
	TypeExpression, name::display_label, variant::for_each_is_variant,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	reifydb_assertions, return_error,
	value::{Value, constraint::Constraint, sumtype::SumTypeId, value_type::ValueType},
};
use tracing::instrument;

use crate::{
	Result,
	vm::{
		instruction::dml::series_insert::resolve_variant_tag,
		volcano::query::{QueryContext, QueryNode, eval_context_from_query},
	},
};

pub(crate) struct InlineDataNode {
	rows: Vec<Vec<AliasExpression>>,
	headers: Option<ColumnHeaders>,
	context: Option<Arc<QueryContext>>,
	series_tag: Option<SumType>,
	executed: bool,
}

impl InlineDataNode {
	pub fn new(rows: Vec<Vec<AliasExpression>>, context: Arc<QueryContext>) -> Self {
		let cloned_context = context.clone();
		let headers = cloned_context.source.as_ref().map(|source| {
			let mut layout = Self::create_columns_layout_from_source(source);
			if Self::series_tag_id(source).is_some()
				&& !layout.columns.iter().any(|c| c.text() == "tag")
				&& rows.iter().flatten().any(|alias| alias.alias.0.text() == "tag")
			{
				layout.columns.push(Fragment::internal("tag"));
			}
			layout
		});

		Self {
			rows,
			headers,
			context: Some(context),
			series_tag: None,
			executed: false,
		}
	}

	fn create_columns_layout_from_source(source: &ResolvedObject) -> ColumnHeaders {
		ColumnHeaders {
			columns: source.columns().iter().map(|col| Fragment::internal(&col.name)).collect(),
		}
	}

	fn series_tag_id(source: &ResolvedObject) -> Option<SumTypeId> {
		match source {
			ResolvedObject::Series(series) => series.def().tag,
			_ => None,
		}
	}

	fn load_series_tag<'a>(&mut self, txn: &mut Transaction<'a>) -> Result<()> {
		let Some(ctx) = self.context.as_ref() else {
			return Ok(());
		};
		if let Some(tag_id) = ctx.source.as_ref().and_then(Self::series_tag_id) {
			self.series_tag = Some(ctx.services.catalog.get_sumtype(txn, tag_id)?);
		}
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::inline::expand_sumtypes")]
	fn expand_sumtype_constructors<'a>(&mut self, txn: &mut Transaction<'a>) -> Result<()> {
		let Some(ctx) = self.context.as_ref().cloned() else {
			return Ok(());
		};
		if !rows_need_sumtype_expansion(&self.rows) {
			return Ok(());
		}
		for row in &mut self.rows {
			let original = mem::take(row);
			let mut expanded = Vec::with_capacity(original.len());
			let mut written = Vec::with_capacity(original.len());
			for alias_expr in original {
				let name = alias_expr.alias.0.clone();
				if let Some(sumtype) = self.series_tag.as_ref()
					&& alias_expr.alias.0.text() == "tag"
				{
					expanded.push(resolve_series_tag_name(&ctx, txn, sumtype, alias_expr)?);
					written.push(name);
					continue;
				}
				match alias_expr.expression.as_ref() {
					Expression::SumTypeConstructor(_) => {
						expand_sumtype_ctor(
							&ctx,
							txn,
							ctx.source.as_ref(),
							alias_expr,
							&mut expanded,
						)?;
					}
					Expression::Column(_) => {
						expand_unit_variant_column(
							&ctx,
							txn,
							ctx.source.as_ref(),
							alias_expr,
							&mut expanded,
						)?;
					}
					_ => {
						expand_sumtype_none(
							&ctx,
							txn,
							ctx.source.as_ref(),
							alias_expr,
							&mut expanded,
						)?;
					}
				}
				written.resize(expanded.len(), name);
			}
			let names: Vec<Fragment> = expanded.iter().map(|alias| alias.alias.0.clone()).collect();
			reject_variant_column_clashes(&names, &written)?;
			*row = expanded;
		}
		Ok(())
	}
}

#[inline]
fn rows_need_sumtype_expansion(rows: &[Vec<AliasExpression>]) -> bool {
	for row in rows {
		for alias_expr in row {
			if matches!(
				alias_expr.expression.as_ref(),
				Expression::SumTypeConstructor(_)
					| Expression::Column(_) | Expression::Constant(ConstantExpression::None { .. })
			) {
				return true;
			}
		}
	}
	false
}

pub(crate) fn expand_aliases(
	expressions: Vec<Expression>,
	mut expand: impl FnMut(AliasExpression, &mut Vec<AliasExpression>) -> Result<()>,
) -> Result<(Vec<Expression>, Vec<Fragment>)> {
	let mut result = Vec::with_capacity(expressions.len());
	let mut written = Vec::with_capacity(expressions.len());
	let mut expanded = Vec::new();
	for expr in expressions {
		let name = display_label(&expr);
		let Expression::Alias(alias_expr) = expr else {
			result.push(expr);
			written.push(name);
			continue;
		};
		expand(alias_expr, &mut expanded)?;
		written.extend(expanded.iter().map(|_| name.clone()));
		result.extend(expanded.drain(..).map(Expression::Alias));
	}
	Ok((result, written))
}

pub(crate) fn reject_variant_column_clashes(names: &[Fragment], written: &[Fragment]) -> Result<()> {
	for (index, name) in names.iter().enumerate() {
		let expanded = name.text() != written[index].text();
		if names[..index].iter().zip(written).any(|(earlier, earlier_written)| {
			earlier.text() == name.text() && (expanded || earlier.text() != earlier_written.text())
		}) {
			return_error!(duplicate_field(written[index].clone(), name.text()));
		}
	}
	Ok(())
}

pub(crate) fn expand_sumtype_ctor(
	ctx: &QueryContext,
	txn: &mut Transaction<'_>,
	source: Option<&ResolvedObject>,
	alias_expr: AliasExpression,
	expanded: &mut Vec<AliasExpression>,
) -> Result<()> {
	let Expression::SumTypeConstructor(ctor) = alias_expr.expression.as_ref() else {
		expanded.push(alias_expr);
		return Ok(());
	};
	let (sumtype, variant) = resolve_sumtype_ctor(&ctx.services.catalog, txn, source, &alias_expr.alias.0, ctor)?;
	let col_name = alias_expr.alias.0.text();
	let variant_name = sumtype.variants[variant].name.to_lowercase();

	expanded.push(tag_alias(source, col_name, sumtype.variants[variant].tag, &alias_expr.fragment));
	for (field_name, field_expr) in &ctor.columns {
		let phys_col_name = format!("{}_{}_{}", col_name, variant_name, field_name.text().to_lowercase());
		expanded.push(AliasExpression {
			alias: IdentExpression(Fragment::internal(phys_col_name)),
			expression: Box::new(field_expr.clone()),
			fragment: alias_expr.fragment.clone(),
		});
	}

	Ok(())
}

fn expand_sumtype_none(
	ctx: &QueryContext,
	txn: &mut Transaction<'_>,
	source: Option<&ResolvedObject>,
	alias_expr: AliasExpression,
	expanded: &mut Vec<AliasExpression>,
) -> Result<()> {
	let col_name = alias_expr.alias.0.text();
	let is_none = matches!(alias_expr.expression.as_ref(), Expression::Constant(ConstantExpression::None { .. }));
	let declared = source.and_then(|source| declared_sumtype_id(source, col_name));

	let (true, Some(id)) = (is_none, declared) else {
		expanded.push(alias_expr);
		return Ok(());
	};

	let sumtype = ctx.services.catalog.get_sumtype(txn, id)?;
	let fragment = alias_expr.fragment.clone();
	let none = || {
		Box::new(Expression::Constant(ConstantExpression::None {
			fragment: fragment.clone(),
		}))
	};

	expanded.push(AliasExpression {
		alias: IdentExpression(Fragment::internal(format!("{col_name}_tag"))),
		expression: none(),
		fragment: fragment.clone(),
	});
	for variant in &sumtype.variants {
		for field in &variant.fields {
			let phys_col_name =
				format!("{}_{}_{}", col_name, variant.name.to_lowercase(), field.name.to_lowercase());
			expanded.push(AliasExpression {
				alias: IdentExpression(Fragment::internal(phys_col_name)),
				expression: none(),
				fragment: fragment.clone(),
			});
		}
	}

	Ok(())
}

pub(crate) fn expand_sumtype_assignment(
	ctx: &QueryContext,
	txn: &mut Transaction<'_>,
	source: &ResolvedObject,
	alias_expr: AliasExpression,
	expanded: &mut Vec<AliasExpression>,
) -> Result<()> {
	match alias_expr.expression.as_ref() {
		Expression::SumTypeConstructor(ctor) => {
			let (sumtype, variant) = resolve_sumtype_ctor(
				&ctx.services.catalog,
				txn,
				Some(source),
				&alias_expr.alias.0,
				ctor,
			)?;
			let given = ctor
				.columns
				.iter()
				.map(|(name, expr)| (name.text().to_lowercase(), expr.clone()))
				.collect();
			push_all_variant_columns(
				source,
				ctx.source.is_none().then_some(source),
				alias_expr.alias.0.text(),
				&sumtype,
				variant,
				given,
				&alias_expr.fragment,
				expanded,
			);
			Ok(())
		}
		Expression::Column(_) => expand_unit_variant_column(ctx, txn, Some(source), alias_expr, expanded),
		_ => {
			expanded.push(alias_expr);
			Ok(())
		}
	}
}

pub(crate) fn resolve_sumtype_ctor(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	source: Option<&ResolvedObject>,
	column: &Fragment,
	ctor: &SumTypeConstructorExpression,
) -> Result<(SumType, usize)> {
	let col_name = column.text();
	let declared = source.and_then(|source| declared_sumtype_id(source, col_name));
	let sumtype = if is_unresolved_ctor(ctor) {
		let Some(id) = declared else {
			return_error!(column_not_sumtype(ctor.variant_name.clone(), col_name));
		};
		catalog.get_sumtype(txn, id)?
	} else {
		let sumtype = find_ctor_sumtype(catalog, txn, ctor)?;
		if let Some(source) = source {
			match declared {
				None if source.columns().iter().any(|c| c.name == col_name) => {
					return_error!(column_not_sumtype(ctor.variant_name.clone(), col_name));
				}
				None => return_error!(column_not_found(column.clone())),
				Some(id) if id != sumtype.id => {
					let declared_name = catalog.get_sumtype(txn, id)?.name;
					return_error!(sumtype_variant_not_found(
						qualified_ctor_fragment(ctor),
						&declared_name
					));
				}
				Some(_) => {}
			}
		}
		sumtype
	};

	let variant_name_lower = ctor.variant_name.text().to_lowercase();
	let Some(variant) = sumtype.variants.iter().position(|v| v.name == variant_name_lower) else {
		return_error!(sumtype_variant_not_found(ctor.variant_name.clone(), &sumtype.name));
	};
	if let Some((unknown, _)) = ctor.columns.iter().find(|(field_name, _)| {
		!sumtype.variants[variant].fields.iter().any(|f| f.name.eq_ignore_ascii_case(field_name.text()))
	}) {
		return_error!(column_not_found(unknown.clone()));
	}

	Ok((sumtype, variant))
}

pub(crate) fn resolve_is_variants(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	source: &ResolvedObject,
	expr: &mut Expression,
) -> Result<()> {
	for_each_is_variant(expr, &mut |is| {
		let column = match is.expression.as_ref() {
			Expression::Column(column) => column.0.name.clone(),
			other => display_label(other),
		};
		let ctor = SumTypeConstructorExpression {
			namespace: is.namespace.clone().unwrap_or_else(|| is.sumtype_name.clone()),
			sumtype_name: is.sumtype_name.clone(),
			variant_name: is.variant_name.clone(),
			columns: Vec::new(),
			fragment: is.fragment.clone(),
		};
		let (sumtype, variant) = resolve_sumtype_ctor(catalog, txn, Some(source), &column, &ctor)?;
		is.tag = Some(sumtype.variants[variant].tag);
		Ok(())
	})
}

#[allow(clippy::too_many_arguments)]
fn push_all_variant_columns(
	source: &ResolvedObject,
	field_types: Option<&ResolvedObject>,
	col_name: &str,
	sumtype: &SumType,
	variant: usize,
	mut given: HashMap<String, Expression>,
	fragment: &Fragment,
	expanded: &mut Vec<AliasExpression>,
) {
	expanded.push(tag_alias(Some(source), col_name, sumtype.variants[variant].tag, fragment));
	for (index, v) in sumtype.variants.iter().enumerate() {
		for field in &v.fields {
			let field_name = field.name.to_lowercase();
			let value = match given.remove(&field_name) {
				Some(expr) if index == variant => expr,
				_ => Expression::Constant(ConstantExpression::None {
					fragment: fragment.clone(),
				}),
			};
			let phys_col_name = format!("{}_{}_{}", col_name, v.name.to_lowercase(), field_name);
			expanded.push(AliasExpression {
				expression: Box::new(cast_to_stored_type(field_types, &phys_col_name, value, fragment)),
				alias: IdentExpression(Fragment::internal(phys_col_name)),
				fragment: fragment.clone(),
			});
		}
	}
}

fn tag_alias(source: Option<&ResolvedObject>, col_name: &str, tag: u8, fragment: &Fragment) -> AliasExpression {
	let tag_col_name = format!("{}_tag", col_name);
	let constant = Expression::Constant(ConstantExpression::Number {
		fragment: Fragment::internal(tag.to_string()),
	});
	AliasExpression {
		expression: Box::new(cast_to_stored_type(source, &tag_col_name, constant, fragment)),
		alias: IdentExpression(Fragment::internal(tag_col_name)),
		fragment: fragment.clone(),
	}
}

fn cast_to_stored_type(
	source: Option<&ResolvedObject>,
	col_name: &str,
	expression: Expression,
	fragment: &Fragment,
) -> Expression {
	match source.and_then(|source| source.columns().iter().find(|c| c.name == col_name)) {
		Some(column) => Expression::Cast(CastExpression {
			fragment: fragment.clone(),
			expression: Box::new(expression),
			to: TypeExpression {
				fragment: Fragment::internal(column.constraint.get_type().to_string()),
				ty: column.constraint.get_type(),
			},
		}),
		None => expression,
	}
}

fn resolve_series_tag_name<'a>(
	ctx: &Arc<QueryContext>,
	txn: &mut Transaction<'a>,
	sumtype: &SumType,
	alias_expr: AliasExpression,
) -> Result<AliasExpression> {
	let variant = match alias_expr.expression.as_ref() {
		Expression::Column(column) => column.0.name.clone(),
		Expression::SumTypeConstructor(ctor) => {
			if let Some((field, _)) = ctor.columns.first() {
				return_error!(column_not_found(field.clone()));
			}
			if !is_unresolved_ctor(ctor)
				&& find_ctor_sumtype(&ctx.services.catalog, txn, ctor)?.id != sumtype.id
			{
				return_error!(sumtype_variant_not_found(qualified_ctor_fragment(ctor), &sumtype.name));
			}
			ctor.variant_name.clone()
		}
		_ => return Ok(alias_expr),
	};
	let Some(tag) = sumtype.variants.iter().find(|v| v.name.eq_ignore_ascii_case(variant.text())).map(|v| v.tag)
	else {
		return_error!(sumtype_variant_not_found(variant, &sumtype.name));
	};
	Ok(AliasExpression {
		alias: alias_expr.alias,
		expression: Box::new(Expression::Constant(ConstantExpression::Number {
			fragment: Fragment::internal(tag.to_string()),
		})),
		fragment: alias_expr.fragment,
	})
}

fn is_unresolved_ctor(ctor: &SumTypeConstructorExpression) -> bool {
	ctor.namespace.text() == ctor.variant_name.text() && ctor.sumtype_name.text() == ctor.variant_name.text()
}

fn qualified_ctor_fragment(ctor: &SumTypeConstructorExpression) -> Fragment {
	ctor.namespace.with_text(format!(
		"{}::{}::{}",
		ctor.namespace.text(),
		ctor.sumtype_name.text(),
		ctor.variant_name.text()
	))
}

fn find_ctor_sumtype(
	catalog: &Catalog,
	txn: &mut Transaction<'_>,
	ctor: &SumTypeConstructorExpression,
) -> Result<SumType> {
	let ns_name = ctor.namespace.text();
	let Some(ns) = catalog.find_namespace_by_name(txn, ns_name)? else {
		return_error!(namespace_not_found(ctor.namespace.clone(), ns_name));
	};
	let sumtype_name = ctor.sumtype_name.text();
	let Some(sumtype) = catalog.find_sumtype_by_name(txn, ns.id(), sumtype_name)? else {
		return_error!(sumtype_not_found(ctor.sumtype_name.clone(), ns_name, sumtype_name));
	};
	Ok(sumtype)
}

fn declared_sumtype_id(source: &ResolvedObject, col_name: &str) -> Option<SumTypeId> {
	let tag_col_name = format!("{}_tag", col_name);
	let tag_col = source.columns().iter().find(|c| c.name == tag_col_name)?;
	match tag_col.constraint.constraint() {
		Some(Constraint::SumType(id)) => Some(*id),
		_ => None,
	}
}

fn expand_unit_variant_column(
	ctx: &QueryContext,
	txn: &mut Transaction<'_>,
	source: Option<&ResolvedObject>,
	alias_expr: AliasExpression,
	expanded: &mut Vec<AliasExpression>,
) -> Result<()> {
	let (Some(source), Expression::Column(col)) = (source, alias_expr.expression.as_ref()) else {
		expanded.push(alias_expr);
		return Ok(());
	};
	let Some((sumtype, variant)) = try_resolve_unit_variant(
		ctx,
		txn,
		source,
		alias_expr.alias.0.text(),
		col.0.name.text(),
		alias_expr.expression.full_fragment_owned(),
	)?
	else {
		expanded.push(alias_expr);
		return Ok(());
	};

	push_all_variant_columns(
		source,
		ctx.source.is_none().then_some(source),
		alias_expr.alias.0.text(),
		&sumtype,
		variant,
		HashMap::new(),
		&alias_expr.fragment,
		expanded,
	);
	Ok(())
}

#[inline]
fn try_resolve_unit_variant(
	ctx: &QueryContext,
	txn: &mut Transaction<'_>,
	source: &ResolvedObject,
	col_name: &str,
	alias_text: &str,
	variant: Fragment,
) -> Result<Option<(SumType, usize)>> {
	let Some(id) = declared_sumtype_id(source, col_name) else {
		return Ok(None);
	};
	let sumtype = ctx.services.catalog.get_sumtype(txn, id)?;
	let variant_name_lower = alias_text.to_lowercase();
	let Some(index) = sumtype.variants.iter().position(|v| v.name.to_lowercase() == variant_name_lower) else {
		return_error!(sumtype_variant_not_found(variant, &sumtype.name));
	};
	Ok(Some((sumtype, index)))
}

impl QueryNode for InlineDataNode {
	#[instrument(level = "trace", skip_all, name = "volcano::inline::initialize")]
	fn initialize<'a>(&mut self, rx: &mut Transaction<'a>, _ctx: &QueryContext) -> Result<()> {
		self.load_series_tag(rx)?;
		self.expand_sumtype_constructors(rx)?;
		Ok(())
	}

	#[instrument(level = "trace", skip_all, name = "volcano::inline::next")]
	fn next<'a>(&mut self, _rx: &mut Transaction<'a>, _ctx: &mut QueryContext) -> Result<Option<Columns>> {
		reifydb_assertions! {
			assert!(self.context.is_some(), "InlineDataNode::next() called before initialize()");
		}
		let stored_ctx = self.context.as_ref().unwrap().clone();

		if self.executed {
			return Ok(None);
		}

		self.executed = true;

		if self.rows.is_empty() {
			let columns = Columns::empty();
			if self.headers.is_none() {
				self.headers = Some(ColumnHeaders::from_columns(&columns));
			}
			return Ok(Some(columns));
		}

		if self.headers.is_some() {
			self.next_with_source(&stored_ctx)
		} else {
			self.next_infer_namespace(&stored_ctx)
		}
	}

	fn headers(&self) -> Option<ColumnHeaders> {
		self.headers.clone()
	}
}

type EvaluatedColumnValues = (Vec<(Value, Fragment)>, Option<ValueType>, Option<Fragment>);

impl InlineDataNode {
	fn find_optimal_integer_type(column: &ColumnBuffer) -> ValueType {
		let mut min_val = i128::MAX;
		let mut max_val = i128::MIN;
		let mut has_values = false;

		for value in column.iter() {
			match value {
				Value::Int16(v) => {
					has_values = true;
					min_val = min_val.min(v);
					max_val = max_val.max(v);
				}
				Value::None {
					..
				} => {}
				_ => {
					return ValueType::Int16;
				}
			}
		}

		if !has_values {
			return ValueType::Int1;
		}

		if min_val >= i8::MIN as i128 && max_val <= i8::MAX as i128 {
			ValueType::Int1
		} else if min_val >= i16::MIN as i128 && max_val <= i16::MAX as i128 {
			ValueType::Int2
		} else if min_val >= i32::MIN as i128 && max_val <= i32::MAX as i128 {
			ValueType::Int4
		} else if min_val >= i64::MIN as i128 && max_val <= i64::MAX as i128 {
			ValueType::Int8
		} else {
			ValueType::Int16
		}
	}

	fn widen_numeric(wide: ValueType, fractional: ValueType) -> ValueType {
		match wide {
			ValueType::Int | ValueType::Uint if fractional.is_floating_point() => ValueType::Float8,
			_ if wide.is_number() => ValueType::promote(wide, fractional),
			_ => wide,
		}
	}

	#[instrument(level = "trace", skip_all, name = "volcano::inline::column_names")]
	fn collect_column_names(rows: &[Vec<AliasExpression>]) -> BTreeSet<String> {
		let mut all_columns: BTreeSet<String> = BTreeSet::new();

		for row in rows {
			for keyed_expr in row {
				let column_name = keyed_expr.alias.0.text().to_string();
				all_columns.insert(column_name);
			}
		}

		all_columns
	}

	#[instrument(level = "trace", skip_all, name = "volcano::inline::row_maps")]
	fn build_row_maps(rows: &[Vec<AliasExpression>]) -> Vec<HashMap<String, &AliasExpression>> {
		let mut rows_data: Vec<HashMap<String, &AliasExpression>> = Vec::new();

		for row in rows {
			let mut row_map: HashMap<String, &AliasExpression> = HashMap::new();
			for alias_expr in row {
				let column_name = alias_expr.alias.0.text().to_string();
				row_map.insert(column_name, alias_expr);
			}
			rows_data.push(row_map);
		}

		rows_data
	}

	#[instrument(level = "trace", skip_all, name = "volcano::inline::eval")]
	fn eval_column_values(
		session: &EvalContext<'_>,
		rows_data: &[HashMap<String, &AliasExpression>],
		column_name: &str,
	) -> Result<EvaluatedColumnValues> {
		let mut all_values = Vec::new();
		let mut first_value_type: Option<ValueType> = None;
		let mut column_fragment: Option<Fragment> = None;

		for row_data in rows_data {
			if let Some(alias_expr) = row_data.get(column_name) {
				if column_fragment.is_none() {
					column_fragment = Some(alias_expr.fragment.clone());
				}
				let eval_ctx = session.with_eval_empty();

				let evaluated = evaluate(&eval_ctx, &alias_expr.expression)?;

				let mut iter = evaluated.data().iter();
				if let Some(value) = iter.next() {
					if first_value_type.is_none() && !matches!(value, Value::None { .. }) {
						first_value_type = Some(value.get_type());
					}
					all_values.push((value, alias_expr.expression.full_fragment_owned()));
				} else {
					all_values.push((Value::none(), Fragment::none()));
				}
			} else {
				all_values.push((Value::none(), Fragment::none()));
			}
		}

		Ok((all_values, first_value_type, column_fragment))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::inline::materialize")]
	fn materialize_inferred_column(
		session: &EvalContext<'_>,
		all_values: &[(Value, Fragment)],
		first_value_type: Option<ValueType>,
	) -> Result<ColumnBuffer> {
		let wide_type = first_value_type.map(|fvt| {
			let first = match fvt {
				ValueType::Int | ValueType::Uint => fvt,
				_ if fvt.is_integer() => ValueType::Int16,
				_ if fvt.is_floating_point() => ValueType::Float8,
				_ => fvt,
			};
			all_values
				.iter()
				.filter(|(value, _)| !matches!(value, Value::None { .. }))
				.map(|(value, _)| value.get_type())
				.filter(|ty| ty.is_floating_point() || *ty == ValueType::Decimal)
				.fold(first, Self::widen_numeric)
		});

		let mut column_data = if wide_type.is_none() {
			let none_type = all_values
				.iter()
				.find_map(|(value, _)| match value {
					Value::None {
						inner,
					} if *inner != ValueType::Any => Some(inner.clone()),
					_ => None,
				})
				.unwrap_or(ValueType::Boolean);
			ColumnBuffer::none_typed(none_type, all_values.len())
		} else {
			let mut data = ColumnBuffer::with_capacity(wide_type.clone().unwrap(), 0);

			for (value, fragment) in all_values {
				if matches!(value, Value::None { .. }) {
					data.push_none();
				} else if wide_type.as_ref().is_some_and(|wt| value.get_type() == *wt) {
					data.push_value(value.clone());
				} else {
					let temp_data = ColumnBuffer::from(value.clone());
					let eval_ctx = session.with_eval_empty();

					let casted = cast_column_data(
						&eval_ctx,
						&temp_data,
						wide_type.clone().unwrap(),
						fragment,
					)?;
					if let Some(casted_value) = casted.iter().next() {
						data.push_value(casted_value);
					} else {
						data.push_none();
					}
				}
			}

			data
		};

		if wide_type == Some(ValueType::Int16) {
			let optimal_type = Self::find_optimal_integer_type(&column_data);
			if optimal_type != ValueType::Int16 {
				let eval_ctx = session.with_eval(Columns::empty(), column_data.len());
				column_data = cast_column_data(&eval_ctx, &column_data, optimal_type, Fragment::none)?;
			}
		}

		Ok(column_data)
	}

	fn next_infer_namespace(&mut self, ctx: &QueryContext) -> Result<Option<Columns>> {
		let all_columns = Self::collect_column_names(&self.rows);
		let rows_data = Self::build_row_maps(&self.rows);

		let session = eval_context_from_query(ctx);

		let mut columns = Vec::new();

		for column_name in all_columns {
			let (all_values, first_value_type, column_fragment) =
				Self::eval_column_values(&session, &rows_data, &column_name)?;

			let column_data = Self::materialize_inferred_column(&session, &all_values, first_value_type)?;

			columns.push(ColumnWithName::new(
				column_fragment
					.map(|f| f.with_text(&column_name))
					.unwrap_or_else(|| Fragment::internal(column_name)),
				column_data,
			));
		}

		let columns = Columns::new(columns);
		self.headers = Some(ColumnHeaders::from_columns(&columns));

		Ok(Some(columns))
	}

	fn next_with_source(&mut self, ctx: &QueryContext) -> Result<Option<Columns>> {
		let source = ctx.source.as_ref().unwrap();
		let headers = self.headers.as_ref().unwrap();
		let session = eval_context_from_query(ctx);

		if let Some(unknown) = self
			.rows
			.iter()
			.flatten()
			.find(|alias| !headers.columns.iter().any(|c| c.text() == alias.alias.0.text()))
		{
			return_error!(column_not_found(unknown.fragment.clone()));
		}

		let rows_data = Self::build_row_maps(&self.rows);

		let mut columns = Vec::new();

		for column_name in &headers.columns {
			columns.push(Self::build_source_column(
				&session,
				source,
				self.series_tag.as_ref(),
				&rows_data,
				column_name,
			)?);
		}

		let columns = Columns::new(columns);

		Ok(Some(columns))
	}

	#[instrument(level = "trace", skip_all, name = "volcano::inline::source_column")]
	fn build_source_column(
		session: &EvalContext<'_>,
		source: &ResolvedObject,
		series_tag: Option<&SumType>,
		rows_data: &[HashMap<String, &AliasExpression>],
		column_name: &Fragment,
	) -> Result<ColumnWithName> {
		let table_column = source.columns().iter().find(|col| col.name == column_name.text());

		let mut column_data = if let Some(tc) = table_column {
			ColumnBuffer::none_typed(tc.constraint.get_type(), 0)
		} else {
			ColumnBuffer::with_capacity(ValueType::Uint1, 0)
		};
		let mut column_fragment: Option<Fragment> = None;

		for row_data in rows_data {
			if let Some(alias_expr) = row_data.get(column_name.text()) {
				if column_fragment.is_none() {
					column_fragment = Some(alias_expr.fragment.clone());
				}
				let mut eval_ctx = session.with_eval_empty();
				eval_ctx.target = table_column.map(|tc| TargetColumn::Partial {
					source_name: Some(source.identifier().text().to_string()),
					column_name: Some(tc.name.clone()),
					column_type: tc.constraint.get_type(),
					properties: tc.properties.iter().map(|cp| cp.property.clone()).collect(),
				});

				let evaluated = evaluate(&eval_ctx, &alias_expr.expression)?;

				let eval_len = evaluated.data().len();
				if table_column.is_some() {
					if eval_len == 1 {
						column_data.extend(evaluated.data().clone())?;
					} else if eval_len == 0 {
						column_data.push_value(Value::none());
					} else {
						let first_value =
							evaluated.data().iter().next().unwrap_or(Value::none());
						column_data.push_value(first_value);
					}
				} else {
					let value = if eval_len > 0 {
						evaluated.data().iter().next().unwrap_or(Value::none())
					} else {
						Value::none()
					};
					if matches!(value, Value::None { .. }) {
						column_data.push_none();
					} else {
						let sumtype = series_tag.expect(
							"the only inline column outside the schema is the series tag",
						);
						column_data.push_value(Value::Uint1(resolve_variant_tag(
							sumtype,
							&value,
							alias_expr.expression.full_fragment_owned(),
						)?));
					}
				}
			} else {
				column_data.push_value(Value::none());
			}
		}

		Ok(ColumnWithName::new(
			column_fragment.map(|f| f.with_text(column_name.text())).unwrap_or_else(|| column_name.clone()),
			column_data,
		))
	}
}
