// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem::discriminant, slice::from_ref, str::FromStr, sync::OnceLock};

use arrow_array::{Array, ArrayRef, UInt32Array};
use arrow_schema::FieldRef;
use arrow_select::take::take;
use reifydb_core::{
	error::{
		CoreError,
		diagnostic::catalog::{variant_enum_not_known, variant_in_expression},
	},
	expression::{Expression, VariableExpression, name::display_label},
	value::{
		batch::{is_scalar, scalar_value},
		column::{
			builder::ColumnBuilder,
			cast::{cast_column_data, error::CastError},
			factory::{self, rename},
		},
	},
};
use reifydb_value::{
	error::{BinaryOp, Error, IntoDiagnostic, LogicalOp, RuntimeErrorKind, TypeError},
	fragment::Fragment,
	return_error,
	value::{
		Value,
		column_view::{ColumnView, ViewData},
		constraint::{precision::Precision, scale::Scale},
		system_columns::{resolve_column, user_columns},
		value_type::ValueType,
	},
};

use super::{
	context::CompileContext,
	option::{binary_op_unwrap_option, unary_op_unwrap_option},
};
use crate::{
	Result,
	expression::{
		access::access_lookup,
		arith::{add::add_columns, div::div_columns, mul::mul_columns, rem::rem_columns, sub::sub_columns},
		branch::BranchLayout,
		call::call_builtin,
		compare::{
			Equal, GreaterThan, GreaterThanEqual, LessThan, LessThanEqual, NotEqual, compare_columns,
			is_family, length_mismatch,
		},
		constant::constant_value,
		context::EvalContext,
		logic::{execute_logical_op, try_short_circuit_and, try_short_circuit_or},
		lookup::column_lookup,
		parameter::parameter_lookup,
		prefix::prefix_apply,
	},
	stack::Variable,
};

type SingleExprFn = Box<dyn Fn(&EvalContext) -> Result<(FieldRef, ArrayRef)> + Send + Sync>;
type MultiExprFn = Box<dyn Fn(&EvalContext) -> Result<Vec<(FieldRef, ArrayRef)>> + Send + Sync>;

pub struct CompiledExpr {
	inner: CompiledExprInner,
	access_column_name: Option<String>,
}

enum CompiledExprInner {
	Single(SingleExprFn),
	Multi(MultiExprFn),
}

impl CompiledExpr {
	pub fn new(f: impl Fn(&EvalContext) -> Result<(FieldRef, ArrayRef)> + Send + Sync + 'static) -> Self {
		Self {
			inner: CompiledExprInner::Single(Box::new(f)),
			access_column_name: None,
		}
	}

	pub fn new_multi(
		f: impl Fn(&EvalContext) -> Result<Vec<(FieldRef, ArrayRef)>> + Send + Sync + 'static,
	) -> Self {
		Self {
			inner: CompiledExprInner::Multi(Box::new(f)),
			access_column_name: None,
		}
	}

	pub fn new_access(
		name: String,
		f: impl Fn(&EvalContext) -> Result<(FieldRef, ArrayRef)> + Send + Sync + 'static,
	) -> Self {
		Self {
			inner: CompiledExprInner::Single(Box::new(f)),
			access_column_name: Some(name),
		}
	}

	pub fn access_column_name(&self) -> Option<&str> {
		self.access_column_name.as_deref()
	}

	pub fn execute(&self, ctx: &EvalContext) -> Result<(FieldRef, ArrayRef)> {
		match &self.inner {
			CompiledExprInner::Single(f) => f(ctx),
			CompiledExprInner::Multi(f) => {
				let columns = f(ctx)?;
				if columns.len() == 1 {
					return Ok(columns.into_iter().next().unwrap());
				}
				Err(TypeError::Runtime {
					kind: RuntimeErrorKind::ExpectedSingleColumn {
						actual: columns.len(),
					},
					message: "expression produces more than one column where one is required"
						.to_string(),
				}
				.into())
			}
		}
	}

	pub fn execute_multi(&self, ctx: &EvalContext) -> Result<Vec<(FieldRef, ArrayRef)>> {
		match &self.inner {
			CompiledExprInner::Single(f) => Ok(vec![f(ctx)?]),
			CompiledExprInner::Multi(f) => f(ctx),
		}
	}
}

macro_rules! compile_arith {
	($ctx:expr, $parent:expr, $e:expr, $op_fn:path) => {{
		let left = compile_expression($ctx, &$e.left)?;
		let right = compile_expression($ctx, &$e.right)?;
		let fragment = $e.full_fragment_owned();
		let label = display_label($parent);
		CompiledExpr::new(move |ctx| {
			let l = left.execute(ctx)?;
			let r = right.execute(ctx)?;
			let col = $op_fn(&ctx.arith(), &l, &r, || fragment.clone())?;
			Ok(rename(col, label.text()))
		})
	}};
}

macro_rules! compile_compare {
	($ctx:expr, $parent:expr, $e:expr, $cmp_type:ty, $binary_op:expr) => {{
		let left = compile_expression($ctx, &$e.left)?;
		let right = compile_expression($ctx, &$e.right)?;
		let fragment = $e.full_fragment_owned();
		let label = display_label($parent);
		CompiledExpr::new(move |ctx| {
			let l = left.execute(ctx)?;
			let r = right.execute(ctx)?;
			let col = compare_columns::<$cmp_type>(&l, &r, fragment.clone(), |f, l, r| {
				TypeError::BinaryOperatorNotApplicable {
					operator: $binary_op,
					left: l,
					right: r,
					fragment: f,
				}
				.into_diagnostic()
			})?;
			Ok(rename(col, label.text()))
		})
	}};
}

pub fn compile_expression(_ctx: &CompileContext, expr: &Expression) -> Result<CompiledExpr> {
	Ok(match expr {
		Expression::Constant(e) => {
			let constant = e.clone();
			let label = display_label(expr);
			let scalar = OnceLock::new();
			CompiledExpr::new(move |ctx| {
				let row_count = ctx.take.unwrap_or(ctx.row_count);
				broadcast(&scalar, row_count, |rows| constant_value(&constant, label.text(), rows))
			})
		}

		Expression::Column(e) => {
			let expr = e.clone();
			CompiledExpr::new(move |ctx| column_lookup(ctx, &expr))
		}

		Expression::Variable(e) => {
			let expr = e.clone();
			let scalar = OnceLock::new();
			CompiledExpr::new(move |ctx| {
				broadcast(&scalar, ctx.row_count, |rows| variable_column(ctx, &expr, rows))
			})
		}

		Expression::Parameter(e) => {
			let expr = e.clone();
			let scalar = OnceLock::new();
			CompiledExpr::new(move |ctx| {
				broadcast(&scalar, ctx.row_count, |rows| parameter_lookup(ctx, &expr, rows))
			})
		}

		Expression::Alias(e) => {
			let inner = compile_expression(_ctx, &e.expression)?;
			let alias = e.alias.0.clone();
			CompiledExpr::new(move |ctx| Ok(rename(inner.execute(ctx)?, alias.text())))
		}

		Expression::Add(e) => compile_arith!(_ctx, expr, e, add_columns),
		Expression::Sub(e) => compile_arith!(_ctx, expr, e, sub_columns),
		Expression::Mul(e) => compile_arith!(_ctx, expr, e, mul_columns),
		Expression::Div(e) => compile_arith!(_ctx, expr, e, div_columns),
		Expression::Rem(e) => compile_arith!(_ctx, expr, e, rem_columns),

		Expression::Equal(e) => compile_compare!(_ctx, expr, e, Equal, BinaryOp::Equal),
		Expression::NotEqual(e) => compile_compare!(_ctx, expr, e, NotEqual, BinaryOp::NotEqual),
		Expression::GreaterThan(e) => compile_compare!(_ctx, expr, e, GreaterThan, BinaryOp::GreaterThan),
		Expression::GreaterThanEqual(e) => {
			compile_compare!(_ctx, expr, e, GreaterThanEqual, BinaryOp::GreaterThanEqual)
		}
		Expression::LessThan(e) => compile_compare!(_ctx, expr, e, LessThan, BinaryOp::LessThan),
		Expression::LessThanEqual(e) => compile_compare!(_ctx, expr, e, LessThanEqual, BinaryOp::LessThanEqual),

		Expression::And(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			let label = display_label(expr);
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				if let Some(short) = try_short_circuit_and(&l, &fragment, l.1.len())? {
					return Ok(rename(short, label.text()));
				}
				let r = right.execute(ctx)?;
				let col = execute_logical_op(&l, &r, &fragment, LogicalOp::And)?;
				Ok(rename(col, label.text()))
			})
		}

		Expression::Or(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			let label = display_label(expr);
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				if let Some(short) = try_short_circuit_or(&l, &fragment, l.1.len())? {
					return Ok(rename(short, label.text()));
				}
				let r = right.execute(ctx)?;
				let col = execute_logical_op(&l, &r, &fragment, LogicalOp::Or)?;
				Ok(rename(col, label.text()))
			})
		}

		Expression::Xor(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			let label = display_label(expr);
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				let col = execute_logical_op(&l, &r, &fragment, LogicalOp::Xor)?;
				Ok(rename(col, label.text()))
			})
		}

		Expression::Prefix(e) => {
			let inner = compile_expression(_ctx, &e.expression)?;
			let operator = e.operator.clone();
			let fragment = e.full_fragment_owned();
			let label = display_label(expr);
			CompiledExpr::new(move |ctx| {
				let column = inner.execute(ctx)?;
				let col = prefix_apply(&column, &operator, &fragment)?;
				Ok(rename(col, label.text()))
			})
		}

		Expression::Type(e) => {
			let ty = e.ty.clone();
			let fragment = e.fragment.clone();
			let scalar = OnceLock::new();
			CompiledExpr::new(move |ctx| {
				let row_count = ctx.take.unwrap_or(ctx.row_count);
				broadcast(&scalar, row_count, |rows| Ok(type_column(rows, &ty, &fragment)))
			})
		}

		Expression::AccessSource(e) => {
			let col_name = e.column.name.text().to_string();
			let expr = e.clone();
			CompiledExpr::new_access(col_name, move |ctx| access_lookup(ctx, &expr))
		}

		Expression::Tuple(e) => {
			if e.expressions.len() == 1 {
				let inner = compile_expression(_ctx, &e.expressions[0])?;
				CompiledExpr::new(move |ctx| inner.execute(ctx))
			} else {
				let compiled: Vec<CompiledExpr> = e
					.expressions
					.iter()
					.map(|expr| compile_expression(_ctx, expr))
					.collect::<Result<Vec<_>>>()?;
				let fragment = e.fragment.clone();
				CompiledExpr::new(move |ctx| {
					let columns: Vec<(FieldRef, ArrayRef)> = compiled
						.iter()
						.map(|expr| expr.execute(ctx))
						.collect::<Result<Vec<_>>>()?;
					let views =
						columns.iter().map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;

					let len = columns.first().map_or(1, |c| c.1.len());
					let mut data: Vec<Value> = Vec::with_capacity(len);

					for i in 0..len {
						let items: Vec<Value> =
							views.iter().map(|view| view.get_value(i)).collect();
						data.push(Value::Tuple(items));
					}

					Ok(factory::any(fragment.text(), data))
				})
			}
		}

		Expression::List(e) => {
			let compiled: Vec<CompiledExpr> = e
				.expressions
				.iter()
				.map(|expr| compile_expression(_ctx, expr))
				.collect::<Result<Vec<_>>>()?;
			let fragment = e.fragment.clone();
			CompiledExpr::new(move |ctx| {
				let columns: Vec<(FieldRef, ArrayRef)> =
					compiled.iter().map(|expr| expr.execute(ctx)).collect::<Result<Vec<_>>>()?;
				let views = columns.iter().map(ColumnView::try_from).collect::<Result<Vec<_>>>()?;

				let len = columns.first().map_or(1, |c| c.1.len());
				let mut data: Vec<Value> = Vec::with_capacity(len);

				for i in 0..len {
					let items: Vec<Value> = views.iter().map(|view| view.get_value(i)).collect();
					data.push(Value::List(items));
				}

				Ok(factory::any(fragment.text(), data))
			})
		}

		Expression::Between(e) => {
			let value = compile_expression(_ctx, &e.value)?;
			let lower = compile_expression(_ctx, &e.lower)?;
			let upper = compile_expression(_ctx, &e.upper)?;
			let fragment = e.fragment.clone();
			CompiledExpr::new(move |ctx| {
				let value_col = value.execute(ctx)?;
				let lower_col = lower.execute(ctx)?;
				let upper_col = upper.execute(ctx)?;

				let ge_result = compare_columns::<GreaterThanEqual>(
					&value_col,
					&lower_col,
					fragment.clone(),
					|f, l, r| {
						TypeError::BinaryOperatorNotApplicable {
							operator: BinaryOp::Between,
							left: l,
							right: r,
							fragment: f,
						}
						.into_diagnostic()
					},
				)?;
				let le_result = compare_columns::<LessThanEqual>(
					&value_col,
					&upper_col,
					fragment.clone(),
					|f, l, r| {
						TypeError::BinaryOperatorNotApplicable {
							operator: BinaryOp::Between,
							left: l,
							right: r,
							fragment: f,
						}
						.into_diagnostic()
					},
				)?;

				let (ge_view, le_view) =
					(ColumnView::try_from(&ge_result)?, ColumnView::try_from(&le_result)?);
				if !matches!(ge_view.data, ViewData::Bool(_))
					|| !matches!(le_view.data, ViewData::Bool(_))
					|| ge_result.0.is_nullable()
					|| le_result.0.is_nullable()
				{
					return Err(TypeError::BinaryOperatorNotApplicable {
						operator: BinaryOp::Between,
						left: ColumnView::try_from(&value_col)?.get_type(),
						right: ColumnView::try_from(&lower_col)?.get_type(),
						fragment: fragment.clone(),
					}
					.into());
				}

				match (&ge_view.data, &le_view.data) {
					(ViewData::Bool(ge_container), ViewData::Bool(le_container)) => {
						let mut data = Vec::with_capacity(ge_container.len());
						let mut bitvec = Vec::with_capacity(ge_container.len());

						for i in 0..ge_container.len() {
							if i < ge_container.len() && i < le_container.len() {
								data.push(
									ge_container.value(i) && le_container.value(i)
								);
								bitvec.push(true);
							} else {
								data.push(false);
								bitvec.push(false);
							}
						}

						Ok(factory::bool_with_bitvec(fragment.text(), data, bitvec))
					}
					_ => unreachable!(
						"Both comparison results should be boolean after the check above"
					),
				}
			})
		}

		Expression::In(e) => {
			let list_expressions = match e.list.as_ref() {
				Expression::Tuple(tuple) => &tuple.expressions,
				Expression::List(list) => &list.expressions,
				_ => from_ref(e.list.as_ref()),
			};
			let value = compile_expression(_ctx, &e.value)?;
			let list: Vec<CompiledExpr> = list_expressions
				.iter()
				.map(|expr| compile_expression(_ctx, expr))
				.collect::<Result<Vec<_>>>()?;
			let negated = e.negated;
			let fragment = e.fragment.clone();
			CompiledExpr::new(move |ctx| {
				if list.is_empty() {
					let value_col = value.execute(ctx)?;
					let len = value_col.1.len();
					let result = vec![negated; len];
					return Ok(factory::bool(fragment.text(), result));
				}

				let value_col = value.execute(ctx)?;

				let first_col = list[0].execute(ctx)?;
				let mut result = compare_columns::<Equal>(
					&value_col,
					&first_col,
					fragment.clone(),
					|f, l, r| {
						TypeError::BinaryOperatorNotApplicable {
							operator: BinaryOp::Equal,
							left: l,
							right: r,
							fragment: f,
						}
						.into_diagnostic()
					},
				)?;

				for list_expr in list.iter().skip(1) {
					let list_col = list_expr.execute(ctx)?;
					let eq_result = compare_columns::<Equal>(
						&value_col,
						&list_col,
						fragment.clone(),
						|f, l, r| {
							TypeError::BinaryOperatorNotApplicable {
								operator: BinaryOp::Equal,
								left: l,
								right: r,
								fragment: f,
							}
							.into_diagnostic()
						},
					)?;
					result = execute_logical_op(&result, &eq_result, &fragment, LogicalOp::Or)?;
				}

				if negated {
					result = negate_column(result, fragment.clone());
				}

				Ok(result)
			})
		}

		Expression::Contains(e) => {
			let list_expressions = match e.list.as_ref() {
				Expression::Tuple(tuple) => &tuple.expressions,
				Expression::List(list) => &list.expressions,
				_ => from_ref(e.list.as_ref()),
			};
			let value = compile_expression(_ctx, &e.value)?;
			let list: Vec<CompiledExpr> = list_expressions
				.iter()
				.map(|expr| compile_expression(_ctx, expr))
				.collect::<Result<Vec<_>>>()?;
			let fragment = e.fragment.clone();
			CompiledExpr::new(move |ctx| {
				let value_col = value.execute(ctx)?;

				if list.is_empty() {
					let len = value_col.1.len();
					let result = vec![true; len];
					return Ok(factory::bool(fragment.text(), result));
				}

				let first_col = list[0].execute(ctx)?;
				let mut result = list_contains_element(&value_col, &first_col, &fragment)?;

				for list_expr in list.iter().skip(1) {
					let list_col = list_expr.execute(ctx)?;
					let element_result = list_contains_element(&value_col, &list_col, &fragment)?;
					result = combine_bool_columns(
						result,
						element_result,
						fragment.clone(),
						|l, r| l && r,
					)?;
				}

				Ok(result)
			})
		}

		Expression::Cast(e) => {
			let label = display_label(expr);
			if let Expression::Constant(const_expr) = e.expression.as_ref() {
				let const_expr = const_expr.clone();
				let target_type = e.to.ty.clone();
				let inner_fragment = e.expression.full_fragment_owned();
				let scalar = OnceLock::new();
				CompiledExpr::new(move |ctx| {
					let row_count = ctx.take.unwrap_or(ctx.row_count);
					broadcast(&scalar, row_count, |rows| {
						let data = constant_value(&const_expr, label.text(), rows)?;
						if ColumnView::try_from(&data)?.get_type() == target_type {
							Ok(data)
						} else {
							apply_cast(ctx, &data, &target_type, &inner_fragment)
						}
					})
				})
			} else {
				let inner = compile_expression(_ctx, &e.expression)?;
				let target_type = e.to.ty.clone();
				let inner_fragment = e.expression.full_fragment_owned();
				CompiledExpr::new(move |ctx| {
					let column = inner.execute(ctx)?;
					let casted = apply_cast(ctx, &column, &target_type, &inner_fragment)?;
					Ok(rename(casted, label.text()))
				})
			}
		}

		Expression::If(e) => {
			let condition = compile_expression(_ctx, &e.condition)?;
			let then_expr = compile_expressions(_ctx, from_ref(e.then_expr.as_ref()))?;
			let else_ifs: Vec<(CompiledExpr, Vec<CompiledExpr>)> = e
				.else_ifs
				.iter()
				.map(|ei| {
					Ok((
						compile_expression(_ctx, &ei.condition)?,
						compile_expressions(_ctx, from_ref(ei.then_expr.as_ref()))?,
					))
				})
				.collect::<Result<Vec<_>>>()?;
			let else_branch: Option<Vec<CompiledExpr>> = match &e.else_expr {
				Some(expr) => Some(compile_expressions(_ctx, from_ref(expr.as_ref()))?),
				None => None,
			};
			let fragment = e.fragment.clone();
			CompiledExpr::new_multi(move |ctx| {
				execute_if_multi(ctx, &condition, &then_expr, &else_ifs, &else_branch, &fragment)
			})
		}

		Expression::Map(e) => {
			let expressions = compile_expressions(_ctx, &e.expressions)?;
			CompiledExpr::new_multi(move |ctx| execute_projection_multi(ctx, &expressions))
		}

		Expression::Extend(e) => {
			let expressions = compile_expressions(_ctx, &e.expressions)?;
			CompiledExpr::new_multi(move |ctx| execute_projection_multi(ctx, &expressions))
		}

		Expression::Call(e) => {
			let compiled_args: Vec<CompiledExpr> =
				e.args.iter().map(|arg| compile_expression(_ctx, arg)).collect::<Result<Vec<_>>>()?;
			let type_named_args: Vec<Option<(ValueType, Fragment)>> =
				e.args.iter()
					.map(|arg| match arg {
						Expression::Column(column) => ValueType::from_str(column.0.name.text())
							.ok()
							.map(|ty| (ty, column.0.name.clone())),
						_ => None,
					})
					.collect();
			let label = display_label(expr);
			let resolved = OnceLock::new();
			let expr = e.clone();
			CompiledExpr::new(move |ctx| {
				let function = match resolved.get() {
					Some(function) => Some(function),
					None => ctx
						.routines
						.get_function(expr.func.0.text())
						.map(|function| resolved.get_or_init(|| function)),
				};
				if let Some(function) = function {
					function.arity().check(&expr.func.0, compiled_args.len())?;
				}
				let type_positions =
					function.map_or(&[][..], |function| function.type_argument_positions());
				let mut arg_columns = Vec::with_capacity(compiled_args.len());
				for (index, compiled_arg) in compiled_args.iter().enumerate() {
					match &type_named_args[index] {
						Some((ty, fragment)) if type_positions.contains(&index) => {
							arg_columns.push(type_column(
								ctx.take.unwrap_or(ctx.row_count),
								ty,
								fragment,
							));
						}
						_ => arg_columns.push(compiled_arg.execute(ctx)?),
					}
				}
				call_builtin(ctx, &expr, function, label.text(), &arg_columns)
			})
		}

		Expression::SumTypeConstructor(ctor) => {
			return_error!(variant_in_expression(ctor.variant_name.clone()));
		}

		Expression::IsVariant(e) => {
			let col_name = match e.expression.as_ref() {
				Expression::Column(c) => c.0.name.text().to_string(),
				other => display_label(other).text().to_string(),
			};
			let tag_col_name = format!("{}_tag", col_name);
			let Some(tag) = e.tag else {
				return_error!(variant_enum_not_known(e.variant_name.clone(), &col_name));
			};
			let fragment = e.fragment.clone();
			CompiledExpr::new(move |ctx| {
				if let Some(index) = resolve_column(&ctx.batch, &tag_col_name) {
					let tag_field = ctx.batch.schema_ref().field(index);
					match &ColumnView::try_from((ctx.batch.column(index), tag_field))?.data {
						ViewData::Uint1(container) if !tag_field.is_nullable() => {
							let results: Vec<bool> = container
								.iter()
								.take(ctx.row_count)
								.map(|v| v == Some(tag))
								.collect();
							Ok(factory::bool(fragment.text(), results))
						}
						_ => Ok(factory::none_typed(
							fragment.text(),
							ValueType::Boolean,
							ctx.row_count,
						)),
					}
				} else {
					Ok(factory::none_typed(fragment.text(), ValueType::Boolean, ctx.row_count))
				}
			})
		}

		Expression::FieldAccess(e) => {
			let field_name = e.field.text().to_string();

			let variable = match e.object.as_ref() {
				Expression::Variable(var_expr) => Some(var_expr.clone()),
				_ => None,
			};
			let object = compile_expression(_ctx, &e.object)?;
			CompiledExpr::new(move |ctx| {
				if let Some(ref variable) = variable {
					variable_field_column(
						ctx,
						variable,
						&field_name,
						ctx.take.unwrap_or(ctx.row_count),
					)
				} else {
					let _obj_col = object.execute(ctx)?;
					Err(TypeError::Runtime {
						kind: RuntimeErrorKind::FieldNotFound {
							variable: "<expression>".to_string(),
							field: field_name.to_string(),
							available: vec![],
						},
						message: format!(
							"Field '{}' not found on variable '<expression>'",
							field_name
						),
					}
					.into())
				}
			})
		}
	})
}

fn compile_expressions(ctx: &CompileContext, exprs: &[Expression]) -> Result<Vec<CompiledExpr>> {
	exprs.iter().map(|e| compile_expression(ctx, e)).collect()
}

fn broadcast(
	scalar: &OnceLock<(FieldRef, ArrayRef)>,
	row_count: usize,
	compute: impl FnOnce(usize) -> Result<(FieldRef, ArrayRef)>,
) -> Result<(FieldRef, ArrayRef)> {
	if row_count == 0 {
		return compute(0);
	}
	let (field, array) = match scalar.get() {
		Some(scalar) => scalar,
		None => {
			let computed = compute(1)?;
			scalar.get_or_init(|| computed)
		}
	};
	let repeated = take(array.as_ref(), &UInt32Array::from_value(0, row_count), None).map_err(|err| {
		CoreError::FrameError {
			message: err.to_string(),
		}
	})?;
	Ok((field.clone(), repeated))
}

pub(crate) fn variable_column(
	ctx: &EvalContext,
	expr: &VariableExpression,
	row_count: usize,
) -> Result<(FieldRef, ArrayRef)> {
	let variable_name = expr.name();

	if variable_name == "env" {
		return Err(TypeError::Runtime {
			kind: RuntimeErrorKind::VariableIsDataframe {
				name: variable_name.to_string(),
			},
			message: format!(
				"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
				variable_name
			),
		}
		.into());
	}

	match ctx.symbols.get(variable_name) {
		Some(Variable::Columns {
			batch,
		}) if is_scalar(batch) => {
			let value = match scalar_value(batch)? {
				Value::Any(inner)
					if matches!(
						inner.as_ref(),
						Value::List(items)
							if items.iter().all(|v| matches!(v, Value::Record(_)))
					) =>
				{
					*inner
				}
				other => other,
			};
			let mut data = ColumnBuilder::with_capacity(value.get_type(), row_count);
			for _ in 0..row_count {
				data.push_value(value.clone());
			}
			Ok(data.finish(variable_name))
		}
		Some(Variable::Columns {
			..
		})
		| Some(Variable::ForIterator {
			..
		})
		| Some(Variable::Closure(_)) => Err(TypeError::Runtime {
			kind: RuntimeErrorKind::VariableIsDataframe {
				name: variable_name.to_string(),
			},
			message: format!(
				"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
				variable_name
			),
		}
		.into()),
		None => {
			if let Some(value) = ctx.params.get_named(variable_name) {
				let mut data = ColumnBuilder::with_capacity(value.get_type(), row_count);
				for _ in 0..row_count {
					data.push_value(value.clone());
				}
				return Ok(data.finish(variable_name));
			}
			Err(TypeError::Runtime {
				kind: RuntimeErrorKind::VariableNotFound {
					fragment: expr.fragment.clone(),
				},
				message: format!("Variable '{}' is not defined", variable_name),
			}
			.into())
		}
	}
}

pub(crate) fn variable_field_column(
	ctx: &EvalContext,
	variable: &VariableExpression,
	field_name: &str,
	row_count: usize,
) -> Result<(FieldRef, ArrayRef)> {
	let variable_name = variable.name();
	match ctx.symbols.get(variable_name) {
		Some(Variable::Columns {
			batch,
		}) if !is_scalar(batch) => {
			let found = user_columns(batch).find(|(field, _)| field.name() == field_name);
			match found {
				Some((field, array)) => {
					let value = ColumnView::try_from((array, field.as_ref()))?.get_value(0);
					let mut data = ColumnBuilder::with_capacity(value.get_type(), row_count);
					for _ in 0..row_count {
						data.push_value(value.clone());
					}
					Ok(data.finish(field_name))
				}
				None => {
					let available: Vec<String> = user_columns(batch)
						.map(|(field, _)| field.name().to_string())
						.collect();
					Err(TypeError::Runtime {
						kind: RuntimeErrorKind::FieldNotFound {
							variable: variable_name.to_string(),
							field: field_name.to_string(),
							available,
						},
						message: format!(
							"Field '{}' not found on variable '{}'",
							field_name, variable_name
						),
					}
					.into())
				}
			}
		}
		Some(Variable::Columns {
			..
		})
		| Some(Variable::Closure(_)) => Err(TypeError::Runtime {
			kind: RuntimeErrorKind::FieldNotFound {
				variable: variable_name.to_string(),
				field: field_name.to_string(),
				available: vec![],
			},
			message: format!("Field '{}' not found on variable '{}'", field_name, variable_name),
		}
		.into()),
		Some(Variable::ForIterator {
			..
		}) => Err(TypeError::Runtime {
			kind: RuntimeErrorKind::VariableIsDataframe {
				name: variable_name.to_string(),
			},
			message: format!(
				"Variable '{}' contains a dataframe and cannot be used directly in scalar expressions",
				variable_name
			),
		}
		.into()),
		None => Err(TypeError::Runtime {
			kind: RuntimeErrorKind::VariableNotFound {
				fragment: variable.fragment.clone(),
			},
			message: format!("Variable '{}' is not defined", variable_name),
		}
		.into()),
	}
}

pub(crate) fn type_column(row_count: usize, ty: &ValueType, fragment: &Fragment) -> (FieldRef, ArrayRef) {
	let values: Vec<Value> = (0..row_count).map(|_| Value::Type(ty.clone())).collect();
	factory::any(fragment.text(), values)
}

fn combine_bool_columns(
	left: (FieldRef, ArrayRef),
	right: (FieldRef, ArrayRef),
	fragment: Fragment,
	combine_fn: fn(bool, bool) -> bool,
) -> Result<(FieldRef, ArrayRef)> {
	if left.1.len() != right.1.len() {
		return Err(length_mismatch(left.1.len(), right.1.len(), &fragment));
	}

	binary_op_unwrap_option(&left, &right, fragment.clone(), |left, right| {
		let (left, right) = (ColumnView::try_from(left)?, ColumnView::try_from(right)?);
		match (&left.data, &right.data) {
			(ViewData::Bool(l), ViewData::Bool(r)) => {
				let len = l.len();
				let mut data = Vec::with_capacity(len);
				let mut bitvec = Vec::with_capacity(len);

				for i in 0..len {
					data.push(combine_fn(l.value(i), r.value(i)));
					bitvec.push(true);
				}

				Ok(factory::bool_with_bitvec(fragment.text(), data, bitvec))
			}
			_ => {
				unreachable!("combine_bool_columns should only be called with boolean columns")
			}
		}
	})
}

fn list_items_contain(items: &[Value], element: &Value, fragment: &Fragment) -> bool {
	if items.iter().any(|item| item == element) {
		return true;
	}
	if items.is_empty() {
		return false;
	}

	if let Some(items_col) = build_homogeneous_buffer(items, fragment.text()) {
		let elems_col = factory::from_many(fragment.text(), element.clone(), items.len());
		return compare_columns::<Equal>(&items_col, &elems_col, fragment.clone(), |f, l, r| {
			TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::Equal,
				left: l,
				right: r,
				fragment: f,
			}
			.into_diagnostic()
		})
		.and_then(|c| bool_column_has_true(&c))
		.unwrap_or(false);
	}

	list_items_contain_per_item(items, element, fragment)
}

fn list_items_contain_per_item(items: &[Value], element: &Value, fragment: &Fragment) -> bool {
	items.iter().any(|item| {
		let item_col = factory::from_one(fragment.text(), item.clone());
		let elem_col = factory::from_one(fragment.text(), element.clone());
		compare_columns::<Equal>(&item_col, &elem_col, fragment.clone(), |f, l, r| {
			TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::Equal,
				left: l,
				right: r,
				fragment: f,
			}
			.into_diagnostic()
		})
		.and_then(|c| {
			let view = ColumnView::try_from(&c)?;
			Ok(match &view.data {
				ViewData::Bool(b) if view.logical_nulls().is_none() => b.value(0),
				_ => false,
			})
		})
		.unwrap_or(false)
	})
}

fn bool_column_has_true(col: &(FieldRef, ArrayRef)) -> Result<bool> {
	let view = ColumnView::try_from(col)?;
	Ok(match &view.data {
		ViewData::Bool(b) => match view.logical_nulls() {
			Some(nulls) => (nulls.inner() & b.values()).has_true(),
			None => b.values().has_true(),
		},
		_ => false,
	})
}

fn build_homogeneous_buffer(items: &[Value], name: &str) -> Option<(FieldRef, ArrayRef)> {
	let first = items.first()?;
	let first_disc = discriminant(first);
	if !items.iter().all(|v| discriminant(v) == first_disc) {
		return None;
	}

	macro_rules! collect {
		($variant:ident, $constructor:ident $([$($arg:expr),*])?, |$x:ident| $convert:expr) => {{
			let data: Vec<_> = items
				.iter()
				.map(|v| match v {
					Value::$variant($x) => $convert,
					_ => unreachable!("homogeneous check guarantees variant"),
				})
				.collect();
			Some(factory::$constructor(name, $($($arg,)*)? data))
		}};
	}

	match first {
		Value::Boolean(_) => collect!(Boolean, bool, |x| *x),
		Value::Float4(_) => collect!(Float4, float4, |x| x.value()),
		Value::Float8(_) => collect!(Float8, float8, |x| x.value()),
		Value::Int1(_) => collect!(Int1, int1, |x| *x),
		Value::Int2(_) => collect!(Int2, int2, |x| *x),
		Value::Int4(_) => collect!(Int4, int4, |x| *x),
		Value::Int8(_) => collect!(Int8, int8, |x| *x),
		Value::Int16(_) => collect!(Int16, int16, |x| *x),
		Value::Uint1(_) => collect!(Uint1, uint1, |x| *x),
		Value::Uint2(_) => collect!(Uint2, uint2, |x| *x),
		Value::Uint4(_) => collect!(Uint4, uint4, |x| *x),
		Value::Uint8(_) => collect!(Uint8, uint8, |x| *x),
		Value::Uint16(_) => collect!(Uint16, uint16, |x| *x),
		Value::Utf8(_) => collect!(Utf8, utf8, |x| x.clone()),
		Value::Date(_) => collect!(Date, date, |x| *x),
		Value::DateTime(_) => collect!(DateTime, datetime, |x| *x),
		Value::Time(_) => collect!(Time, time, |x| *x),
		Value::Duration(_) => collect!(Duration, duration, |x| *x),
		Value::Uuid4(_) => collect!(Uuid4, uuid4, |x| *x),
		Value::Uuid7(_) => collect!(Uuid7, uuid7, |x| *x),
		Value::IdentityId(_) => collect!(IdentityId, identity_id, |x| *x),
		Value::Blob(_) => collect!(Blob, blob, |x| x.clone()),
		Value::Decimal(_) => {
			let scale = items
				.iter()
				.filter_map(|v| match v {
					Value::Decimal(d) => Some(d.scale()),
					_ => None,
				})
				.max()?;
			if !items.iter().all(|v| matches!(v, Value::Decimal(d) if d.rescale(scale).is_some())) {
				return None;
			}
			collect!(Decimal, decimal[Precision::MAX, Scale::new(scale)], |x| x.clone())
		}
		Value::DictionaryId(_) => collect!(DictionaryId, dictionary_id, |x| *x),

		_ => None,
	}
}

fn list_contains_element(
	list_col: &(FieldRef, ArrayRef),
	element_col: &(FieldRef, ArrayRef),
	fragment: &Fragment,
) -> Result<(FieldRef, ArrayRef)> {
	let (list_view, element_view) = (ColumnView::try_from(list_col)?, ColumnView::try_from(element_col)?);
	let len = list_col.1.len();
	let mut data = Vec::with_capacity(len);

	for i in 0..len {
		let list_value = list_view.get_value(i);
		let element_value = element_view.get_value(i);

		let contained = match &list_value {
			Value::List(items) => list_items_contain(items, &element_value, fragment),
			Value::Tuple(items) => list_items_contain(items, &element_value, fragment),
			Value::Any(boxed) => match boxed.as_ref() {
				Value::List(items) => list_items_contain(items, &element_value, fragment),
				Value::Tuple(items) => list_items_contain(items, &element_value, fragment),
				_ => false,
			},
			_ => false,
		};
		data.push(contained);
	}

	Ok(factory::bool(fragment.text(), data))
}

fn negate_column(col: (FieldRef, ArrayRef), fragment: Fragment) -> (FieldRef, ArrayRef) {
	unary_op_unwrap_option(&col, |col| match &ColumnView::try_from(col)?.data {
		ViewData::Bool(container) => {
			let len = container.len();
			let mut data = Vec::with_capacity(len);
			let mut bitvec = Vec::with_capacity(len);

			for i in 0..len {
				data.push(!container.value(i));
				bitvec.push(true);
			}

			Ok(factory::bool_with_bitvec(fragment.text(), data, bitvec))
		}
		_ => unreachable!("negate_column should only be called with boolean columns"),
	})
	.unwrap()
}

fn is_truthy(value: &Value) -> bool {
	match value {
		Value::Boolean(true) => true,
		Value::Boolean(false) => false,
		Value::None {
			..
		} => false,
		Value::Int1(0) | Value::Int2(0) | Value::Int4(0) | Value::Int8(0) | Value::Int16(0) => false,
		Value::Uint1(0) | Value::Uint2(0) | Value::Uint4(0) | Value::Uint8(0) | Value::Uint16(0) => false,
		Value::Int1(_) | Value::Int2(_) | Value::Int4(_) | Value::Int8(_) | Value::Int16(_) => true,
		Value::Uint1(_) | Value::Uint2(_) | Value::Uint4(_) | Value::Uint8(_) | Value::Uint16(_) => true,
		Value::Utf8(s) => !s.is_empty(),
		_ => true,
	}
}

fn execute_if_multi(
	ctx: &EvalContext,
	condition: &CompiledExpr,
	then_expr: &[CompiledExpr],
	else_ifs: &[(CompiledExpr, Vec<CompiledExpr>)],
	else_branch: &Option<Vec<CompiledExpr>>,
	_fragment: &Fragment,
) -> Result<Vec<(FieldRef, ArrayRef)>> {
	const NO_BRANCH: usize = usize::MAX;

	let condition_column = condition.execute(ctx)?;
	let condition_view = ColumnView::try_from(&condition_column)?;

	let else_index = else_ifs.len() + 1;
	let mut selection: Vec<usize> = Vec::with_capacity(ctx.row_count);
	let mut unresolved: Vec<usize> = Vec::new();

	for row_idx in 0..ctx.row_count {
		if is_truthy(&condition_view.get_value(row_idx)) {
			selection.push(0);
		} else {
			selection.push(NO_BRANCH);
			unresolved.push(row_idx);
		}
	}

	for (offset, (else_if_condition, _)) in else_ifs.iter().enumerate() {
		if unresolved.is_empty() {
			break;
		}
		let else_if_column = else_if_condition.execute(ctx)?;
		let else_if_view = ColumnView::try_from(&else_if_column)?;
		unresolved.retain(|&row_idx| {
			if is_truthy(&else_if_view.get_value(row_idx)) {
				selection[row_idx] = offset + 1;
				false
			} else {
				true
			}
		});
	}

	if else_branch.is_some() {
		for &row_idx in &unresolved {
			selection[row_idx] = else_index;
		}
	}

	let mut evaluated: Vec<Option<Vec<(FieldRef, ArrayRef)>>> = (0..=else_index).map(|_| None).collect();
	for &branch in &selection {
		if branch == NO_BRANCH || evaluated[branch].is_some() {
			continue;
		}
		let columns = if branch == 0 {
			execute_multi_exprs(ctx, then_expr)?
		} else if branch < else_index {
			execute_multi_exprs(ctx, &else_ifs[branch - 1].1)?
		} else {
			execute_multi_exprs(ctx, else_branch.as_ref().unwrap())?
		};
		evaluated[branch] = Some(columns);
	}

	let mut layout: Option<BranchLayout> = None;
	for columns in evaluated.iter().flatten() {
		let named_types = columns
			.iter()
			.map(|col| {
				let view = ColumnView::try_from(col)?;
				Ok((col.0.name().as_str(), view.get_type(), view.is_untyped_none()))
			})
			.collect::<Result<Vec<_>>>()?;
		let Some(expected) = layout.as_mut() else {
			layout = Some(BranchLayout::new(named_types));
			continue;
		};
		expected.admit(named_types, _fragment)?;
	}
	if let Some(layout) = &layout {
		for columns in evaluated.iter_mut().flatten() {
			for (column, target) in columns.iter_mut().zip(layout.types()) {
				if is_family(target)
					&& ColumnView::try_from(&*column)?.get_type().inner_type() != target
				{
					*column = apply_cast(ctx, column, target, _fragment)?;
				}
			}
		}
	}

	let views: Vec<Option<Vec<ColumnView<'_>>>> = evaluated
		.iter()
		.map(|columns| {
			columns.as_ref().map(|columns| columns.iter().map(ColumnView::try_from).collect()).transpose()
		})
		.collect::<Result<_>>()?;

	let mut result_data: Option<Vec<ColumnBuilder>> = None;
	let mut result_names: Vec<String> = Vec::new();

	for (row_idx, &selected) in selection.iter().enumerate() {
		let branch_results: &[ColumnView<'_>] = match selected {
			NO_BRANCH => &[],
			branch => views[branch].as_deref().unwrap(),
		};

		if branch_results.is_empty() {
			if let Some(data) = result_data.as_mut() {
				for col_data in data.iter_mut() {
					col_data.push_value(Value::none());
				}
			}
			continue;
		}

		if result_data.is_none() {
			let mut data: Vec<ColumnBuilder> = layout
				.as_ref()
				.unwrap()
				.types()
				.iter()
				.map(|ty| ColumnBuilder::with_capacity(ty.clone(), ctx.row_count))
				.collect();
			for _ in 0..row_idx {
				for col_data in data.iter_mut() {
					col_data.push_value(Value::none());
				}
			}
			result_data = Some(data);
			result_names = evaluated[selected]
				.as_deref()
				.unwrap()
				.iter()
				.map(|col| col.0.name().to_string())
				.collect();
		}

		let data = result_data.as_mut().unwrap();
		for (slot, branch_col) in data.iter_mut().zip(branch_results.iter()) {
			slot.push_value(branch_col.get_value(row_idx));
		}
	}

	let result_data = result_data.unwrap_or_default();
	let result: Vec<(FieldRef, ArrayRef)> = result_data
		.into_iter()
		.enumerate()
		.map(|(i, data)| data.finish(result_names.get(i).map_or("column", String::as_str)))
		.collect();

	if result.is_empty() {
		Ok(vec![factory::none("none", ctx.row_count)])
	} else {
		Ok(result)
	}
}

fn execute_multi_exprs(ctx: &EvalContext, exprs: &[CompiledExpr]) -> Result<Vec<(FieldRef, ArrayRef)>> {
	let mut result = Vec::new();
	for expr in exprs {
		result.extend(expr.execute_multi(ctx)?);
	}
	Ok(result)
}

fn execute_projection_multi(ctx: &EvalContext, expressions: &[CompiledExpr]) -> Result<Vec<(FieldRef, ArrayRef)>> {
	let mut result = Vec::with_capacity(expressions.len());

	for expr in expressions {
		result.push(expr.execute(ctx)?);
	}

	Ok(result)
}

fn apply_cast(
	ctx: &EvalContext,
	column: &(FieldRef, ArrayRef),
	target: &ValueType,
	fragment: &Fragment,
) -> Result<(FieldRef, ArrayRef)> {
	cast_column_data(ctx, &ColumnView::try_from(column)?, target.clone(), &|| fragment.clone())
		.map_err(|e| wrap_cast_error(e, fragment.clone(), target))
}

fn wrap_cast_error(err: Error, fragment: Fragment, target: &ValueType) -> Error {
	if err.0.code.starts_with("CAST_") {
		return err;
	}
	let cause = err.diagnostic();
	let wrapped = if target.is_bool() {
		CastError::InvalidBoolean {
			fragment,
			cause,
		}
	} else if target.is_temporal() {
		CastError::InvalidTemporal {
			fragment,
			target: target.clone(),
			cause,
		}
	} else if target.is_uuid() || *target == ValueType::IdentityId {
		CastError::InvalidUuid {
			fragment,
			target: target.clone(),
			cause,
		}
	} else {
		CastError::InvalidNumber {
			fragment,
			target: target.clone(),
			cause,
		}
	};
	Error::from(wrapped)
}

#[cfg(test)]
mod tests {
	use arrow_array::{ArrayRef, RecordBatch};
	use arrow_schema::FieldRef;
	use reifydb_core::{
		expression::{
			CastExpression, ColumnExpression, ConstantExpression, ElseIfExpression, Expression,
			IfExpression, MapExpression, TypeExpression,
		},
		interface::identifier::ColumnIdentifier,
		value::{batch::batch, column::factory},
	};
	use reifydb_value::{
		fragment::Fragment,
		value::{Value, column_view::ColumnView, value_type::ValueType},
	};

	use super::combine_bool_columns;
	use crate::expression::{context::EvalContext, eval::evaluate};

	fn column(name: &str) -> Expression {
		Expression::Column(ColumnExpression(ColumnIdentifier::with_alias(
			Fragment::internal("t"),
			Fragment::internal(name),
		)))
	}

	fn uncastable() -> Expression {
		Expression::Cast(CastExpression {
			fragment: Fragment::testing_empty(),
			expression: Box::new(Expression::Constant(ConstantExpression::Text {
				fragment: Fragment::internal("not-a-number"),
			})),
			to: TypeExpression {
				fragment: Fragment::testing_empty(),
				ty: ValueType::Int4,
			},
		})
	}

	fn conditional(
		condition: Expression,
		then_expr: Expression,
		else_ifs: Vec<(Expression, Expression)>,
		else_expr: Option<Expression>,
	) -> Expression {
		Expression::If(IfExpression {
			condition: Box::new(condition),
			then_expr: Box::new(then_expr),
			else_ifs: else_ifs
				.into_iter()
				.map(|(condition, then_expr)| ElseIfExpression {
					condition: Box::new(condition),
					then_expr: Box::new(then_expr),
					fragment: Fragment::testing_empty(),
				})
				.collect(),
			else_expr: else_expr.map(Box::new),
			fragment: Fragment::testing_empty(),
		})
	}

	fn bools(name: &str, data: [bool; 4]) -> (FieldRef, ArrayRef) {
		factory::bool(name, data)
	}

	fn ints(name: &str, data: [i32; 4]) -> (FieldRef, ArrayRef) {
		factory::int4(name, data)
	}

	#[test]
	fn every_row_reads_its_own_index_from_the_branch_it_selected() {
		// A branch is evaluated as a whole column, so row i must take branch[i]. Assembling the
		// result in append order instead of by row index shifts every value after the first switch.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			batch(vec![
				bools("flag", [true, false, false, true]),
				ints("hi", [100, 200, 300, 400]),
				ints("lo", [1, 2, 3, 4]),
			])
			.unwrap(),
			4,
		);

		let result =
			evaluate(&ctx, &conditional(column("flag"), column("hi"), vec![], Some(column("lo")))).unwrap();

		assert_eq!(result.1.as_ref(), factory::int4("", [100, 2, 3, 400]).1.as_ref());
	}

	#[test]
	fn an_else_if_chain_gives_each_row_its_first_matching_branch() {
		// Later conditions must never override an earlier match: row 1 satisfies both `second` and
		// nothing else, row 2 satisfies `second` alone, and row 3 falls through to the else.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			batch(vec![
				bools("first", [true, false, false, false]),
				bools("second", [true, true, true, false]),
				ints("a", [10, 20, 30, 40]),
				ints("b", [1, 2, 3, 4]),
				ints("c", [-1, -2, -3, -4]),
			])
			.unwrap(),
			4,
		);

		let result = evaluate(
			&ctx,
			&conditional(
				column("first"),
				column("a"),
				vec![(column("second"), column("b"))],
				Some(column("c")),
			),
		)
		.unwrap();

		assert_eq!(result.1.as_ref(), factory::int4("", [10, 2, 3, -4]).1.as_ref());
	}

	#[test]
	fn a_row_that_matches_no_branch_becomes_none() {
		// Without an else branch the unmatched rows must still occupy their slot, otherwise the
		// result column is shorter than the input and every downstream row pairs with the wrong key.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			batch(vec![bools("flag", [true, false, false, true]), ints("hi", [7, 8, 9, 10])]).unwrap(),
			4,
		);

		let result = evaluate(&ctx, &conditional(column("flag"), column("hi"), vec![], None)).unwrap();

		let view = ColumnView::try_from(&result).unwrap();
		assert_eq!(result.1.len(), 4);
		assert_eq!(view.get_value(0), Value::Int4(7));
		assert!(matches!(view.get_value(1), Value::None { .. }));
		assert!(matches!(view.get_value(2), Value::None { .. }));
		assert_eq!(view.get_value(3), Value::Int4(10));
	}

	#[test]
	fn a_branch_that_no_row_selects_is_never_evaluated() {
		// Hoisting a branch out of the row loop must not make it eager: a guard exists precisely to
		// keep a failing expression away from the rows that cannot satisfy it.
		let base = EvalContext::testing();
		let all_true = base.with_eval(
			batch(vec![bools("flag", [true, true, true, true]), ints("hi", [1, 2, 3, 4])]).unwrap(),
			4,
		);

		let skipped =
			evaluate(&all_true, &conditional(column("flag"), column("hi"), vec![], Some(uncastable())));

		assert!(skipped.is_ok(), "an else branch no row selects must not be evaluated");

		let one_false = base.with_eval(
			batch(vec![bools("flag", [true, true, false, true]), ints("hi", [1, 2, 3, 4])]).unwrap(),
			4,
		);

		let taken =
			evaluate(&one_false, &conditional(column("flag"), column("hi"), vec![], Some(uncastable())));

		assert!(
			taken.is_err(),
			"the branch must really fail when a row selects it, or the case above is vacuous"
		);
	}

	fn multi(names: [&str; 2]) -> Expression {
		Expression::Map(MapExpression {
			expressions: names.iter().map(|name| column(name)).collect(),
			fragment: Fragment::testing_empty(),
		})
	}

	fn none_literal() -> Expression {
		Expression::Constant(ConstantExpression::None {
			fragment: Fragment::testing_empty(),
		})
	}

	fn four_row_ctx(extra: Vec<(FieldRef, ArrayRef)>) -> RecordBatch {
		let mut cols = vec![bools("flag", [true, false, false, true])];
		cols.extend(extra);
		batch(cols).unwrap()
	}

	#[test]
	fn branches_of_different_types_are_rejected_instead_of_aborting() {
		// The column buffer matches the value variant exactly and panics on anything else, so an
		// unvalidated mismatch takes the process down rather than failing the query.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			four_row_ctx(vec![ints("small", [1, 2, 3, 4]), factory::int8("wide", [5i64, 6, 7, 8])]),
			4,
		);

		let err = evaluate(&ctx, &conditional(column("flag"), column("small"), vec![], Some(column("wide"))))
			.expect_err("int4 and int8 branches must not be accepted");

		assert_eq!(err.0.code, "RUNTIME_012");
	}

	#[test]
	fn an_optional_branch_still_pairs_with_its_bare_type() {
		// Option is a wrapper over the same base type, so these branches agree; rejecting them
		// would break every conditional whose branches differ only in nullability.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			four_row_ctx(vec![
				ints("bare", [1, 2, 3, 4]),
				factory::int4_with_bitvec("opt", [9, 8, 7, 6], vec![true, false, true, true]),
			]),
			4,
		);

		let result = evaluate(&ctx, &conditional(column("flag"), column("bare"), vec![], Some(column("opt"))))
			.expect("a bare and an optional branch of one base type must agree");

		assert_eq!(result.1.len(), 4);
	}

	#[test]
	fn a_none_branch_widens_to_the_other_branch_type() {
		// A none literal carries Option(Any); treating Any as a concrete type would reject the
		// guarded-value shape that every conditional projection relies on.
		let base = EvalContext::testing();
		let ctx = base.with_eval(four_row_ctx(vec![ints("hi", [7, 8, 9, 10])]), 4);

		let result = evaluate(&ctx, &conditional(column("flag"), column("hi"), vec![], Some(none_literal())))
			.expect("a none branch must widen to the other branch type");

		let view = ColumnView::try_from(&result).unwrap();
		assert_eq!(view.get_value(0), Value::Int4(7));
		assert!(matches!(view.get_value(1), Value::None { .. }));
	}

	#[test]
	fn a_none_first_row_does_not_fix_the_result_type() {
		// Typing the result from the first row's none branch made it Any, so the first int after it aborted the
		// process.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			batch(vec![bools("flag", [false, true, false, true]), ints("hi", [7, 8, 9, 10])]).unwrap(),
			4,
		);

		let result = evaluate(&ctx, &conditional(column("flag"), column("hi"), vec![], Some(none_literal())))
			.expect("a none first row must not fix the result type");

		let view = ColumnView::try_from(&result).unwrap();
		assert!(matches!(view.get_value(0), Value::None { .. }));
		assert_eq!(view.get_value(1), Value::Int4(8));
		assert!(matches!(view.get_value(2), Value::None { .. }));
		assert_eq!(view.get_value(3), Value::Int4(10));
	}

	#[test]
	fn branches_of_different_widths_are_rejected() {
		// A narrower branch used to leave its unfilled columns short, silently misaligning every
		// value after the first switch; a wider one had its surplus columns dropped.
		let base = EvalContext::testing();
		let ctx = base.with_eval(four_row_ctx(vec![ints("a", [1, 2, 3, 4]), ints("b", [10, 20, 30, 40])]), 4);

		let wide_then =
			evaluate(&ctx, &conditional(column("flag"), multi(["a", "b"]), vec![], Some(column("a"))))
				.expect_err("a two column branch must not pair with a one column branch");
		assert_eq!(wide_then.0.code, "RUNTIME_012");

		let wide_else =
			evaluate(&ctx, &conditional(column("flag"), column("a"), vec![], Some(multi(["a", "b"]))))
				.expect_err("a one column branch must not pair with a two column branch");
		assert_eq!(wide_else.0.code, "RUNTIME_012");
	}

	#[test]
	fn a_mismatched_branch_no_row_selects_is_still_not_rejected() {
		// Validation must read only the branches that were evaluated, otherwise it resurrects the
		// eager evaluation that the guard exists to prevent.
		let base = EvalContext::testing();
		let ctx = base.with_eval(
			batch(vec![
				bools("flag", [true, true, true, true]),
				ints("small", [1, 2, 3, 4]),
				factory::int8("wide", [5i64, 6, 7, 8]),
			])
			.unwrap(),
			4,
		);

		let result =
			evaluate(&ctx, &conditional(column("flag"), column("small"), vec![], Some(column("wide"))))
				.expect("an unselected branch must not be validated");

		assert_eq!(result.1.len(), 4);
	}

	#[test]
	fn a_multi_column_value_in_a_single_column_slot_is_rejected() {
		// Taking the first column and discarding the rest loses data with no signal; map, extend
		// and patch all reach a value expression through this path.
		let base = EvalContext::testing();
		let ctx = base.with_eval(four_row_ctx(vec![ints("a", [1, 2, 3, 4]), ints("b", [10, 20, 30, 40])]), 4);

		let err = evaluate(&ctx, &multi(["a", "b"])).expect_err("a two column value must not be truncated");

		assert_eq!(err.0.code, "RUNTIME_010");
	}

	#[test]
	fn combining_boolean_columns_of_different_lengths_is_an_error() {
		// A length mismatch in one batch must be an error, never padded none rows or an out of range read.
		let left = factory::bool("l", [true, true, false]);
		let right = factory::bool("r", [true, false]);

		let result = combine_bool_columns(left, right, Fragment::internal("and"), |l, r| l && r);

		assert!(
			result.is_err(),
			"a right side shorter than the left must be an error, got {:?}",
			result.map(|c| c.1)
		);
	}
}
