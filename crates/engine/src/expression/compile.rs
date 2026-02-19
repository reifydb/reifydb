// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData};
use reifydb_rql::expression::Expression;
use reifydb_type::{
	error,
	error::diagnostic::{
		cast,
		operator::{
			and_can_not_applied_to_number, and_can_not_applied_to_temporal, and_can_not_applied_to_text,
			and_can_not_applied_to_uuid, between_cannot_be_applied_to_incompatible_types,
			equal_cannot_be_applied_to_incompatible_types,
			greater_than_cannot_be_applied_to_incompatible_types,
			greater_than_equal_cannot_be_applied_to_incompatible_types,
			less_than_cannot_be_applied_to_incompatible_types,
			less_than_equal_cannot_be_applied_to_incompatible_types,
			not_equal_cannot_be_applied_to_incompatible_types, or_can_not_applied_to_number,
			or_can_not_applied_to_temporal, or_can_not_applied_to_text, or_can_not_applied_to_uuid,
			xor_can_not_applied_to_number, xor_can_not_applied_to_temporal, xor_can_not_applied_to_text,
			xor_can_not_applied_to_uuid,
		},
		runtime::{self, variable_is_dataframe, variable_not_found},
	},
	fragment::Fragment,
	return_error,
	value::{Value, r#type::Type},
};

use super::context::CompileContext;
use crate::{
	expression::{
		arith::{add::add_columns, div::div_columns, mul::mul_columns, rem::rem_columns, sub::sub_columns},
		call::call_eval,
		cast::cast_column_data,
		compare::{Equal, GreaterThan, GreaterThanEqual, LessThan, LessThanEqual, NotEqual, compare_columns},
		constant::{constant_value, constant_value_of},
		context::EvalContext,
		lookup::column_lookup,
	},
	vm::stack::Variable,
};

pub struct CompiledExpr {
	inner: CompiledExprInner,
	access_column_name: Option<String>,
}

enum CompiledExprInner {
	Single(Box<dyn Fn(&EvalContext) -> crate::Result<Column> + Send + Sync>),
	Multi(Box<dyn Fn(&EvalContext) -> crate::Result<Vec<Column>> + Send + Sync>),
}

impl CompiledExpr {
	pub fn new(f: impl Fn(&EvalContext) -> crate::Result<Column> + Send + Sync + 'static) -> Self {
		Self {
			inner: CompiledExprInner::Single(Box::new(f)),
			access_column_name: None,
		}
	}

	pub fn new_multi(f: impl Fn(&EvalContext) -> crate::Result<Vec<Column>> + Send + Sync + 'static) -> Self {
		Self {
			inner: CompiledExprInner::Multi(Box::new(f)),
			access_column_name: None,
		}
	}

	pub fn new_access(
		name: String,
		f: impl Fn(&EvalContext) -> crate::Result<Column> + Send + Sync + 'static,
	) -> Self {
		Self {
			inner: CompiledExprInner::Single(Box::new(f)),
			access_column_name: Some(name),
		}
	}

	pub fn access_column_name(&self) -> Option<&str> {
		self.access_column_name.as_deref()
	}

	pub fn execute(&self, ctx: &EvalContext) -> crate::Result<Column> {
		match &self.inner {
			CompiledExprInner::Single(f) => f(ctx),
			CompiledExprInner::Multi(f) => {
				let columns = f(ctx)?;
				Ok(columns.into_iter().next().unwrap_or_else(|| Column {
					name: Fragment::internal("none"),
					data: ColumnData::with_capacity(Type::Option(Box::new(Type::Boolean)), 0),
				}))
			}
		}
	}

	pub fn execute_multi(&self, ctx: &EvalContext) -> crate::Result<Vec<Column>> {
		match &self.inner {
			CompiledExprInner::Single(f) => Ok(vec![f(ctx)?]),
			CompiledExprInner::Multi(f) => f(ctx),
		}
	}
}

/// Compile an `Expression` into a `CompiledExpr`.
///
/// All execution logic is baked into closures at compile time — no match dispatch at runtime.
pub fn compile_expression(_ctx: &CompileContext, expr: &Expression) -> crate::Result<CompiledExpr> {
	Ok(match expr {
		Expression::Constant(e) => {
			let expr = e.clone();
			CompiledExpr::new(move |ctx| {
				let row_count = ctx.take.unwrap_or(ctx.row_count);
				Ok(Column {
					name: expr.full_fragment_owned(),
					data: constant_value(&expr, row_count)?,
				})
			})
		}

		Expression::Column(e) => {
			let expr = e.clone();
			CompiledExpr::new(move |ctx| column_lookup(ctx, &expr))
		}

		Expression::Variable(e) => {
			let expr = e.clone();
			CompiledExpr::new(move |ctx| {
				let variable_name = expr.name();

				if variable_name == "env" {
					return_error!(variable_is_dataframe(variable_name));
				}

				match ctx.symbol_table.get(variable_name) {
					Some(Variable::Scalar(columns)) => {
						let value = columns.scalar_value();
						let mut data =
							ColumnData::with_capacity(value.get_type(), ctx.row_count);
						for _ in 0..ctx.row_count {
							data.push_value(value.clone());
						}
						Ok(Column {
							name: Fragment::internal(variable_name),
							data,
						})
					}
					Some(Variable::Columns(_))
					| Some(Variable::ForIterator {
						..
					})
					| Some(Variable::Closure(_)) => {
						return_error!(variable_is_dataframe(variable_name));
					}
					None => {
						return_error!(variable_not_found(variable_name));
					}
				}
			})
		}

		Expression::Parameter(e) => {
			let expr = e.clone();
			CompiledExpr::new(move |ctx| crate::expression::parameter::parameter_lookup(ctx, &expr))
		}

		Expression::Alias(e) => {
			let inner = compile_expression(_ctx, &e.expression)?;
			let alias = e.alias.0.clone();
			CompiledExpr::new(move |ctx| {
				let mut column = inner.execute(ctx)?;
				column.name = alias.clone();
				Ok(column)
			})
		}

		Expression::Add(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				add_columns(ctx, &l, &r, || fragment.clone())
			})
		}

		Expression::Sub(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				sub_columns(ctx, &l, &r, || fragment.clone())
			})
		}

		Expression::Mul(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				mul_columns(ctx, &l, &r, || fragment.clone())
			})
		}

		Expression::Div(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				div_columns(ctx, &l, &r, || fragment.clone())
			})
		}

		Expression::Rem(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				rem_columns(ctx, &l, &r, || fragment.clone())
			})
		}

		Expression::Equal(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				let result = compare_columns::<Equal>(
					&l,
					&r,
					fragment.clone(),
					equal_cannot_be_applied_to_incompatible_types,
				);
				result
			})
		}

		Expression::NotEqual(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				let result = compare_columns::<NotEqual>(
					&l,
					&r,
					fragment.clone(),
					not_equal_cannot_be_applied_to_incompatible_types,
				);
				result
			})
		}

		Expression::GreaterThan(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				let result = compare_columns::<GreaterThan>(
					&l,
					&r,
					fragment.clone(),
					greater_than_cannot_be_applied_to_incompatible_types,
				);
				result
			})
		}

		Expression::GreaterThanEqual(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				let result = compare_columns::<GreaterThanEqual>(
					&l,
					&r,
					fragment.clone(),
					greater_than_equal_cannot_be_applied_to_incompatible_types,
				);
				result
			})
		}

		Expression::LessThan(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				let result = compare_columns::<LessThan>(
					&l,
					&r,
					fragment.clone(),
					less_than_cannot_be_applied_to_incompatible_types,
				);
				result
			})
		}

		Expression::LessThanEqual(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				let result = compare_columns::<LessThanEqual>(
					&l,
					&r,
					fragment.clone(),
					less_than_equal_cannot_be_applied_to_incompatible_types,
				);
				result
			})
		}

		Expression::And(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				let result = execute_and(&l, &r, &fragment);
				result
			})
		}

		Expression::Or(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				let result = execute_or(&l, &r, &fragment);
				result
			})
		}

		Expression::Xor(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				let result = execute_xor(&l, &r, &fragment);
				result
			})
		}

		Expression::Prefix(e) => {
			let expr = e.clone();
			CompiledExpr::new(move |ctx| {
				crate::expression::prefix::prefix_eval(ctx, &expr, ctx.functions, ctx.clock)
			})
		}

		Expression::Type(e) => {
			let ty = e.ty.clone();
			let fragment = e.fragment.clone();
			CompiledExpr::new(move |ctx| {
				let row_count = ctx.take.unwrap_or(ctx.row_count);
				let values: Vec<Box<Value>> =
					(0..row_count).map(|_| Box::new(Value::Type(ty.clone()))).collect();
				Ok(Column::new(fragment.text(), ColumnData::any(values)))
			})
		}

		Expression::AccessSource(e) => {
			let col_name = e.column.name.text().to_string();
			let expr = e.clone();
			CompiledExpr::new_access(col_name, move |ctx| {
				crate::expression::access::access_lookup(ctx, &expr)
			})
		}

		Expression::Tuple(e) => {
			if e.expressions.len() == 1 {
				let inner = compile_expression(_ctx, &e.expressions[0])?;
				CompiledExpr::new(move |ctx| inner.execute(ctx))
			} else {
				unimplemented!("Multi-element tuple evaluation not yet supported: {:?}", e)
			}
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
					greater_than_equal_cannot_be_applied_to_incompatible_types,
				)?;
				let le_result = compare_columns::<LessThanEqual>(
					&value_col,
					&upper_col,
					fragment.clone(),
					less_than_equal_cannot_be_applied_to_incompatible_types,
				)?;

				if !matches!(ge_result.data(), ColumnData::Bool(_))
					|| !matches!(le_result.data(), ColumnData::Bool(_))
				{
					return_error!(between_cannot_be_applied_to_incompatible_types(
						fragment.clone(),
						value_col.get_type(),
						lower_col.get_type(),
					))
				}

				match (ge_result.data(), le_result.data()) {
					(ColumnData::Bool(ge_container), ColumnData::Bool(le_container)) => {
						let mut data = Vec::with_capacity(ge_container.len());
						let mut bitvec = Vec::with_capacity(ge_container.len());

						for i in 0..ge_container.len() {
							if ge_container.is_defined(i) && le_container.is_defined(i) {
								data.push(ge_container.data().get(i)
									&& le_container.data().get(i));
								bitvec.push(true);
							} else {
								data.push(false);
								bitvec.push(false);
							}
						}

						Ok(Column {
							name: fragment.clone(),
							data: ColumnData::bool_with_bitvec(data, bitvec),
						})
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
				_ => std::slice::from_ref(e.list.as_ref()),
			};
			let value = compile_expression(_ctx, &e.value)?;
			let list: Vec<CompiledExpr> = list_expressions
				.iter()
				.map(|expr| compile_expression(_ctx, expr))
				.collect::<crate::Result<Vec<_>>>()?;
			let negated = e.negated;
			let fragment = e.fragment.clone();
			CompiledExpr::new(move |ctx| {
				if list.is_empty() {
					let value_col = value.execute(ctx)?;
					let len = value_col.data().len();
					let result = vec![negated; len];
					return Ok(Column {
						name: fragment.clone(),
						data: ColumnData::bool(result),
					});
				}

				let value_col = value.execute(ctx)?;

				let first_col = list[0].execute(ctx)?;
				let mut result = compare_columns::<Equal>(
					&value_col,
					&first_col,
					fragment.clone(),
					equal_cannot_be_applied_to_incompatible_types,
				)?;

				for list_expr in list.iter().skip(1) {
					let list_col = list_expr.execute(ctx)?;
					let eq_result = compare_columns::<Equal>(
						&value_col,
						&list_col,
						fragment.clone(),
						equal_cannot_be_applied_to_incompatible_types,
					)?;
					result = or_columns(result, eq_result, fragment.clone())?;
				}

				if negated {
					result = negate_column(result, fragment.clone());
				}

				Ok(result)
			})
		}

		Expression::Cast(e) => {
			if let Expression::Constant(const_expr) = e.expression.as_ref() {
				let const_expr = const_expr.clone();
				let target_type = e.to.ty.clone();
				CompiledExpr::new(move |ctx| {
					let row_count = ctx.take.unwrap_or(ctx.row_count);
					let data = constant_value(&const_expr, row_count)?;
					let casted = if data.get_type() == target_type {
						data
					} else {
						constant_value_of(&const_expr, target_type.clone(), row_count)?
					};
					Ok(Column {
						name: const_expr.full_fragment_owned(),
						data: casted,
					})
				})
			} else {
				let inner = compile_expression(_ctx, &e.expression)?;
				let target_type = e.to.ty.clone();
				let inner_fragment = e.expression.full_fragment_owned();
				CompiledExpr::new(move |ctx| {
					let column = inner.execute(ctx)?;
					let frag = inner_fragment.clone();
					let casted =
						cast_column_data(ctx, &column.data(), target_type.clone(), &|| {
							inner_fragment.clone()
						})
						.map_err(|e| {
							error!(cast::invalid_number(
								frag,
								target_type.clone(),
								e.diagnostic()
							))
						})?;
					Ok(Column {
						name: column.name_owned(),
						data: casted,
					})
				})
			}
		}

		Expression::If(e) => {
			let condition = compile_expression(_ctx, &e.condition)?;
			let then_expr = compile_expressions(_ctx, std::slice::from_ref(e.then_expr.as_ref()))?;
			let else_ifs: Vec<(CompiledExpr, Vec<CompiledExpr>)> = e
				.else_ifs
				.iter()
				.map(|ei| {
					Ok((
						compile_expression(_ctx, &ei.condition)?,
						compile_expressions(_ctx, std::slice::from_ref(ei.then_expr.as_ref()))?,
					))
				})
				.collect::<crate::Result<Vec<_>>>()?;
			let else_branch: Option<Vec<CompiledExpr>> = match &e.else_expr {
				Some(expr) => Some(compile_expressions(_ctx, std::slice::from_ref(expr.as_ref()))?),
				None => None,
			};
			let fragment = e.fragment.clone();
			CompiledExpr::new_multi(move |ctx| {
				execute_if_multi(ctx, &condition, &then_expr, &else_ifs, &else_branch, &fragment)
			})
		}

		Expression::Map(e) => {
			let expressions = compile_expressions(_ctx, &e.expressions)?;
			CompiledExpr::new_multi(move |ctx| execute_map_multi(ctx, &expressions))
		}

		Expression::Extend(e) => {
			let expressions = compile_expressions(_ctx, &e.expressions)?;
			CompiledExpr::new_multi(move |ctx| execute_extend_multi(ctx, &expressions))
		}

		Expression::Call(e) => {
			let expr = e.clone();
			CompiledExpr::new(move |ctx| call_eval(ctx, &expr, ctx.functions, ctx.clock))
		}

		Expression::SumTypeConstructor(_) => {
			panic!(
				"SumTypeConstructor in expression context — constructors should be expanded by InlineDataNode before expression compilation"
			);
		}

		Expression::IsVariant(e) => {
			let col_name = match e.expression.as_ref() {
				Expression::Column(c) => c.0.name.text().to_string(),
				other => other.full_fragment_owned().text().to_string(),
			};
			let tag_col_name = format!("{}_tag", col_name);
			let tag = e.tag.expect("IS variant tag must be resolved before compilation");
			let fragment = e.fragment.clone();
			CompiledExpr::new(move |ctx| {
				if let Some(tag_col) =
					ctx.columns.iter().find(|c| c.name().text() == tag_col_name.as_str())
				{
					match tag_col.data() {
						ColumnData::Uint1(container) => {
							let results: Vec<bool> = container
								.iter()
								.take(ctx.row_count)
								.map(|v| v == Some(tag))
								.collect();
							Ok(Column {
								name: fragment.clone(),
								data: ColumnData::bool(results),
							})
						}
						_ => Ok(Column {
							name: fragment.clone(),
							data: ColumnData::none_typed(Type::Boolean, ctx.row_count),
						}),
					}
				} else {
					Ok(Column {
						name: fragment.clone(),
						data: ColumnData::none_typed(Type::Boolean, ctx.row_count),
					})
				}
			})
		}

		Expression::FieldAccess(e) => {
			let field_name = e.field.text().to_string();
			// Extract variable name at compile time if the object is a variable
			let var_name = match e.object.as_ref() {
				Expression::Variable(var_expr) => Some(var_expr.name().to_string()),
				_ => None,
			};
			let object = compile_expression(_ctx, &e.object)?;
			CompiledExpr::new(move |ctx| {
				if let Some(ref variable_name) = var_name {
					match ctx.symbol_table.get(variable_name) {
						Some(Variable::Columns(columns)) => {
							let col = columns
								.columns
								.iter()
								.find(|c| c.name.text() == field_name);
							match col {
								Some(col) => {
									let value = col.data.get_value(0);
									let row_count =
										ctx.take.unwrap_or(ctx.row_count);
									let mut data = ColumnData::with_capacity(
										value.get_type(),
										row_count,
									);
									for _ in 0..row_count {
										data.push_value(value.clone());
									}
									Ok(Column {
										name: Fragment::internal(&field_name),
										data,
									})
								}
								None => {
									let available: Vec<String> = columns
										.columns
										.iter()
										.map(|c| c.name.text().to_string())
										.collect();
									return_error!(runtime::field_not_found(
										variable_name,
										&field_name,
										&available
									));
								}
							}
						}
						Some(Variable::Scalar(_)) | Some(Variable::Closure(_)) => {
							return_error!(runtime::field_not_found(
								variable_name,
								&field_name,
								&[]
							));
						}
						Some(Variable::ForIterator {
							..
						}) => {
							return_error!(runtime::variable_is_dataframe(variable_name));
						}
						None => {
							return_error!(runtime::variable_not_found(variable_name));
						}
					}
				} else {
					// For non-variable objects, evaluate the object and try to interpret result
					let _obj_col = object.execute(ctx)?;
					return_error!(runtime::field_not_found("<expression>", &field_name, &[]));
				}
			})
		}
	})
}

fn compile_expressions(ctx: &CompileContext, exprs: &[Expression]) -> crate::Result<Vec<CompiledExpr>> {
	exprs.iter().map(|e| compile_expression(ctx, e)).collect()
}

// --- Helper functions (moved from execute.rs) ---

fn execute_and(left: &Column, right: &Column, fragment: &Fragment) -> crate::Result<Column> {
	super::option::binary_op_unwrap_option(left, right, fragment.clone(), |left, right| {
		match (&left.data(), &right.data()) {
			(ColumnData::Bool(l_container), ColumnData::Bool(r_container)) => {
				let data: Vec<bool> = l_container
					.data()
					.iter()
					.zip(r_container.data().iter())
					.map(|(l_val, r_val)| l_val && r_val)
					.collect();

				Ok(Column {
					name: fragment.clone(),
					data: ColumnData::bool(data),
				})
			}
			(l, r) => {
				if l.is_number() || r.is_number() {
					return_error!(and_can_not_applied_to_number(fragment.clone()));
				} else if l.is_text() || r.is_text() {
					return_error!(and_can_not_applied_to_text(fragment.clone()));
				} else if l.is_temporal() || r.is_temporal() {
					return_error!(and_can_not_applied_to_temporal(fragment.clone()));
				} else if l.is_uuid() || r.is_uuid() {
					return_error!(and_can_not_applied_to_uuid(fragment.clone()));
				} else {
					unimplemented!("{} and {}", l.get_type(), r.get_type());
				}
			}
		}
	})
}

fn execute_or(left: &Column, right: &Column, fragment: &Fragment) -> crate::Result<Column> {
	super::option::binary_op_unwrap_option(left, right, fragment.clone(), |left, right| {
		match (&left.data(), &right.data()) {
			(ColumnData::Bool(l_container), ColumnData::Bool(r_container)) => {
				let data: Vec<bool> = l_container
					.data()
					.iter()
					.zip(r_container.data().iter())
					.map(|(l_val, r_val)| l_val || r_val)
					.collect();

				Ok(Column {
					name: fragment.clone(),
					data: ColumnData::bool(data),
				})
			}
			(l, r) => {
				if l.is_number() || r.is_number() {
					return_error!(or_can_not_applied_to_number(fragment.clone()));
				} else if l.is_text() || r.is_text() {
					return_error!(or_can_not_applied_to_text(fragment.clone()));
				} else if l.is_temporal() || r.is_temporal() {
					return_error!(or_can_not_applied_to_temporal(fragment.clone()));
				} else if l.is_uuid() || r.is_uuid() {
					return_error!(or_can_not_applied_to_uuid(fragment.clone()));
				} else {
					unimplemented!("{} or {}", l.get_type(), r.get_type());
				}
			}
		}
	})
}

fn execute_xor(left: &Column, right: &Column, fragment: &Fragment) -> crate::Result<Column> {
	super::option::binary_op_unwrap_option(left, right, fragment.clone(), |left, right| {
		match (&left.data(), &right.data()) {
			(ColumnData::Bool(l_container), ColumnData::Bool(r_container)) => {
				let data: Vec<bool> = l_container
					.data()
					.iter()
					.zip(r_container.data().iter())
					.map(|(l_val, r_val)| l_val != r_val)
					.collect();

				Ok(Column {
					name: fragment.clone(),
					data: ColumnData::bool(data),
				})
			}
			(l, r) => {
				if l.is_number() || r.is_number() {
					return_error!(xor_can_not_applied_to_number(fragment.clone()));
				} else if l.is_text() || r.is_text() {
					return_error!(xor_can_not_applied_to_text(fragment.clone()));
				} else if l.is_temporal() || r.is_temporal() {
					return_error!(xor_can_not_applied_to_temporal(fragment.clone()));
				} else if l.is_uuid() || r.is_uuid() {
					return_error!(xor_can_not_applied_to_uuid(fragment.clone()));
				} else {
					unimplemented!("{} xor {}", l.get_type(), r.get_type());
				}
			}
		}
	})
}

fn or_columns(left: Column, right: Column, fragment: Fragment) -> crate::Result<Column> {
	super::option::binary_op_unwrap_option(&left, &right, fragment.clone(), |left, right| {
		match (left.data(), right.data()) {
			(ColumnData::Bool(l), ColumnData::Bool(r)) => {
				let len = l.len();
				let mut data = Vec::with_capacity(len);
				let mut bitvec = Vec::with_capacity(len);

				for i in 0..len {
					let l_defined = l.is_defined(i);
					let r_defined = r.is_defined(i);
					let l_val = l.data().get(i);
					let r_val = r.data().get(i);

					if l_defined && r_defined {
						data.push(l_val || r_val);
						bitvec.push(true);
					} else {
						data.push(false);
						bitvec.push(false);
					}
				}

				Ok(Column {
					name: fragment.clone(),
					data: ColumnData::bool_with_bitvec(data, bitvec),
				})
			}
			_ => {
				unreachable!(
					"OR columns should only be called with boolean columns from equality comparisons"
				)
			}
		}
	})
}

fn negate_column(col: Column, fragment: Fragment) -> Column {
	super::option::unary_op_unwrap_option(&col, |col| match col.data() {
		ColumnData::Bool(container) => {
			let len = container.len();
			let mut data = Vec::with_capacity(len);
			let mut bitvec = Vec::with_capacity(len);

			for i in 0..len {
				if container.is_defined(i) {
					data.push(!container.data().get(i));
					bitvec.push(true);
				} else {
					data.push(false);
					bitvec.push(false);
				}
			}

			Ok(Column {
				name: fragment.clone(),
				data: ColumnData::bool_with_bitvec(data, bitvec),
			})
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
) -> crate::Result<Vec<Column>> {
	let condition_column = condition.execute(ctx)?;

	let mut result_data: Option<Vec<ColumnData>> = None;
	let mut result_names: Vec<Fragment> = Vec::new();

	for row_idx in 0..ctx.row_count {
		let condition_value = condition_column.data().get_value(row_idx);

		let branch_results = if is_truthy(&condition_value) {
			execute_multi_exprs(ctx, then_expr)?
		} else {
			let mut found_branch = false;
			let mut branch_columns = None;

			for (else_if_condition, else_if_then) in else_ifs {
				let else_if_col = else_if_condition.execute(ctx)?;
				let else_if_value = else_if_col.data().get_value(row_idx);

				if is_truthy(&else_if_value) {
					branch_columns = Some(execute_multi_exprs(ctx, else_if_then)?);
					found_branch = true;
					break;
				}
			}

			if found_branch {
				branch_columns.unwrap()
			} else if let Some(else_exprs) = else_branch {
				execute_multi_exprs(ctx, else_exprs)?
			} else {
				vec![]
			}
		};

		// Handle empty branch results (from empty blocks like `{}` or no-branch-taken)
		let is_empty_result = branch_results.is_empty();
		if is_empty_result {
			if let Some(data) = result_data.as_mut() {
				for col_data in data.iter_mut() {
					col_data.push_value(Value::none());
				}
			}
			continue;
		}

		// Initialize from first non-empty branch, backfilling previous empty rows
		if result_data.is_none() {
			let mut data: Vec<ColumnData> = branch_results
				.iter()
				.map(|col| ColumnData::with_capacity(col.data().get_type(), ctx.row_count))
				.collect();
			for _ in 0..row_idx {
				for col_data in data.iter_mut() {
					col_data.push_value(Value::none());
				}
			}
			result_data = Some(data);
			result_names = branch_results.iter().map(|col| col.name.clone()).collect();
		}

		let data = result_data.as_mut().unwrap();
		for (i, branch_col) in branch_results.iter().enumerate() {
			if i < data.len() {
				let branch_value = branch_col.data().get_value(row_idx);
				data[i].push_value(branch_value);
			}
		}
	}

	let result_data = result_data.unwrap_or_default();
	let result: Vec<Column> = result_data
		.into_iter()
		.enumerate()
		.map(|(i, data)| Column {
			name: result_names.get(i).cloned().unwrap_or_else(|| Fragment::internal("column")),
			data,
		})
		.collect();

	if result.is_empty() {
		Ok(vec![Column {
			name: Fragment::internal("none"),
			data: ColumnData::none_typed(Type::Boolean, ctx.row_count),
		}])
	} else {
		Ok(result)
	}
}

fn execute_multi_exprs(ctx: &EvalContext, exprs: &[CompiledExpr]) -> crate::Result<Vec<Column>> {
	let mut result = Vec::new();
	for expr in exprs {
		result.extend(expr.execute_multi(ctx)?);
	}
	Ok(result)
}

fn execute_map_multi(ctx: &EvalContext, expressions: &[CompiledExpr]) -> crate::Result<Vec<Column>> {
	let mut result = Vec::with_capacity(expressions.len());

	for expr in expressions {
		let column = expr.execute(ctx)?;
		let name = column.name.text().to_string();
		result.push(Column {
			name: Fragment::internal(name),
			data: column.data,
		});
	}

	Ok(result)
}

fn execute_extend_multi(ctx: &EvalContext, expressions: &[CompiledExpr]) -> crate::Result<Vec<Column>> {
	let mut result = Vec::with_capacity(expressions.len());

	for expr in expressions {
		let column = expr.execute(ctx)?;
		let name = column.name.text().to_string();
		result.push(Column {
			name: Fragment::internal(name),
			data: column.data,
		});
	}

	Ok(result)
}
