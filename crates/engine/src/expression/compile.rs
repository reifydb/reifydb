// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::slice::from_ref;

use reifydb_core::value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns};
use reifydb_rql::expression::Expression;
use reifydb_type::{
	error::{BinaryOp, Error, IntoDiagnostic, LogicalOp, RuntimeErrorKind, TypeError},
	fragment::Fragment,
	value::{Value, r#type::Type},
};

use super::{
	context::CompileContext,
	option::{binary_op_unwrap_option, unary_op_unwrap_option},
};
use crate::{
	Result,
	error::CastError,
	expression::{
		access::access_lookup,
		arith::{add::add_columns, div::div_columns, mul::mul_columns, rem::rem_columns, sub::sub_columns},
		call::call_builtin,
		cast::cast_column_data,
		compare::{Equal, GreaterThan, GreaterThanEqual, LessThan, LessThanEqual, NotEqual, compare_columns},
		constant::{constant_value, constant_value_of},
		context::EvalContext,
		logic::execute_logical_op,
		lookup::column_lookup,
		parameter::parameter_lookup,
		prefix::prefix_apply,
	},
	vm::stack::Variable,
};

type SingleExprFn = Box<dyn Fn(&EvalContext) -> Result<ColumnWithName> + Send + Sync>;
type MultiExprFn = Box<dyn Fn(&EvalContext) -> Result<Vec<ColumnWithName>> + Send + Sync>;

pub struct CompiledExpr {
	inner: CompiledExprInner,
	access_column_name: Option<String>,
}

enum CompiledExprInner {
	Single(SingleExprFn),
	Multi(MultiExprFn),
}

impl CompiledExpr {
	pub fn new(f: impl Fn(&EvalContext) -> Result<ColumnWithName> + Send + Sync + 'static) -> Self {
		Self {
			inner: CompiledExprInner::Single(Box::new(f)),
			access_column_name: None,
		}
	}

	pub fn new_multi(f: impl Fn(&EvalContext) -> Result<Vec<ColumnWithName>> + Send + Sync + 'static) -> Self {
		Self {
			inner: CompiledExprInner::Multi(Box::new(f)),
			access_column_name: None,
		}
	}

	pub fn new_access(
		name: String,
		f: impl Fn(&EvalContext) -> Result<ColumnWithName> + Send + Sync + 'static,
	) -> Self {
		Self {
			inner: CompiledExprInner::Single(Box::new(f)),
			access_column_name: Some(name),
		}
	}

	pub fn access_column_name(&self) -> Option<&str> {
		self.access_column_name.as_deref()
	}

	pub fn execute(&self, ctx: &EvalContext) -> Result<ColumnWithName> {
		match &self.inner {
			CompiledExprInner::Single(f) => f(ctx),
			CompiledExprInner::Multi(f) => {
				let columns = f(ctx)?;
				Ok(columns.into_iter().next().unwrap_or_else(|| ColumnWithName {
					name: Fragment::internal("none"),
					data: ColumnBuffer::with_capacity(Type::Option(Box::new(Type::Boolean)), 0),
				}))
			}
		}
	}

	pub fn execute_multi(&self, ctx: &EvalContext) -> Result<Vec<ColumnWithName>> {
		match &self.inner {
			CompiledExprInner::Single(f) => Ok(vec![f(ctx)?]),
			CompiledExprInner::Multi(f) => f(ctx),
		}
	}
}

macro_rules! compile_arith {
	($ctx:expr, $e:expr, $op_fn:path) => {{
		let left = compile_expression($ctx, &$e.left)?;
		let right = compile_expression($ctx, &$e.right)?;
		let fragment = $e.full_fragment_owned();
		CompiledExpr::new(move |ctx| {
			let l = left.execute(ctx)?;
			let r = right.execute(ctx)?;
			$op_fn(ctx, &l, &r, || fragment.clone())
		})
	}};
}

macro_rules! compile_compare {
	($ctx:expr, $e:expr, $cmp_type:ty, $binary_op:expr) => {{
		let left = compile_expression($ctx, &$e.left)?;
		let right = compile_expression($ctx, &$e.right)?;
		let fragment = $e.full_fragment_owned();
		CompiledExpr::new(move |ctx| {
			let l = left.execute(ctx)?;
			let r = right.execute(ctx)?;
			compare_columns::<$cmp_type>(&l, &r, fragment.clone(), |f, l, r| {
				TypeError::BinaryOperatorNotApplicable {
					operator: $binary_op,
					left: l,
					right: r,
					fragment: f,
				}
				.into_diagnostic()
			})
		})
	}};
}

/// Compile an `Expression` into a `CompiledExpr`.
///
/// All execution logic is baked into closures at compile time - no match dispatch at runtime.
pub fn compile_expression(_ctx: &CompileContext, expr: &Expression) -> Result<CompiledExpr> {
	Ok(match expr {
		Expression::Constant(e) => {
			let expr = e.clone();
			CompiledExpr::new(move |ctx| {
				let row_count = ctx.take.unwrap_or(ctx.row_count);
				Ok(ColumnWithName {
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
						columns,
					}) if columns.is_scalar() => {
						let value = columns.scalar_value();
						let mut data =
							ColumnBuffer::with_capacity(value.get_type(), ctx.row_count);
						for _ in 0..ctx.row_count {
							data.push_value(value.clone());
						}
						Ok(ColumnWithName {
							name: Fragment::internal(variable_name),
							data,
						})
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
						// Fallback: check named params (for remote pushdown)
						if let Some(value) = ctx.params.get_named(variable_name) {
							let mut data = ColumnBuffer::with_capacity(
								value.get_type(),
								ctx.row_count,
							);
							for _ in 0..ctx.row_count {
								data.push_value(value.clone());
							}
							return Ok(ColumnWithName {
								name: Fragment::internal(variable_name),
								data,
							});
						}
						Err(TypeError::Runtime {
							kind: RuntimeErrorKind::VariableNotFound {
								name: variable_name.to_string(),
							},
							message: format!("Variable '{}' is not defined", variable_name),
						}
						.into())
					}
				}
			})
		}

		Expression::Parameter(e) => {
			let expr = e.clone();
			CompiledExpr::new(move |ctx| parameter_lookup(ctx, &expr))
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

		Expression::Add(e) => compile_arith!(_ctx, e, add_columns),
		Expression::Sub(e) => compile_arith!(_ctx, e, sub_columns),
		Expression::Mul(e) => compile_arith!(_ctx, e, mul_columns),
		Expression::Div(e) => compile_arith!(_ctx, e, div_columns),
		Expression::Rem(e) => compile_arith!(_ctx, e, rem_columns),

		Expression::Equal(e) => compile_compare!(_ctx, e, Equal, BinaryOp::Equal),
		Expression::NotEqual(e) => compile_compare!(_ctx, e, NotEqual, BinaryOp::NotEqual),
		Expression::GreaterThan(e) => compile_compare!(_ctx, e, GreaterThan, BinaryOp::GreaterThan),
		Expression::GreaterThanEqual(e) => {
			compile_compare!(_ctx, e, GreaterThanEqual, BinaryOp::GreaterThanEqual)
		}
		Expression::LessThan(e) => compile_compare!(_ctx, e, LessThan, BinaryOp::LessThan),
		Expression::LessThanEqual(e) => compile_compare!(_ctx, e, LessThanEqual, BinaryOp::LessThanEqual),

		Expression::And(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				execute_logical_op(&l, &r, &fragment, LogicalOp::And, |a, b| a && b)
			})
		}

		Expression::Or(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				execute_logical_op(&l, &r, &fragment, LogicalOp::Or, |a, b| a || b)
			})
		}

		Expression::Xor(e) => {
			let left = compile_expression(_ctx, &e.left)?;
			let right = compile_expression(_ctx, &e.right)?;
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let l = left.execute(ctx)?;
				let r = right.execute(ctx)?;
				execute_logical_op(&l, &r, &fragment, LogicalOp::Xor, |a, b| a != b)
			})
		}

		Expression::Prefix(e) => {
			let inner = compile_expression(_ctx, &e.expression)?;
			let operator = e.operator.clone();
			let fragment = e.full_fragment_owned();
			CompiledExpr::new(move |ctx| {
				let column = inner.execute(ctx)?;
				prefix_apply(&column, &operator, &fragment)
			})
		}

		Expression::Type(e) => {
			let ty = e.ty.clone();
			let fragment = e.fragment.clone();
			CompiledExpr::new(move |ctx| {
				let row_count = ctx.take.unwrap_or(ctx.row_count);
				let values: Vec<Box<Value>> =
					(0..row_count).map(|_| Box::new(Value::Type(ty.clone()))).collect();
				Ok(ColumnWithName::new(fragment.text(), ColumnBuffer::any(values)))
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
					let columns: Vec<ColumnWithName> = compiled
						.iter()
						.map(|expr| expr.execute(ctx))
						.collect::<Result<Vec<_>>>()?;

					let len = columns.first().map_or(1, |c| c.data().len());
					let mut data: Vec<Box<Value>> = Vec::with_capacity(len);

					for i in 0..len {
						let items: Vec<Value> =
							columns.iter().map(|col| col.data().get_value(i)).collect();
						data.push(Box::new(Value::Tuple(items)));
					}

					Ok(ColumnWithName::new(fragment.clone(), ColumnBuffer::any(data)))
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
				let columns: Vec<ColumnWithName> =
					compiled.iter().map(|expr| expr.execute(ctx)).collect::<Result<Vec<_>>>()?;

				let len = columns.first().map_or(1, |c| c.data().len());
				let mut data: Vec<Box<Value>> = Vec::with_capacity(len);

				for i in 0..len {
					let items: Vec<Value> =
						columns.iter().map(|col| col.data().get_value(i)).collect();
					data.push(Box::new(Value::List(items)));
				}

				Ok(ColumnWithName::new(fragment.clone(), ColumnBuffer::any(data)))
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

				if !matches!(ge_result.data(), ColumnBuffer::Bool(_))
					|| !matches!(le_result.data(), ColumnBuffer::Bool(_))
				{
					return Err(TypeError::BinaryOperatorNotApplicable {
						operator: BinaryOp::Between,
						left: value_col.get_type(),
						right: lower_col.get_type(),
						fragment: fragment.clone(),
					}
					.into());
				}

				match (ge_result.data(), le_result.data()) {
					(ColumnBuffer::Bool(ge_container), ColumnBuffer::Bool(le_container)) => {
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

						Ok(ColumnWithName {
							name: fragment.clone(),
							data: ColumnBuffer::bool_with_bitvec(data, bitvec),
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
					let len = value_col.data().len();
					let result = vec![negated; len];
					return Ok(ColumnWithName::new(fragment.clone(), ColumnBuffer::bool(result)));
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
					result = combine_bool_columns(result, eq_result, fragment.clone(), |l, r| {
						l || r
					})?;
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

				// Empty list → vacuous truth (all elements trivially contained)
				if list.is_empty() {
					let len = value_col.data().len();
					let result = vec![true; len];
					return Ok(ColumnWithName::new(fragment.clone(), ColumnBuffer::bool(result)));
				}

				// For each list element, check if it's contained in the set value
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
					Ok(ColumnWithName::new(const_expr.full_fragment_owned(), casted))
				})
			} else {
				let inner = compile_expression(_ctx, &e.expression)?;
				let target_type = e.to.ty.clone();
				let inner_fragment = e.expression.full_fragment_owned();
				CompiledExpr::new(move |ctx| {
					let column = inner.execute(ctx)?;
					let frag = inner_fragment.clone();
					let casted = cast_column_data(ctx, column.data(), target_type.clone(), &|| {
						inner_fragment.clone()
					})
					.map_err(|e| {
						Error::from(CastError::InvalidNumber {
							fragment: frag,
							target: target_type.clone(),
							cause: e.diagnostic(),
						})
					})?;
					Ok(ColumnWithName::new(column.name_owned(), casted))
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
			let expr = e.clone();
			CompiledExpr::new(move |ctx| {
				let mut arg_columns = Vec::with_capacity(compiled_args.len());
				for compiled_arg in &compiled_args {
					arg_columns.push(compiled_arg.execute(ctx)?);
				}
				let arguments = Columns::new(arg_columns);
				call_builtin(ctx, &expr, arguments, ctx.functions)
			})
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
						ColumnBuffer::Uint1(container) => {
							let results: Vec<bool> = container
								.iter()
								.take(ctx.row_count)
								.map(|v| v == Some(tag))
								.collect();
							Ok(ColumnWithName::new(
								fragment.clone(),
								ColumnBuffer::bool(results),
							))
						}
						_ => Ok(ColumnWithName {
							name: fragment.clone(),
							data: ColumnBuffer::none_typed(Type::Boolean, ctx.row_count),
						}),
					}
				} else {
					Ok(ColumnWithName {
						name: fragment.clone(),
						data: ColumnBuffer::none_typed(Type::Boolean, ctx.row_count),
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
					match ctx.symbols.get(variable_name) {
						Some(Variable::Columns {
							columns,
						}) if !columns.is_scalar() => {
							let col = columns
								.columns
								.iter()
								.find(|c| c.name.text() == field_name);
							match col {
								Some(col) => {
									let value = col.data.get_value(0);
									let row_count =
										ctx.take.unwrap_or(ctx.row_count);
									let mut data = ColumnBuffer::with_capacity(
										value.get_type(),
										row_count,
									);
									for _ in 0..row_count {
										data.push_value(value.clone());
									}
									Ok(ColumnWithName {
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
									Err(TypeError::Runtime {
										kind: RuntimeErrorKind::FieldNotFound {
											variable: variable_name
												.to_string(),
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
							message: format!(
								"Field '{}' not found on variable '{}'",
								field_name, variable_name
							),
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
								name: variable_name.to_string(),
							},
							message: format!("Variable '{}' is not defined", variable_name),
						}
						.into()),
					}
				} else {
					// For non-variable objects, evaluate the object and try to interpret result
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

fn combine_bool_columns(
	left: ColumnWithName,
	right: ColumnWithName,
	fragment: Fragment,
	combine_fn: fn(bool, bool) -> bool,
) -> Result<ColumnWithName> {
	binary_op_unwrap_option(&left, &right, fragment.clone(), |left, right| match (left.data(), right.data()) {
		(ColumnBuffer::Bool(l), ColumnBuffer::Bool(r)) => {
			let len = l.len();
			let mut data = Vec::with_capacity(len);
			let mut bitvec = Vec::with_capacity(len);

			for i in 0..len {
				let l_defined = l.is_defined(i);
				let r_defined = r.is_defined(i);
				let l_val = l.data().get(i);
				let r_val = r.data().get(i);

				if l_defined && r_defined {
					data.push(combine_fn(l_val, r_val));
					bitvec.push(true);
				} else {
					data.push(false);
					bitvec.push(false);
				}
			}

			Ok(ColumnWithName {
				name: fragment.clone(),
				data: ColumnBuffer::bool_with_bitvec(data, bitvec),
			})
		}
		_ => {
			unreachable!("combine_bool_columns should only be called with boolean columns")
		}
	})
}

fn list_items_contain(items: &[Value], element: &Value, fragment: &Fragment) -> bool {
	items.iter().any(|item| {
		if item == element {
			return true;
		}
		let item_col = ColumnWithName::new(fragment.clone(), ColumnBuffer::from(item.clone()));
		let elem_col = ColumnWithName::new(fragment.clone(), ColumnBuffer::from(element.clone()));
		compare_columns::<Equal>(&item_col, &elem_col, fragment.clone(), |f, l, r| {
			TypeError::BinaryOperatorNotApplicable {
				operator: BinaryOp::Equal,
				left: l,
				right: r,
				fragment: f,
			}
			.into_diagnostic()
		})
		.ok()
		.and_then(|c| match c.data() {
			ColumnBuffer::Bool(b) => Some(b.data().get(0)),
			_ => None,
		})
		.unwrap_or(false)
	})
}

fn list_contains_element(
	list_col: &ColumnWithName,
	element_col: &ColumnWithName,
	fragment: &Fragment,
) -> Result<ColumnWithName> {
	let len = list_col.data().len();
	let mut data = Vec::with_capacity(len);

	for i in 0..len {
		let list_value = list_col.data().get_value(i);
		let element_value = element_col.data().get_value(i);

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

	Ok(ColumnWithName::new(fragment.clone(), ColumnBuffer::bool(data)))
}

fn negate_column(col: ColumnWithName, fragment: Fragment) -> ColumnWithName {
	unary_op_unwrap_option(&col, |col| match col.data() {
		ColumnBuffer::Bool(container) => {
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

			Ok(ColumnWithName {
				name: fragment.clone(),
				data: ColumnBuffer::bool_with_bitvec(data, bitvec),
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
) -> Result<Vec<ColumnWithName>> {
	let condition_column = condition.execute(ctx)?;

	let mut result_data: Option<Vec<ColumnBuffer>> = None;
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
			let mut data: Vec<ColumnBuffer> = branch_results
				.iter()
				.map(|col| ColumnBuffer::with_capacity(col.data().get_type(), ctx.row_count))
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
	let result: Vec<ColumnWithName> = result_data
		.into_iter()
		.enumerate()
		.map(|(i, data)| ColumnWithName {
			name: result_names.get(i).cloned().unwrap_or_else(|| Fragment::internal("column")),
			data,
		})
		.collect();

	if result.is_empty() {
		Ok(vec![ColumnWithName {
			name: Fragment::internal("none"),
			data: ColumnBuffer::none_typed(Type::Boolean, ctx.row_count),
		}])
	} else {
		Ok(result)
	}
}

fn execute_multi_exprs(ctx: &EvalContext, exprs: &[CompiledExpr]) -> Result<Vec<ColumnWithName>> {
	let mut result = Vec::new();
	for expr in exprs {
		result.extend(expr.execute_multi(ctx)?);
	}
	Ok(result)
}

fn execute_projection_multi(ctx: &EvalContext, expressions: &[CompiledExpr]) -> Result<Vec<ColumnWithName>> {
	let mut result = Vec::with_capacity(expressions.len());

	for expr in expressions {
		let column = expr.execute(ctx)?;
		let name = column.name.text().to_string();
		result.push(ColumnWithName::new(Fragment::internal(name), column.data));
	}

	Ok(result)
}
