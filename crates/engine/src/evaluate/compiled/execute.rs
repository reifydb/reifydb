// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::value::column::{Column, data::ColumnData};
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
		runtime::{variable_is_dataframe, variable_not_found},
	},
	fragment::Fragment,
	return_error,
	value::{Value, container::undefined::UndefinedContainer, r#type::Type},
};

use super::{context::ExecContext, expr::CompiledExpr};
use crate::{
	evaluate::column::{
		arith::{add::add_columns, div::div_columns, mul::mul_columns, rem::rem_columns, sub::sub_columns},
		call::call_eval,
		cast::cast_column_data,
		compare::{Equal, GreaterThan, GreaterThanEqual, LessThan, LessThanEqual, NotEqual, compare_columns},
		constant::{constant_value, constant_value_of},
	},
	vm::stack::Variable,
};

impl CompiledExpr {
	pub fn execute(&self, ctx: &ExecContext) -> crate::Result<Column> {
		match self {
			CompiledExpr::Constant(expr) => {
				let row_count = ctx.take.unwrap_or(ctx.row_count);
				Ok(Column {
					name: expr.full_fragment_owned(),
					data: constant_value(expr, row_count)?,
				})
			}

			// Leaf nodes
			CompiledExpr::Column(expr) => {
				let eval_ctx = ctx.to_column_eval_ctx();
				crate::evaluate::column::column::column_lookup(&eval_ctx, expr)
			}

			CompiledExpr::Variable(expr) => {
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
					}) => {
						return_error!(variable_is_dataframe(variable_name));
					}
					None => {
						return_error!(variable_not_found(variable_name));
					}
				}
			}

			CompiledExpr::Parameter(expr) => {
				let eval_ctx = ctx.to_column_eval_ctx();
				crate::evaluate::column::parameter::parameter_lookup(&eval_ctx, expr)
			}

			// Unary
			CompiledExpr::Alias {
				inner,
				alias,
			} => {
				let mut column = inner.execute(ctx)?;
				column.name = alias.clone();
				Ok(column)
			}

			// Binary arithmetic
			CompiledExpr::Add {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				add_columns(&eval_ctx, &left_col, &right_col, || fragment.clone())
			}
			CompiledExpr::Sub {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				sub_columns(&eval_ctx, &left_col, &right_col, || fragment.clone())
			}
			CompiledExpr::Mul {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				mul_columns(&eval_ctx, &left_col, &right_col, || fragment.clone())
			}
			CompiledExpr::Div {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				div_columns(&eval_ctx, &left_col, &right_col, || fragment.clone())
			}
			CompiledExpr::Rem {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				rem_columns(&eval_ctx, &left_col, &right_col, || fragment.clone())
			}

			// Comparisons
			CompiledExpr::Equal {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				compare_columns::<Equal>(
					&eval_ctx,
					&left_col,
					&right_col,
					fragment.clone(),
					equal_cannot_be_applied_to_incompatible_types,
				)
			}
			CompiledExpr::NotEqual {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				compare_columns::<NotEqual>(
					&eval_ctx,
					&left_col,
					&right_col,
					fragment.clone(),
					not_equal_cannot_be_applied_to_incompatible_types,
				)
			}
			CompiledExpr::GreaterThan {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				compare_columns::<GreaterThan>(
					&eval_ctx,
					&left_col,
					&right_col,
					fragment.clone(),
					greater_than_cannot_be_applied_to_incompatible_types,
				)
			}
			CompiledExpr::GreaterThanEqual {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				compare_columns::<GreaterThanEqual>(
					&eval_ctx,
					&left_col,
					&right_col,
					fragment.clone(),
					greater_than_equal_cannot_be_applied_to_incompatible_types,
				)
			}
			CompiledExpr::LessThan {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				compare_columns::<LessThan>(
					&eval_ctx,
					&left_col,
					&right_col,
					fragment.clone(),
					less_than_cannot_be_applied_to_incompatible_types,
				)
			}
			CompiledExpr::LessThanEqual {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				compare_columns::<LessThanEqual>(
					&eval_ctx,
					&left_col,
					&right_col,
					fragment.clone(),
					less_than_equal_cannot_be_applied_to_incompatible_types,
				)
			}

			// Logic
			CompiledExpr::And {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				execute_and(&left_col, &right_col, fragment)
			}
			CompiledExpr::Or {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				execute_or(&left_col, &right_col, fragment)
			}
			CompiledExpr::Xor {
				left,
				right,
				fragment,
			} => {
				let left_col = left.execute(ctx)?;
				let right_col = right.execute(ctx)?;
				execute_xor(&left_col, &right_col, fragment)
			}

			// Prefix
			CompiledExpr::Prefix(expr) => {
				let eval_ctx = ctx.to_column_eval_ctx();
				crate::evaluate::column::prefix::prefix_eval(&eval_ctx, expr, ctx.functions, ctx.clock)
			}

			// Type
			CompiledExpr::Type {
				ty,
				fragment,
			} => {
				let row_count = ctx.take.unwrap_or(ctx.row_count);
				let values: Vec<Box<Value>> =
					(0..row_count).map(|_| Box::new(Value::Type(*ty))).collect();
				Ok(Column::new(fragment.text(), ColumnData::any(values)))
			}

			// AccessSource
			CompiledExpr::AccessSource(expr) => {
				let eval_ctx = ctx.to_column_eval_ctx();
				crate::evaluate::column::access::access_lookup(&eval_ctx, expr)
			}

			// Tuple (single-element)
			CompiledExpr::Tuple {
				inner,
			} => inner.execute(ctx),

			// Between
			CompiledExpr::Between {
				value,
				lower,
				upper,
				fragment,
			} => {
				let value_col = value.execute(ctx)?;
				let lower_col = lower.execute(ctx)?;
				let upper_col = upper.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();

				let ge_result = compare_columns::<GreaterThanEqual>(
					&eval_ctx,
					&value_col,
					&lower_col,
					fragment.clone(),
					greater_than_equal_cannot_be_applied_to_incompatible_types,
				)?;
				let le_result = compare_columns::<LessThanEqual>(
					&eval_ctx,
					&value_col,
					&upper_col,
					fragment.clone(),
					less_than_equal_cannot_be_applied_to_incompatible_types,
				)?;

				if matches!(ge_result.data(), ColumnData::Undefined(_))
					|| matches!(le_result.data(), ColumnData::Undefined(_))
				{
					let len = ge_result.data().len();
					return Ok(Column {
						name: fragment.clone(),
						data: ColumnData::Undefined(UndefinedContainer::new(len)),
					});
				}

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
			}

			// In
			CompiledExpr::In {
				value,
				list,
				negated,
				fragment,
			} => {
				if list.is_empty() {
					let value_col = value.execute(ctx)?;
					let len = value_col.data().len();
					let result = vec![*negated; len];
					return Ok(Column {
						name: fragment.clone(),
						data: ColumnData::bool(result),
					});
				}

				let value_col = value.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();

				let first_col = list[0].execute(ctx)?;
				let mut result = compare_columns::<Equal>(
					&eval_ctx,
					&value_col,
					&first_col,
					fragment.clone(),
					equal_cannot_be_applied_to_incompatible_types,
				)?;

				for list_expr in list.iter().skip(1) {
					let list_col = list_expr.execute(ctx)?;
					let eq_result = compare_columns::<Equal>(
						&eval_ctx,
						&value_col,
						&list_col,
						fragment.clone(),
						equal_cannot_be_applied_to_incompatible_types,
					)?;
					result = or_columns(result, eq_result, fragment.clone())?;
				}

				if *negated {
					result = negate_column(result, fragment.clone());
				}

				Ok(result)
			}

			// Cast
			CompiledExpr::Cast {
				inner,
				target_type,
				inner_fragment,
			} => {
				// Optimization: constant values can be directly created with the target type
				if let CompiledExpr::Constant(const_expr) = inner.as_ref() {
					let row_count = ctx.take.unwrap_or(ctx.row_count);
					let data = constant_value(const_expr, row_count)?;
					let casted = if data.get_type() == *target_type {
						data
					} else {
						constant_value_of(const_expr, *target_type, row_count)?
					};
					return Ok(Column {
						name: const_expr.full_fragment_owned(),
						data: casted,
					});
				}

				let column = inner.execute(ctx)?;
				let eval_ctx = ctx.to_column_eval_ctx();
				let frag = inner_fragment.clone();
				let casted = cast_column_data(&eval_ctx, &column.data(), *target_type, &|| {
					inner_fragment.clone()
				})
				.map_err(|e| error!(cast::invalid_number(frag, *target_type, e.diagnostic())))?;
				Ok(Column {
					name: column.name_owned(),
					data: casted,
				})
			}

			// If
			CompiledExpr::If {
				condition,
				then_expr,
				else_ifs,
				else_branch,
				fragment,
			} => {
				let columns =
					execute_if_multi(ctx, condition, then_expr, else_ifs, else_branch, fragment)?;
				Ok(columns.into_iter().next().unwrap_or_else(|| Column {
					name: Fragment::internal("undefined"),
					data: ColumnData::with_capacity(Type::Undefined, 0),
				}))
			}

			// Map (single column)
			CompiledExpr::Map {
				expressions,
			} => {
				if expressions.len() == 1 {
					return expressions[0].execute(ctx);
				}
				let columns = execute_map_multi(ctx, expressions)?;
				Ok(columns.into_iter().next().unwrap())
			}

			// Extend (single column)
			CompiledExpr::Extend {
				expressions,
			} => {
				if expressions.len() == 1 {
					return expressions[0].execute(ctx);
				}
				let columns = execute_extend_multi(ctx, expressions)?;
				Ok(columns.into_iter().next().unwrap())
			}

			// Call
			CompiledExpr::Call(expr) => {
				let eval_ctx = ctx.to_column_eval_ctx();
				call_eval(&eval_ctx, expr, ctx.functions, ctx.clock)
			}
		}
	}

	pub fn execute_multi(&self, ctx: &ExecContext) -> crate::Result<Vec<Column>> {
		match self {
			CompiledExpr::If {
				condition,
				then_expr,
				else_ifs,
				else_branch,
				fragment,
			} => execute_if_multi(ctx, condition, then_expr, else_ifs, else_branch, fragment),
			CompiledExpr::Map {
				expressions,
			} => execute_map_multi(ctx, expressions),
			CompiledExpr::Extend {
				expressions,
			} => execute_extend_multi(ctx, expressions),
			_ => Ok(vec![self.execute(ctx)?]),
		}
	}
}

fn execute_and(left: &Column, right: &Column, fragment: &Fragment) -> crate::Result<Column> {
	match (&left.data(), &right.data()) {
		(ColumnData::Bool(l_container), ColumnData::Bool(r_container)) => {
			if l_container.is_fully_defined() && r_container.is_fully_defined() {
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
			} else {
				let mut data = Vec::with_capacity(l_container.data().len());
				let mut bitvec = Vec::with_capacity(l_container.bitvec().len());

				for i in 0..l_container.data().len() {
					match (l_container.get(i), r_container.get(i)) {
						(Some(l), Some(r)) => {
							data.push(l && r);
							bitvec.push(true);
						}
						_ => {
							data.push(false);
							bitvec.push(false);
						}
					}
				}

				Ok(Column {
					name: fragment.clone(),
					data: ColumnData::bool_with_bitvec(data, bitvec),
				})
			}
		}
		(ColumnData::Undefined(container), _) => Ok(Column {
			name: fragment.clone(),
			data: ColumnData::Undefined(container.clone()),
		}),
		(_, ColumnData::Undefined(container)) => Ok(Column {
			name: fragment.clone(),
			data: ColumnData::Undefined(container.clone()),
		}),
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
}

fn execute_or(left: &Column, right: &Column, fragment: &Fragment) -> crate::Result<Column> {
	match (&left.data(), &right.data()) {
		(ColumnData::Bool(l_container), ColumnData::Bool(r_container)) => {
			if l_container.is_fully_defined() && r_container.is_fully_defined() {
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
			} else {
				let mut data = Vec::with_capacity(l_container.data().len());
				let mut bitvec = Vec::with_capacity(l_container.bitvec().len());

				for i in 0..l_container.data().len() {
					match (l_container.get(i), r_container.get(i)) {
						(Some(l), Some(r)) => {
							data.push(l || r);
							bitvec.push(true);
						}
						_ => {
							data.push(false);
							bitvec.push(false);
						}
					}
				}

				Ok(Column {
					name: fragment.clone(),
					data: ColumnData::bool_with_bitvec(data, bitvec),
				})
			}
		}
		(ColumnData::Undefined(container), _) => Ok(Column {
			name: fragment.clone(),
			data: ColumnData::Undefined(container.clone()),
		}),
		(_, ColumnData::Undefined(container)) => Ok(Column {
			name: fragment.clone(),
			data: ColumnData::Undefined(container.clone()),
		}),
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
}

fn execute_xor(left: &Column, right: &Column, fragment: &Fragment) -> crate::Result<Column> {
	match (&left.data(), &right.data()) {
		(ColumnData::Bool(l_container), ColumnData::Bool(r_container)) => {
			if l_container.is_fully_defined() && r_container.is_fully_defined() {
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
			} else {
				let mut data = Vec::with_capacity(l_container.data().len());
				let mut bitvec = Vec::with_capacity(l_container.bitvec().len());

				for i in 0..l_container.data().len() {
					if l_container.is_defined(i) && r_container.is_defined(i) {
						data.push(l_container.data().get(i) != r_container.data().get(i));
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
		}
		(ColumnData::Undefined(container), _) => Ok(Column {
			name: fragment.clone(),
			data: ColumnData::Undefined(container.clone()),
		}),
		(_, ColumnData::Undefined(container)) => Ok(Column {
			name: fragment.clone(),
			data: ColumnData::Undefined(container.clone()),
		}),
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
}

/// OR two boolean columns together with proper undefined handling for IN expressions.
fn or_columns(left: Column, right: Column, fragment: Fragment) -> crate::Result<Column> {
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
				name: fragment,
				data: ColumnData::bool_with_bitvec(data, bitvec),
			})
		}
		(ColumnData::Undefined(u), _) | (_, ColumnData::Undefined(u)) => {
			let len = u.len();
			let data = vec![false; len];
			let bitvec = vec![false; len];
			Ok(Column {
				name: fragment,
				data: ColumnData::bool_with_bitvec(data, bitvec),
			})
		}
		_ => {
			unreachable!("OR columns should only be called with boolean columns from equality comparisons")
		}
	}
}

/// Negate a boolean column. Undefined stays undefined.
fn negate_column(col: Column, fragment: Fragment) -> Column {
	match col.data() {
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

			Column {
				name: fragment,
				data: ColumnData::bool_with_bitvec(data, bitvec),
			}
		}
		ColumnData::Undefined(_) => col,
		_ => unreachable!("negate_column should only be called with boolean columns"),
	}
}

/// Evaluate if a value is truthy according to RQL semantics
fn is_truthy(value: &Value) -> bool {
	match value {
		Value::Boolean(true) => true,
		Value::Boolean(false) => false,
		Value::Undefined => false,
		Value::Int1(0) | Value::Int2(0) | Value::Int4(0) | Value::Int8(0) | Value::Int16(0) => false,
		Value::Uint1(0) | Value::Uint2(0) | Value::Uint4(0) | Value::Uint8(0) | Value::Uint16(0) => false,
		Value::Int1(_) | Value::Int2(_) | Value::Int4(_) | Value::Int8(_) | Value::Int16(_) => true,
		Value::Uint1(_) | Value::Uint2(_) | Value::Uint4(_) | Value::Uint8(_) | Value::Uint16(_) => true,
		Value::Utf8(s) => !s.is_empty(),
		_ => true,
	}
}

fn execute_if_multi(
	ctx: &ExecContext,
	condition: &CompiledExpr,
	then_expr: &[CompiledExpr],
	else_ifs: &[(Box<CompiledExpr>, Vec<CompiledExpr>)],
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
				let mut data = ColumnData::with_capacity(Type::Undefined, ctx.row_count);
				for _ in 0..ctx.row_count {
					data.push_undefined();
				}
				vec![Column {
					name: Fragment::internal("undefined"),
					data,
				}]
			}
		};

		if result_data.is_none() {
			result_data = Some(branch_results
				.iter()
				.map(|col| ColumnData::with_capacity(col.data().get_type(), ctx.row_count))
				.collect());
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
			name: Fragment::internal("undefined"),
			data: ColumnData::with_capacity(Type::Undefined, 0),
		}])
	} else {
		Ok(result)
	}
}

fn execute_multi_exprs(ctx: &ExecContext, exprs: &[CompiledExpr]) -> crate::Result<Vec<Column>> {
	let mut result = Vec::new();
	for expr in exprs {
		result.extend(expr.execute_multi(ctx)?);
	}
	Ok(result)
}

fn execute_map_multi(ctx: &ExecContext, expressions: &[CompiledExpr]) -> crate::Result<Vec<Column>> {
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

fn execute_extend_multi(ctx: &ExecContext, expressions: &[CompiledExpr]) -> crate::Result<Vec<Column>> {
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
