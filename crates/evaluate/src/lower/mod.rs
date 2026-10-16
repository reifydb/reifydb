// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod arith;
mod cast;
mod compare;
mod conditional;
mod construct;
mod contains;
mod direct;
mod error;
mod field_access;
mod in_list;
mod literal;
mod prefix;
mod routine;
mod schema;
mod variant;
mod xor;

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions, new_empty_array};
use arrow_schema::{FieldRef, SchemaRef};
use datafusion_expr::{
	BinaryExpr, Expr, Operator, ScalarUDF, ScalarUDFImpl, Signature, Volatility, execution_props::ExecutionProps,
	expr::ScalarFunction, physical_planning_context::PhysicalPlanningContext,
};
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use reifydb_core::{
	expression::{Expression, PrefixExpression, PrefixOperator, name::display_label},
	internal_error,
	value::column::{arith::ArithOp, factory::default_typed},
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{
	error::{BinaryOp, Diagnostic, Error, LogicalOp},
	fragment::Fragment,
	value::{
		column_view::ColumnView,
		value_type::{
			ValueType,
			field::{FieldType, from_field, to_field},
		},
	},
};

use crate::{
	Result,
	expression::{context::EvalContext, logic::execute_logical_op, prefix::prefix_apply},
	lower::{
		cast::lower_cast,
		direct::{Direct, Spliced},
		error::from_datafusion,
		schema::positional_schema,
	},
};

pub struct LoweredExpr {
	expression: Expression,
	operator: &'static str,
	state: Mutex<Option<Arc<Ready>>>,
}

struct Ready {
	schema: SchemaRef,
	plan: Plan,
}

enum Plan {
	Lowered {
		expr: Arc<dyn PhysicalExpr>,
		field: FieldRef,
	},
	Direct(Direct),
	Spliced(Spliced),
	Failed(Box<Diagnostic>),
}

struct Node {
	expr: Expr,
	field: FieldRef,
}

enum Stop {
	RowChanging,
	Error(Error),
}

impl From<Error> for Stop {
	fn from(error: Error) -> Self {
		Stop::Error(error)
	}
}

type Lowered<T> = std::result::Result<T, Stop>;

pub fn compile_check(expression: &Expression) -> Result<()> {
	match variant::compile_time_error(expression) {
		Some(error) => Err(error),
		None => Ok(()),
	}
}

impl LoweredExpr {
	pub fn new(expression: Expression, operator: &'static str) -> Self {
		Self {
			expression,
			operator,
			state: Mutex::new(None),
		}
	}

	pub fn access_column_name(&self) -> Option<&str> {
		match &self.expression {
			Expression::AccessSource(access) => Some(access.column.name.text()),
			_ => None,
		}
	}

	pub fn evaluate(&self, ctx: &EvalContext) -> Result<(FieldRef, ArrayRef)> {
		let ready = self.ready(ctx);
		match &ready.plan {
			Plan::Lowered {
				expr,
				field,
			} => {
				let sized;
				let batch = if ctx.batch.num_columns() == 0 && ctx.batch.num_rows() != ctx.row_count {
					sized = RecordBatch::try_new_with_options(
						ctx.batch.schema(),
						vec![],
						&RecordBatchOptions::new().with_row_count(Some(ctx.row_count)),
					)
					.map_err(|err| from_datafusion(self.operator, err.into()))?;
					&sized
				} else {
					&ctx.batch
				};
				let value = expr.evaluate(batch).map_err(|err| from_datafusion(self.operator, err))?;
				let array = value
					.into_array(ctx.row_count)
					.map_err(|err| from_datafusion(self.operator, err))?;
				let array = match ctx.take {
					Some(take) if take < array.len() => array.slice(0, take),
					_ => array,
				};
				if !field.is_nullable() && array.logical_null_count() > 0 {
					let mut field_type = from_field(field)?;
					field_type.value_type = field_type
						.value_type
						.map(|value_type| ValueType::Option(Box::new(value_type)));
					return Ok((Arc::new(to_field(field.name(), &field_type)), array));
				}
				Ok((field.clone(), array))
			}
			Plan::Direct(direct) => direct.evaluate(ctx),
			Plan::Spliced(spliced) => spliced.evaluate(ctx),
			Plan::Failed(diagnostic) => Err(Error(diagnostic.clone())),
		}
	}

	fn ready(&self, ctx: &EvalContext) -> Arc<Ready> {
		let schema = ctx.batch.schema();
		let mut state = self.state.lock();
		if let Some(ready) = state.as_ref()
			&& same_schema(&ready.schema, &schema)
		{
			return ready.clone();
		}
		let ready = Arc::new(Ready {
			plan: self.plan(ctx),
			schema,
		});
		if !matches!(ready.plan, Plan::Failed(_)) {
			*state = Some(ready.clone());
		}
		ready
	}

	fn plan(&self, ctx: &EvalContext) -> Plan {
		match lower_root(ctx, self.operator, &self.expression) {
			Ok((expr, field)) => Plan::Lowered {
				expr,
				field,
			},
			Err(Stop::Error(error)) => Plan::Failed(error.0),
			Err(Stop::RowChanging) => {
				let planned = match &self.expression {
					Expression::Call(_) => {
						Direct::new(ctx, self.operator, &self.expression).map(Plan::Direct)
					}
					Expression::Alias(alias)
						if matches!(alias.expression.as_ref(), Expression::Call(_)) =>
					{
						Direct::new(ctx, self.operator, &self.expression).map(Plan::Direct)
					}
					_ => Spliced::new(ctx, self.operator, &self.expression).map(Plan::Spliced),
				};
				planned.unwrap_or_else(|error| Plan::Failed(error.0))
			}
		}
	}
}

fn same_schema(left: &SchemaRef, right: &SchemaRef) -> bool {
	Arc::ptr_eq(left, right) || left == right
}

fn lower_root<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
) -> Lowered<(Arc<dyn PhysicalExpr>, FieldRef)> {
	if let Some(error) = variant::compile_time_error(expression) {
		return Err(Stop::Error(error));
	}
	let node = lower_node(ctx, operator, expression)?;
	let schema = positional_schema(operator, ctx.batch.schema_ref())?;
	let expr =
		create_physical_expr(&node.expr, &schema, &ExecutionProps::new(), &PhysicalPlanningContext::default())
			.map_err(|err| from_datafusion(operator, err))?;
	Ok((expr, node.field))
}

fn lower_node<'e>(ctx: &EvalContext, operator: &'static str, expression: &'e Expression) -> Lowered<Node> {
	match expression {
		Expression::Column(column) => Ok(schema::column(ctx, operator, column)?),
		Expression::AccessSource(access) => Ok(schema::access(ctx, access)?),
		Expression::Constant(constant) => {
			Ok(literal::constant(operator, constant, display_label(expression).text())?)
		}
		Expression::Prefix(prefix) => match prefix.operator {
			PrefixOperator::Not(_) => lower_not(ctx, operator, expression, prefix),
			PrefixOperator::Minus(_) | PrefixOperator::Plus(_) => {
				prefix::lower_prefix(ctx, operator, expression, prefix)
			}
		},
		Expression::And(and) => lower_logical(
			ctx,
			operator,
			expression,
			(&and.left, &and.right),
			&and.full_fragment_owned(),
			(LogicalOp::And, Operator::And),
		),
		Expression::Or(or) => lower_logical(
			ctx,
			operator,
			expression,
			(&or.left, &or.right),
			&or.full_fragment_owned(),
			(LogicalOp::Or, Operator::Or),
		),
		Expression::Equal(e) => compare::lower_compare(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			(BinaryOp::Equal, Operator::Eq),
		),
		Expression::NotEqual(e) => compare::lower_compare(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			(BinaryOp::NotEqual, Operator::NotEq),
		),
		Expression::LessThan(e) => compare::lower_compare(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			(BinaryOp::LessThan, Operator::Lt),
		),
		Expression::LessThanEqual(e) => compare::lower_compare(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			(BinaryOp::LessThanEqual, Operator::LtEq),
		),
		Expression::GreaterThan(e) => compare::lower_compare(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			(BinaryOp::GreaterThan, Operator::Gt),
		),
		Expression::GreaterThanEqual(e) => compare::lower_compare(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			(BinaryOp::GreaterThanEqual, Operator::GtEq),
		),
		Expression::Between(e) => compare::lower_between(ctx, operator, expression, e),
		Expression::Add(e) => arith::lower_arith(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			ArithOp::Add,
		),
		Expression::Sub(e) => arith::lower_arith(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			ArithOp::Sub,
		),
		Expression::Mul(e) => arith::lower_arith(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			ArithOp::Mul,
		),
		Expression::Div(e) => arith::lower_arith(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			ArithOp::Div,
		),
		Expression::Rem(e) => arith::lower_arith(
			ctx,
			operator,
			expression,
			(&e.left, &e.right),
			e.full_fragment_owned(),
			ArithOp::Rem,
		),
		Expression::Call(call) => routine::lower_call(ctx, operator, expression, call),
		Expression::Variable(variable) => Ok(literal::variable(ctx, operator, variable)?),
		Expression::Parameter(parameter) => Ok(literal::parameter(ctx, operator, parameter)?),
		Expression::Type(ty) => Ok(literal::type_literal(operator, &ty.ty, &ty.fragment)?),
		Expression::FieldAccess(access) => match access.object.as_ref() {
			Expression::Variable(variable) => {
				Ok(literal::variable_field(ctx, operator, variable, access.field.text())?)
			}
			_ => field_access::lower_field_access(ctx, operator, expression, access),
		},
		Expression::Alias(alias) => {
			let inner = lower_node(ctx, operator, &alias.expression)?;
			Ok(Node {
				expr: inner.expr,
				field: Arc::new(inner.field.as_ref().clone().with_name(alias.alias.name())),
			})
		}
		Expression::Cast(cast) => lower_cast(ctx, operator, expression, cast),
		Expression::In(e) => in_list::lower_in(ctx, operator, expression, e),
		Expression::If(e) => conditional::lower_if(ctx, operator, expression, e),
		Expression::Xor(e) => xor::lower_xor(ctx, operator, expression, e),
		Expression::Tuple(e) => {
			construct::lower_construct(ctx, operator, construct::Shape::Tuple, &e.expressions, &e.fragment)
		}
		Expression::List(e) => {
			construct::lower_construct(ctx, operator, construct::Shape::List, &e.expressions, &e.fragment)
		}
		Expression::Map(e) => construct::lower_projection(ctx, operator, &e.expressions, &e.fragment),
		Expression::Extend(e) => construct::lower_projection(ctx, operator, &e.expressions, &e.fragment),
		Expression::Contains(e) => contains::lower_contains(ctx, operator, expression, e),
		Expression::IsVariant(e) => Ok(variant::lower_is_variant(ctx, operator, e)?),
		Expression::SumTypeConstructor(_) => Err(Stop::Error(
			variant::compile_time_error(expression).expect("a sum type constructor is a compile error"),
		)),
	}
}

fn lower_logical<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	(left, right): (&'e Expression, &'e Expression),
	fragment: &Fragment,
	(logical_op, df_op): (LogicalOp, Operator),
) -> Lowered<Node> {
	let left = lower_node(ctx, operator, left)?;
	let right = lower_node(ctx, operator, right)?;
	execute_logical_op(&logic_probe(&left.field)?, &logic_probe(&right.field)?, fragment, logical_op)?;
	let nullable = left.field.is_nullable() || right.field.is_nullable();
	Ok(Node {
		expr: Expr::BinaryExpr(BinaryExpr::new(
			Box::new(bool_operand(left)?),
			df_op,
			Box::new(bool_operand(right)?),
		)),
		field: bool_field(display_label(expression).text(), nullable),
	})
}

fn lower_not<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	prefix: &'e PrefixExpression,
) -> Lowered<Node> {
	let inner = lower_node(ctx, operator, &prefix.expression)?;
	if !is_untyped_none(&inner.field)? {
		let fragment = prefix.full_fragment_owned();
		prefix_apply(&empty_of(&inner.field), &prefix.operator, &fragment)?;
		if let Some(value_type) = from_field(&inner.field)?.value_type {
			prefix_apply(&default_typed("probe", value_type, 1), &prefix.operator, &fragment)?;
		}
	}
	let nullable = inner.field.is_nullable();
	Ok(Node {
		expr: Expr::Not(Box::new(bool_operand(inner)?)),
		field: bool_field(display_label(expression).text(), nullable),
	})
}

fn logic_probe(field: &FieldRef) -> Result<(FieldRef, ArrayRef)> {
	if is_untyped_none(field)? {
		return Ok(empty_of(&bool_field("none", true)));
	}
	Ok(empty_of(&Arc::new(field.as_ref().clone().with_nullable(false))))
}

fn bool_operand(node: Node) -> Result<Expr> {
	if is_untyped_none(&node.field)? {
		return Ok(literal::none_bool());
	}
	Ok(node.expr)
}

fn is_untyped_none(field: &FieldRef) -> Result<bool> {
	Ok(ColumnView::try_from(&empty_of(field))?.is_untyped_none())
}

fn inner_type(field: &FieldRef) -> Result<ValueType> {
	match from_field(field)?.value_type {
		Some(value_type) => Ok(value_type.inner_type().clone()),
		None => Err(internal_error!("operand {} has no value type", field.name())),
	}
}

fn empty_of(field: &FieldRef) -> (FieldRef, ArrayRef) {
	(field.clone(), new_empty_array(field.data_type()))
}

fn bool_field(name: &str, nullable: bool) -> FieldRef {
	typed_field(name, ValueType::Boolean, nullable)
}

fn typed_field(name: &str, value_type: ValueType, nullable: bool) -> FieldRef {
	let value_type = match nullable {
		true => ValueType::Option(Box::new(value_type)),
		false => value_type,
	};
	Arc::new(to_field(name, &FieldType::from(value_type)))
}

fn udf(function: impl ScalarUDFImpl + 'static, args: Vec<Expr>) -> Expr {
	Expr::ScalarFunction(ScalarFunction::new_udf(Arc::new(ScalarUDF::new_from_impl(function)), args))
}

fn volatile() -> Signature {
	Signature::user_defined(Volatility::Volatile)
}
