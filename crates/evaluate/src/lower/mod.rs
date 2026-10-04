// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod arith;
mod cast;
mod compare;
mod error;
mod literal;
mod routine;
mod schema;

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
	value::column::factory::default_typed,
};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::{
	error::{BinaryOp, Diagnostic, Error, LogicalOp},
	fragment::Fragment,
	reifydb_assertions,
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
	expression::{
		arith::ArithOp,
		compile::{CompiledExpr, compile_expression},
		context::{CompileContext, EvalContext},
		logic::execute_logical_op,
		prefix::prefix_apply,
	},
	lower::{cast::lower_cast, error::from_datafusion, schema::positional_schema},
};

pub const CLAIMED: &[&str] = &[
	"Column",
	"AccessSource",
	"Constant",
	"Prefix(Not)",
	"And",
	"Or",
	"Equal",
	"NotEqual",
	"LessThan",
	"LessThanEqual",
	"GreaterThan",
	"GreaterThanEqual",
	"Between",
	"Add",
	"Sub",
	"Mul",
	"Div",
	"Rem",
	"Call",
	"Variable",
	"FieldAccess(Variable)",
	"Alias",
	"Cast",
];

pub fn kind(expression: &Expression) -> &'static str {
	match expression {
		Expression::AccessSource(_) => "AccessSource",
		Expression::Alias(_) => "Alias",
		Expression::Cast(_) => "Cast",
		Expression::Constant(_) => "Constant",
		Expression::Column(_) => "Column",
		Expression::Add(_) => "Add",
		Expression::Div(_) => "Div",
		Expression::Call(_) => "Call",
		Expression::Rem(_) => "Rem",
		Expression::Mul(_) => "Mul",
		Expression::Sub(_) => "Sub",
		Expression::Tuple(_) => "Tuple",
		Expression::List(_) => "List",
		Expression::Prefix(prefix) => match prefix.operator {
			PrefixOperator::Minus(_) => "Prefix(Minus)",
			PrefixOperator::Plus(_) => "Prefix(Plus)",
			PrefixOperator::Not(_) => "Prefix(Not)",
		},
		Expression::GreaterThan(_) => "GreaterThan",
		Expression::GreaterThanEqual(_) => "GreaterThanEqual",
		Expression::LessThan(_) => "LessThan",
		Expression::LessThanEqual(_) => "LessThanEqual",
		Expression::Equal(_) => "Equal",
		Expression::NotEqual(_) => "NotEqual",
		Expression::Between(_) => "Between",
		Expression::And(_) => "And",
		Expression::Or(_) => "Or",
		Expression::Xor(_) => "Xor",
		Expression::In(_) => "In",
		Expression::Contains(_) => "Contains",
		Expression::Type(_) => "Type",
		Expression::Parameter(_) => "Parameter",
		Expression::Variable(_) => "Variable",
		Expression::If(_) => "If",
		Expression::Map(_) => "Map",
		Expression::Extend(_) => "Extend",
		Expression::SumTypeConstructor(_) => "SumTypeConstructor",
		Expression::IsVariant(_) => "IsVariant",
		Expression::FieldAccess(access) => match access.object.as_ref() {
			Expression::Variable(_) => "FieldAccess(Variable)",
			_ => "FieldAccess(Other)",
		},
	}
}

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
	Old(CompiledExpr),
	Failed(Box<Diagnostic>),
}

struct Node {
	expr: Expr,
	field: FieldRef,
}

enum Stop<'e> {
	Unsupported(&'e Expression),
	RowChanging,
	NonPropagating,
	Error(Error),
}

impl From<Error> for Stop<'_> {
	fn from(error: Error) -> Self {
		Stop::Error(error)
	}
}

type Lowered<'e, T> = std::result::Result<T, Stop<'e>>;

impl LoweredExpr {
	pub fn new(expression: Expression, operator: &'static str) -> Self {
		Self {
			expression,
			operator,
			state: Mutex::new(None),
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
				if !field.is_nullable() && array.logical_null_count() > 0 {
					let mut field_type = from_field(field)?;
					field_type.value_type = field_type
						.value_type
						.map(|value_type| ValueType::Option(Box::new(value_type)));
					return Ok((Arc::new(to_field(field.name(), &field_type)), array));
				}
				Ok((field.clone(), array))
			}
			Plan::Old(compiled) => compiled.execute(ctx),
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
		*state = Some(ready.clone());
		ready
	}

	fn plan(&self, ctx: &EvalContext) -> Plan {
		if let Some(node) = first_unclaimed(&self.expression) {
			assert_not_claimed(self.operator, node);
			return self.old_plan(ctx);
		}
		match lower_root(ctx, self.operator, &self.expression) {
			Ok((expr, field)) => Plan::Lowered {
				expr,
				field,
			},
			Err(Stop::Error(error)) => Plan::Failed(error.0),
			Err(Stop::Unsupported(node)) => {
				assert_not_claimed(self.operator, node);
				self.old_plan(ctx)
			}
			Err(Stop::RowChanging) | Err(Stop::NonPropagating) => self.old_plan(ctx),
		}
	}

	fn old_plan(&self, ctx: &EvalContext) -> Plan {
		let compile_ctx = CompileContext {
			symbols: ctx.symbols,
		};
		match compile_expression(&compile_ctx, &self.expression) {
			Ok(compiled) => Plan::Old(compiled),
			Err(error) => Plan::Failed(error.0),
		}
	}
}

fn first_unclaimed(expression: &Expression) -> Option<&Expression> {
	if !CLAIMED.contains(&kind(expression)) {
		return Some(expression);
	}
	match expression {
		Expression::Column(_)
		| Expression::AccessSource(_)
		| Expression::Constant(_)
		| Expression::Variable(_)
		| Expression::FieldAccess(_) => None,
		Expression::Alias(e) => first_unclaimed(&e.expression),
		Expression::Cast(e) => first_unclaimed(&e.expression),
		Expression::Prefix(e) => first_unclaimed(&e.expression),
		Expression::And(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::Or(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::Equal(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::NotEqual(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::LessThan(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::LessThanEqual(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::GreaterThan(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::GreaterThanEqual(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::Add(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::Sub(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::Mul(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::Div(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::Rem(e) => first_unclaimed(&e.left).or_else(|| first_unclaimed(&e.right)),
		Expression::Between(e) => first_unclaimed(&e.value)
			.or_else(|| first_unclaimed(&e.lower))
			.or_else(|| first_unclaimed(&e.upper)),
		Expression::Call(e) => e.args.iter().find_map(first_unclaimed),
		_ => Some(expression),
	}
}

#[cfg_attr(not(reifydb_assertions), allow(unused_variables))]
fn assert_not_claimed(operator: &str, node: &Expression) {
	reifydb_assertions! {
		let kind = kind(node);
		assert!(
			!CLAIMED.contains(&kind),
			"{operator}: claimed kind {kind} fell back at {:?}",
			node.full_fragment_owned().text()
		);
	}
}

fn same_schema(left: &SchemaRef, right: &SchemaRef) -> bool {
	Arc::ptr_eq(left, right) || left == right
}

fn lower_root<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
) -> Lowered<'e, (Arc<dyn PhysicalExpr>, FieldRef)> {
	let node = lower_node(ctx, operator, expression)?;
	let schema = positional_schema(operator, ctx.batch.schema_ref())?;
	let expr =
		create_physical_expr(&node.expr, &schema, &ExecutionProps::new(), &PhysicalPlanningContext::default())
			.map_err(|err| from_datafusion(operator, err))?;
	Ok((expr, node.field))
}

fn lower_node<'e>(ctx: &EvalContext, operator: &'static str, expression: &'e Expression) -> Lowered<'e, Node> {
	match expression {
		Expression::Column(column) => Ok(schema::column(ctx, operator, column)?),
		Expression::AccessSource(access) => Ok(schema::access(ctx, access)?),
		Expression::Constant(constant) => {
			Ok(literal::constant(operator, constant, display_label(expression).text())?)
		}
		Expression::Prefix(prefix) if matches!(prefix.operator, PrefixOperator::Not(_)) => {
			lower_not(ctx, operator, expression, prefix)
		}
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
		Expression::Call(call) if !ctx.is_aggregate_context => {
			routine::lower_call(ctx, operator, expression, call)
		}
		Expression::Variable(variable) => Ok(literal::variable(ctx, operator, variable)?),
		Expression::FieldAccess(access) => match access.object.as_ref() {
			Expression::Variable(variable) => {
				Ok(literal::variable_field(ctx, operator, variable, access.field.text())?)
			}
			_ => Err(Stop::Unsupported(expression)),
		},
		Expression::Alias(alias) => {
			let inner = lower_node(ctx, operator, &alias.expression)?;
			Ok(Node {
				expr: inner.expr,
				field: Arc::new(inner.field.as_ref().clone().with_name(alias.alias.name())),
			})
		}
		Expression::Cast(cast) => lower_cast(ctx, operator, expression, cast),
		_ => Err(Stop::Unsupported(expression)),
	}
}

fn lower_logical<'e>(
	ctx: &EvalContext,
	operator: &'static str,
	expression: &'e Expression,
	(left, right): (&'e Expression, &'e Expression),
	fragment: &Fragment,
	(logical_op, df_op): (LogicalOp, Operator),
) -> Lowered<'e, Node> {
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
) -> Lowered<'e, Node> {
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
