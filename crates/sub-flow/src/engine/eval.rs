// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{collections::BTreeMap, sync::LazyLock};

use reifydb_core::value::column::columns::Columns;
use reifydb_engine::{
	expression::{
		compile::compile_expression,
		context::{CompileContext, EvalContext},
	},
	vm::stack::SymbolTable,
};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::Expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_type::{
	Result,
	params::Params,
	value::{Value, identity::IdentityId},
};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(SymbolTable::new);

/// Evaluate a list of expressions into operator configuration
///
/// Only processes `Expression::Alias` variants:
/// - The alias name becomes the BTreeMap key
/// - The inner expression is evaluated to become the value
/// - Non-Alias expressions are skipped
pub fn evaluate_operator_config(
	expressions: &[Expression],
	routines: &Routines,
	runtime_context: &RuntimeContext,
) -> Result<BTreeMap<String, Value>> {
	let mut result = BTreeMap::new();

	let compile_ctx = CompileContext {
		symbols: &EMPTY_SYMBOL_TABLE,
	};

	let empty_columns = Columns::empty();

	let session = EvalContext {
		params: &EMPTY_PARAMS,
		symbols: &EMPTY_SYMBOL_TABLE,
		routines,
		runtime_context,
		arena: None,
		identity: IdentityId::root(),
		is_aggregate_context: false,
		columns: Columns::empty(),
		row_count: 1,
		target: None,
		take: None,
	};
	let exec_ctx = session.with_eval(empty_columns, 1);

	for expr in expressions {
		if let Expression::Alias(alias_expr) = expr {
			let key = alias_expr.alias.name().to_string();

			let expr = compile_expression(&compile_ctx, &alias_expr.expression)?;
			let column = expr.execute(&exec_ctx)?;

			let value = if !column.data().is_empty() {
				column.data().get_value(0)
			} else {
				Value::none()
			};
			result.insert(key, value);
		}
	}

	Ok(result)
}

#[cfg(test)]
pub mod tests {
	use reifydb_routine::routine::registry::Routines;
	use reifydb_rql::expression::{AliasExpression, ConstantExpression, Expression, IdentExpression};
	use reifydb_runtime::context::{RuntimeContext, clock::Clock};
	use reifydb_type::{fragment::Fragment, value::Value};

	use super::evaluate_operator_config;

	fn create_alias_expression(alias_name: &str, inner_expression: Expression) -> Expression {
		Expression::Alias(AliasExpression {
			alias: IdentExpression(Fragment::internal(alias_name.to_string())),
			expression: Box::new(inner_expression),
			fragment: Fragment::testing_empty(),
		})
	}

	fn create_constant_text(text: &str) -> Expression {
		Expression::Constant(ConstantExpression::Text {
			fragment: Fragment::internal(text.to_string()),
		})
	}

	fn create_constant_number(num: i64) -> Expression {
		Expression::Constant(ConstantExpression::Number {
			fragment: Fragment::internal(num.to_string()),
		})
	}

	fn create_constant_bool(value: bool) -> Expression {
		Expression::Constant(ConstantExpression::Bool {
			fragment: Fragment::internal(value.to_string()),
		})
	}

	fn create_constant_undefined() -> Expression {
		Expression::Constant(ConstantExpression::None {
			fragment: Fragment::internal("none".to_string()),
		})
	}

	#[test]
	fn test_empty_expressions() {
		let routines = Routines::empty();
		let runtime_context = RuntimeContext::with_clock(Clock::Real);
		let expressions: Vec<Expression> = vec![];

		let result = evaluate_operator_config(&expressions, &routines, &runtime_context).unwrap();

		assert!(result.is_empty());
	}

	#[test]
	fn test_single_alias_string() {
		let routines = Routines::empty();
		let runtime_context = RuntimeContext::with_clock(Clock::Real);
		let expressions = vec![create_alias_expression("key1", create_constant_text("value1"))];

		let result = evaluate_operator_config(&expressions, &routines, &runtime_context).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("key1"), Some(&Value::Utf8("value1".into())));
	}

	#[test]
	fn test_single_alias_number() {
		let routines = Routines::empty();
		let runtime_context = RuntimeContext::with_clock(Clock::Real);
		let expressions = vec![create_alias_expression("count", create_constant_number(42))];

		let result = evaluate_operator_config(&expressions, &routines, &runtime_context).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("count"), Some(&Value::Int1(42)));
	}

	#[test]
	fn test_single_alias_bool() {
		let routines = Routines::empty();
		let runtime_context = RuntimeContext::with_clock(Clock::Real);
		let expressions = vec![create_alias_expression("enabled", create_constant_bool(true))];

		let result = evaluate_operator_config(&expressions, &routines, &runtime_context).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("enabled"), Some(&Value::Boolean(true)));
	}

	#[test]
	fn test_single_alias_undefined() {
		let routines = Routines::empty();
		let runtime_context = RuntimeContext::with_clock(Clock::Real);
		let expressions = vec![create_alias_expression("optional", create_constant_undefined())];

		let result = evaluate_operator_config(&expressions, &routines, &runtime_context).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("optional"), Some(&Value::none()));
	}

	#[test]
	fn test_multiple_aliases() {
		let routines = Routines::empty();
		let runtime_context = RuntimeContext::with_clock(Clock::Real);
		let expressions = vec![
			create_alias_expression("key1", create_constant_text("value1")),
			create_alias_expression("key2", create_constant_number(100)),
			create_alias_expression("key3", create_constant_bool(false)),
		];

		let result = evaluate_operator_config(&expressions, &routines, &runtime_context).unwrap();

		assert_eq!(result.len(), 3);
		assert_eq!(result.get("key1"), Some(&Value::Utf8("value1".into())));
		assert_eq!(result.get("key2"), Some(&Value::Int1(100)));
		assert_eq!(result.get("key3"), Some(&Value::Boolean(false)));
	}

	#[test]
	fn test_non_alias_expressions_skipped() {
		let routines = Routines::empty();
		let runtime_context = RuntimeContext::with_clock(Clock::Real);
		let expressions = vec![
			create_alias_expression("valid", create_constant_text("included")),
			create_constant_text("standalone"),
			create_constant_number(999),
		];

		let result = evaluate_operator_config(&expressions, &routines, &runtime_context).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("valid"), Some(&Value::Utf8("included".into())));
	}

	#[test]
	fn test_only_non_alias_expressions() {
		let routines = Routines::empty();
		let runtime_context = RuntimeContext::with_clock(Clock::Real);
		let expressions = vec![
			create_constant_text("standalone"),
			create_constant_number(999),
			create_constant_bool(true),
		];

		let result = evaluate_operator_config(&expressions, &routines, &runtime_context).unwrap();

		assert!(result.is_empty());
	}

	#[test]
	fn test_unknown_function_returns_error() {
		let routines = Routines::empty();
		let runtime_context = RuntimeContext::with_clock(Clock::Real);
		let expressions = vec![create_alias_expression("custom_function", create_constant_text("data"))];

		let result = evaluate_operator_config(&expressions, &routines, &runtime_context);

		assert!(result.is_ok());
	}
}
