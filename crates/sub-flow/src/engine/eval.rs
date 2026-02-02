// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::{collections::HashMap, sync::LazyLock};

use reifydb_core::value::column::columns::Columns;
use reifydb_engine::{
	evaluate::{ColumnEvaluationContext, column::StandardColumnEvaluator},
	vm::stack::SymbolTable,
};
use reifydb_rql::expression::Expression;
use reifydb_type::{Result, params::Params, value::Value};

static EMPTY_PARAMS: Params = Params::None;
static EMPTY_SYMBOL_TABLE: LazyLock<SymbolTable> = LazyLock::new(|| SymbolTable::new());

/// Evaluate a list of expressions into operator configuration
///
/// Only processes `Expression::Alias` variants:
/// - The alias name becomes the HashMap key
/// - The inner expression is evaluated to become the value
/// - Non-Alias expressions are skipped
///
/// # Arguments
/// * `expressions` - The expressions to evaluate (typically from FlowNode::Apply)
/// * `evaluator` - The StandardColumnEvaluator to use for expression evaluation
///
/// # Returns
/// HashMap<String, Value> where keys are alias names and values are evaluated results
///
/// # Errors
/// Returns error if expression evaluation fails
pub fn evaluate_operator_config(
	expressions: &[Expression],
	evaluator: &StandardColumnEvaluator,
) -> Result<HashMap<String, Value>> {
	let mut result = HashMap::new();

	// Create an empty Columns for evaluating constant expressions
	let empty_columns = Columns::empty();

	let eval_ctx = ColumnEvaluationContext {
		target: None,
		columns: empty_columns,
		row_count: 1, // Need at least 1 row to evaluate constants
		take: None,
		params: &EMPTY_PARAMS,
		symbol_table: &EMPTY_SYMBOL_TABLE,
		is_aggregate_context: false,
	};

	// Process each expression
	for expr in expressions {
		match expr {
			Expression::Alias(alias_expr) => {
				let key = alias_expr.alias.name().to_string();
				let column = evaluator.evaluate(&eval_ctx, &alias_expr.expression)?;
				// Extract the single value from the column (constant expressions produce 1 value)
				let value = if column.data().len() > 0 {
					column.data().get_value(0)
				} else {
					Value::Undefined
				};
				result.insert(key, value);
			}
			_ => {
				// Skip non-Alias expressions
			}
		}
	}

	Ok(result)
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::evaluate::column::StandardColumnEvaluator;
	use reifydb_function::registry::Functions;
	use reifydb_rql::expression::{AliasExpression, ConstantExpression, Expression, IdentExpression};
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
		Expression::Constant(ConstantExpression::Undefined {
			fragment: Fragment::internal("undefined".to_string()),
		})
	}

	#[test]
	fn test_empty_expressions() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions: Vec<Expression> = vec![];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert!(result.is_empty());
	}

	#[test]
	fn test_single_alias_string() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![create_alias_expression("key1", create_constant_text("value1"))];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("key1"), Some(&Value::Utf8("value1".into())));
	}

	#[test]
	fn test_single_alias_number() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![create_alias_expression("count", create_constant_number(42))];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("count"), Some(&Value::Int1(42)));
	}

	#[test]
	fn test_single_alias_bool() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![create_alias_expression("enabled", create_constant_bool(true))];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("enabled"), Some(&Value::Boolean(true)));
	}

	#[test]
	fn test_single_alias_undefined() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![create_alias_expression("optional", create_constant_undefined())];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("optional"), Some(&Value::Undefined));
	}

	#[test]
	fn test_multiple_aliases() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![
			create_alias_expression("key1", create_constant_text("value1")),
			create_alias_expression("key2", create_constant_number(100)),
			create_alias_expression("key3", create_constant_bool(false)),
		];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 3);
		assert_eq!(result.get("key1"), Some(&Value::Utf8("value1".into())));
		assert_eq!(result.get("key2"), Some(&Value::Int1(100)));
		assert_eq!(result.get("key3"), Some(&Value::Boolean(false)));
	}

	// Expression filtering tests

	#[test]
	fn test_non_alias_expressions_skipped() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![
			create_alias_expression("valid", create_constant_text("included")),
			create_constant_text("standalone"), // Non-alias, should be skipped
			create_constant_number(999),        // Non-alias, should be skipped
		];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("valid"), Some(&Value::Utf8("included".into())));
	}

	#[test]
	fn test_only_non_alias_expressions() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions =
			vec![create_constant_text("text"), create_constant_number(42), create_constant_bool(true)];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert!(result.is_empty());
	}

	// Value type coverage tests

	#[test]
	fn test_all_basic_value_types() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![
			create_alias_expression("text_val", create_constant_text("hello")),
			create_alias_expression("num_val", create_constant_number(-42)),
			create_alias_expression("bool_true", create_constant_bool(true)),
			create_alias_expression("bool_false", create_constant_bool(false)),
			create_alias_expression("undef_val", create_constant_undefined()),
		];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 5);
		assert_eq!(result.get("text_val"), Some(&Value::Utf8("hello".into())));
		assert_eq!(result.get("num_val"), Some(&Value::Int1(-42)));
		assert_eq!(result.get("bool_true"), Some(&Value::Boolean(true)));
		assert_eq!(result.get("bool_false"), Some(&Value::Boolean(false)));
		assert_eq!(result.get("undef_val"), Some(&Value::Undefined));
	}

	#[test]
	fn test_duplicate_alias_names_last_wins() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![
			create_alias_expression("key", create_constant_text("first")),
			create_alias_expression("key", create_constant_text("second")),
			create_alias_expression("key", create_constant_number(42)),
		];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("key"), Some(&Value::Int1(42)));
	}

	#[test]
	fn test_empty_string_value() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![create_alias_expression("empty", create_constant_text(""))];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("empty"), Some(&Value::Utf8("".into())));
	}

	#[test]
	fn test_special_characters_in_alias_name() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![
			create_alias_expression("key_with_underscore", create_constant_number(1)),
			create_alias_expression("keyWithCamelCase", create_constant_number(2)),
			create_alias_expression("key123", create_constant_number(3)),
		];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 3);
		assert_eq!(result.get("key_with_underscore"), Some(&Value::Int1(1)));
		assert_eq!(result.get("keyWithCamelCase"), Some(&Value::Int1(2)));
		assert_eq!(result.get("key123"), Some(&Value::Int1(3)));
	}

	#[test]
	fn test_large_number_values() {
		let evaluator = StandardColumnEvaluator::new(Functions::builder().build());
		let expressions = vec![
			create_alias_expression("small", create_constant_number(0)),
			create_alias_expression("large_positive", create_constant_number(i64::MAX)),
			create_alias_expression("large_negative", create_constant_number(i64::MIN)),
		];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 3);
		assert_eq!(result.get("small"), Some(&Value::Int1(0)));
		assert_eq!(result.get("large_positive"), Some(&Value::Int8(i64::MAX)));
		assert_eq!(result.get("large_negative"), Some(&Value::Int8(i64::MIN)));
	}
}
