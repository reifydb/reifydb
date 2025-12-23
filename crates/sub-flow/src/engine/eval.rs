// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::HashMap;

use reifydb_core::{Row, interface::Params, value::encoded::EncodedValuesNamedLayout};
use reifydb_engine::{RowEvaluationContext, StandardRowEvaluator};
use reifydb_rql::expression::Expression;
use reifydb_type::{RowNumber, Value};

use crate::Result;

/// Evaluate a list of expressions into operator configuration
///
/// Only processes `Expression::Alias` variants:
/// - The alias name becomes the HashMap key
/// - The inner expression is evaluated to become the value
/// - Non-Alias expressions are skipped
///
/// # Arguments
/// * `expressions` - The expressions to evaluate (typically from FlowNode::Apply)
/// * `evaluator` - The StandardRowEvaluator to use for expression evaluation
///
/// # Returns
/// HashMap<String, Value> where keys are alias names and values are evaluated results
///
/// # Errors
/// Returns error if expression evaluation fails
pub fn evaluate_operator_config(
	expressions: &[Expression],
	evaluator: &StandardRowEvaluator,
) -> Result<HashMap<String, Value>> {
	let mut result = HashMap::new();

	// Create a dummy layout with a single field since EncodedValuesLayout requires at least one type
	// This is needed for constant expression evaluation even though the row data isn't accessed
	let dummy_layout = EncodedValuesNamedLayout::new(vec![("_dummy".to_string(), reifydb_type::Type::Boolean)]);
	let dummy_row = Row {
		number: RowNumber(0),
		layout: dummy_layout.clone(),
		encoded: dummy_layout.layout().allocate(),
	};

	let empty_params = Params::empty();

	let eval_ctx = RowEvaluationContext {
		row: dummy_row,
		target: None,
		params: &empty_params,
	};

	// Process each expression
	for expr in expressions {
		match expr {
			Expression::Alias(alias_expr) => {
				let key = alias_expr.alias.name().to_string();
				let value = evaluator.evaluate(&eval_ctx, &alias_expr.expression)?;
				result.insert(key, value);
			}
			_ => {
				// Skip non-Alias expressions
				// Could add logging here if needed
			}
		}
	}

	Ok(result)
}

#[cfg(test)]
mod tests {
	use reifydb_engine::StandardRowEvaluator;
	use reifydb_rql::expression::{AliasExpression, ConstantExpression, Expression, IdentExpression};
	use reifydb_type::{Fragment, Value};

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

	#[tokio::test]
	async fn test_empty_expressions() {
		let evaluator = StandardRowEvaluator::new();
		let expressions: Vec<Expression> = vec![];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert!(result.is_empty());
	}

	#[tokio::test]
	async fn test_single_alias_string() {
		let evaluator = StandardRowEvaluator::new();
		let expressions = vec![create_alias_expression("key1", create_constant_text("value1"))];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("key1"), Some(&Value::Utf8("value1".into())));
	}

	#[tokio::test]
	async fn test_single_alias_number() {
		let evaluator = StandardRowEvaluator::new();
		let expressions = vec![create_alias_expression("count", create_constant_number(42))];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("count"), Some(&Value::Int1(42)));
	}

	#[tokio::test]
	async fn test_single_alias_bool() {
		let evaluator = StandardRowEvaluator::new();
		let expressions = vec![create_alias_expression("enabled", create_constant_bool(true))];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("enabled"), Some(&Value::Boolean(true)));
	}

	#[tokio::test]
	async fn test_single_alias_undefined() {
		let evaluator = StandardRowEvaluator::new();
		let expressions = vec![create_alias_expression("optional", create_constant_undefined())];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("optional"), Some(&Value::Undefined));
	}

	#[tokio::test]
	async fn test_multiple_aliases() {
		let evaluator = StandardRowEvaluator::new();
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

	#[tokio::test]
	async fn test_non_alias_expressions_skipped() {
		let evaluator = StandardRowEvaluator::new();
		let expressions = vec![
			create_alias_expression("valid", create_constant_text("included")),
			create_constant_text("standalone"), // Non-alias, should be skipped
			create_constant_number(999),        // Non-alias, should be skipped
		];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("valid"), Some(&Value::Utf8("included".into())));
	}

	#[tokio::test]
	async fn test_only_non_alias_expressions() {
		let evaluator = StandardRowEvaluator::new();
		let expressions =
			vec![create_constant_text("text"), create_constant_number(42), create_constant_bool(true)];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert!(result.is_empty());
	}

	// Value type coverage tests

	#[tokio::test]
	async fn test_all_basic_value_types() {
		let evaluator = StandardRowEvaluator::new();
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

	#[tokio::test]
	async fn test_duplicate_alias_names_last_wins() {
		let evaluator = StandardRowEvaluator::new();
		let expressions = vec![
			create_alias_expression("key", create_constant_text("first")),
			create_alias_expression("key", create_constant_text("second")),
			create_alias_expression("key", create_constant_number(42)),
		];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("key"), Some(&Value::Int1(42)));
	}

	#[tokio::test]
	async fn test_empty_string_value() {
		let evaluator = StandardRowEvaluator::new();
		let expressions = vec![create_alias_expression("empty", create_constant_text(""))];

		let result = evaluate_operator_config(&expressions, &evaluator).unwrap();

		assert_eq!(result.len(), 1);
		assert_eq!(result.get("empty"), Some(&Value::Utf8("".into())));
	}

	#[tokio::test]
	async fn test_special_characters_in_alias_name() {
		let evaluator = StandardRowEvaluator::new();
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

	#[tokio::test]
	async fn test_large_number_values() {
		let evaluator = StandardRowEvaluator::new();
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
