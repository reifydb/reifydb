// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::FlowNodeId;
use reifydb_rql::expression::Expression;

use crate::operator::{BoxedOperator, Operator};

pub mod registry;

pub trait TransformOperator: Operator {}

pub trait TransformOperatorFactory: Send + Sync {
	fn create_from_expressions(
		node: FlowNodeId,
		expressions: &[Expression<'static>],
	) -> crate::Result<BoxedOperator>;
}

pub mod extract {
	use reifydb_rql::expression::ConstantExpression;

	use super::*;

	pub fn int(expr: &Expression) -> crate::Result<i64> {
		match expr {
			Expression::Constant(ConstantExpression::Number {
				fragment,
			}) => {
				// Parse the number from the fragment text
				let text = fragment.text();
				text.parse::<i64>().map_err(|_| panic!("Failed to parse integer from: {}", text))
			}
			_ => panic!("Expected integer value"),
		}
	}

	/// Extract float from expression
	pub fn float(expr: &Expression) -> crate::Result<f64> {
		match expr {
			Expression::Constant(ConstantExpression::Number {
				fragment,
			}) => {
				// Parse the number from the fragment text
				let text = fragment.text();
				text.parse::<f64>().map_err(|_| panic!("Failed to parse float from: {}", text))
			}
			_ => panic!("Expected numeric value"),
		}
	}

	/// Extract string from expression
	pub fn string(expr: &Expression) -> crate::Result<String> {
		match expr {
			Expression::Constant(ConstantExpression::Text {
				fragment,
			}) => Ok(fragment.text().to_string()),
			Expression::Column(col) => {
				// Convert Fragment to string
				match &col.0.name {
					reifydb_type::Fragment::Owned(owned) => Ok(owned.to_string()),
					reifydb_type::Fragment::Borrowed(borrowed) => Ok(borrowed.text().to_string()),
					_ => unimplemented!(),
				}
			}
			_ => panic!("Expected string value"),
		}
	}
}
