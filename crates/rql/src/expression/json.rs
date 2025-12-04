// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! JSON serialization for Expression types.
//!
//! This module provides clean JSON serialization that:
//! - Skips Fragment source location metadata (line/column positions)
//! - Preserves semantic data only (values, types, names)
//! - Supports round-trip serialization/deserialization
//! - Is suitable for frontend query builders

use reifydb_core::interface::{ColumnIdentifier, ColumnSource};
use reifydb_type::{Fragment, OwnedFragment};
use serde::{Deserialize, Serialize};
use serde_json::from_str;

use super::{
	AccessSourceExpression, AddExpression, AliasExpression, AndExpression, BetweenExpression, CallExpression,
	CastExpression, ColumnExpression, ConstantExpression, DivExpression, ElseIfExpression, EqExpression,
	Expression, ExtendExpression, GreaterThanEqExpression, GreaterThanExpression, IdentExpression, IfExpression,
	InExpression, LessThanEqExpression, LessThanExpression, MapExpression, MulExpression, NotEqExpression,
	OrExpression, ParameterExpression, PrefixExpression, PrefixOperator, RemExpression, SubExpression,
	TupleExpression, TypeExpression, VariableExpression, XorExpression,
};

/// JSON-serializable expression for query builders.
///
/// This enum mirrors the `Expression` type but uses simple owned types
/// instead of lifetimed `Fragment`s, making it suitable for JSON serialization.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JsonExpression {
	// Constants
	Undefined,
	Bool {
		value: String,
	},
	Number {
		value: String,
	},
	Text {
		value: String,
	},
	Temporal {
		value: String,
	},

	// Identifiers
	Column {
		namespace: String,
		source: String,
		name: String,
	},
	AccessSource {
		namespace: String,
		source: String,
		name: String,
	},
	Variable {
		name: String,
	},
	#[serde(rename = "parameter_positional")]
	ParameterPositional {
		position: String,
	},
	#[serde(rename = "parameter_named")]
	ParameterNamed {
		name: String,
	},

	// Comparison
	GreaterThan {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	GreaterThanEqual {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	LessThan {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	LessThanEqual {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	Equal {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	NotEqual {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},

	// Logical
	And {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	Or {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	Xor {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},

	// Arithmetic
	Add {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	Sub {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	Mul {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	Div {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},
	Rem {
		left: Box<JsonExpression>,
		right: Box<JsonExpression>,
	},

	// Complex
	Alias {
		alias: String,
		expression: Box<JsonExpression>,
	},
	Cast {
		expression: Box<JsonExpression>,
		to: String,
	},
	Call {
		function: String,
		args: Vec<JsonExpression>,
	},
	Tuple {
		expressions: Vec<JsonExpression>,
	},
	Prefix {
		operator: String,
		expression: Box<JsonExpression>,
	},
	Between {
		value: Box<JsonExpression>,
		lower: Box<JsonExpression>,
		upper: Box<JsonExpression>,
	},
	In {
		value: Box<JsonExpression>,
		list: Box<JsonExpression>,
		negated: bool,
	},
	If {
		condition: Box<JsonExpression>,
		then: Box<JsonExpression>,
		else_ifs: Vec<JsonElseIf>,
		#[serde(skip_serializing_if = "Option::is_none")]
		else_expr: Option<Box<JsonExpression>>,
	},
	Map {
		expressions: Vec<JsonExpression>,
	},
	Extend {
		expressions: Vec<JsonExpression>,
	},
	Type {
		ty: String,
	},
}

/// JSON representation of an else-if branch.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JsonElseIf {
	pub condition: Box<JsonExpression>,
	pub then: Box<JsonExpression>,
}

// Helper to extract namespace and source from ColumnSource
fn extract_source(source: &ColumnSource) -> (String, String) {
	match source {
		ColumnSource::Source {
			namespace,
			source,
		} => (namespace.text().to_string(), source.text().to_string()),
		ColumnSource::Alias(alias) => ("_alias".to_string(), alias.text().to_string()),
	}
}

// Helper to create an internal fragment
fn internal_fragment(text: &str) -> Fragment<'static> {
	Fragment::Owned(OwnedFragment::Internal {
		text: text.to_string(),
	})
}

// ============================================================================
// Expression -> JsonExpression conversion
// ============================================================================

impl From<&Expression<'_>> for JsonExpression {
	fn from(expr: &Expression) -> Self {
		match expr {
			// Constants
			Expression::Constant(constant) => match constant {
				ConstantExpression::Undefined {
					..
				} => JsonExpression::Undefined,
				ConstantExpression::Bool {
					fragment,
				} => JsonExpression::Bool {
					value: fragment.text().to_string(),
				},
				ConstantExpression::Number {
					fragment,
				} => JsonExpression::Number {
					value: fragment.text().to_string(),
				},
				ConstantExpression::Text {
					fragment,
				} => JsonExpression::Text {
					value: fragment.text().to_string(),
				},
				ConstantExpression::Temporal {
					fragment,
				} => JsonExpression::Temporal {
					value: fragment.text().to_string(),
				},
			},

			// Identifiers
			Expression::Column(ColumnExpression(col)) => {
				let (namespace, source) = extract_source(&col.source);
				JsonExpression::Column {
					namespace,
					source,
					name: col.name.text().to_string(),
				}
			}
			Expression::AccessSource(AccessSourceExpression {
				column,
			}) => {
				let (namespace, source) = extract_source(&column.source);
				JsonExpression::AccessSource {
					namespace,
					source,
					name: column.name.text().to_string(),
				}
			}
			Expression::Variable(var) => JsonExpression::Variable {
				name: var.name().to_string(),
			},
			Expression::Parameter(param) => match param {
				ParameterExpression::Positional {
					fragment,
				} => JsonExpression::ParameterPositional {
					position: fragment.text()[1..].to_string(), // Skip '$'
				},
				ParameterExpression::Named {
					fragment,
				} => JsonExpression::ParameterNamed {
					name: fragment.text()[1..].to_string(), // Skip '$'
				},
			},

			// Comparison
			Expression::GreaterThan(e) => JsonExpression::GreaterThan {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::GreaterThanEqual(e) => JsonExpression::GreaterThanEqual {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::LessThan(e) => JsonExpression::LessThan {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::LessThanEqual(e) => JsonExpression::LessThanEqual {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::Equal(e) => JsonExpression::Equal {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::NotEqual(e) => JsonExpression::NotEqual {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},

			// Logical
			Expression::And(e) => JsonExpression::And {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::Or(e) => JsonExpression::Or {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::Xor(e) => JsonExpression::Xor {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},

			// Arithmetic
			Expression::Add(e) => JsonExpression::Add {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::Sub(e) => JsonExpression::Sub {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::Mul(e) => JsonExpression::Mul {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::Div(e) => JsonExpression::Div {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},
			Expression::Rem(e) => JsonExpression::Rem {
				left: Box::new((&*e.left).into()),
				right: Box::new((&*e.right).into()),
			},

			// Complex
			Expression::Alias(e) => JsonExpression::Alias {
				alias: e.alias.name().to_string(),
				expression: Box::new((&*e.expression).into()),
			},
			Expression::Cast(e) => JsonExpression::Cast {
				expression: Box::new((&*e.expression).into()),
				to: format!("{:?}", e.to.ty),
			},
			Expression::Call(e) => JsonExpression::Call {
				function: e.func.name().to_string(),
				args: e.args.iter().map(|a| a.into()).collect(),
			},
			Expression::Tuple(e) => JsonExpression::Tuple {
				expressions: e.expressions.iter().map(|a| a.into()).collect(),
			},
			Expression::Prefix(e) => {
				let operator = match &e.operator {
					PrefixOperator::Minus(_) => "-",
					PrefixOperator::Plus(_) => "+",
					PrefixOperator::Not(_) => "not",
				};
				JsonExpression::Prefix {
					operator: operator.to_string(),
					expression: Box::new((&*e.expression).into()),
				}
			}
			Expression::Between(e) => JsonExpression::Between {
				value: Box::new((&*e.value).into()),
				lower: Box::new((&*e.lower).into()),
				upper: Box::new((&*e.upper).into()),
			},
			Expression::In(e) => JsonExpression::In {
				value: Box::new((&*e.value).into()),
				list: Box::new((&*e.list).into()),
				negated: e.negated,
			},
			Expression::If(e) => JsonExpression::If {
				condition: Box::new((&*e.condition).into()),
				then: Box::new((&*e.then_expr).into()),
				else_ifs: e
					.else_ifs
					.iter()
					.map(|ei| JsonElseIf {
						condition: Box::new((&*ei.condition).into()),
						then: Box::new((&*ei.then_expr).into()),
					})
					.collect(),
				else_expr: e.else_expr.as_ref().map(|ee| Box::new((&**ee).into())),
			},
			Expression::Map(e) => JsonExpression::Map {
				expressions: e.expressions.iter().map(|a| a.into()).collect(),
			},
			Expression::Extend(e) => JsonExpression::Extend {
				expressions: e.expressions.iter().map(|a| a.into()).collect(),
			},
			Expression::Type(e) => JsonExpression::Type {
				ty: format!("{:?}", e.ty),
			},
		}
	}
}

// ============================================================================
// JsonExpression -> Expression conversion
// ============================================================================

impl TryFrom<JsonExpression> for Expression<'static> {
	type Error = reifydb_type::Error;

	fn try_from(json: JsonExpression) -> Result<Self, Self::Error> {
		Ok(match json {
			// Constants
			JsonExpression::Undefined => Expression::Constant(ConstantExpression::Undefined {
				fragment: Fragment::None,
			}),
			JsonExpression::Bool {
				value,
			} => Expression::Constant(ConstantExpression::Bool {
				fragment: internal_fragment(&value),
			}),
			JsonExpression::Number {
				value,
			} => Expression::Constant(ConstantExpression::Number {
				fragment: internal_fragment(&value),
			}),
			JsonExpression::Text {
				value,
			} => Expression::Constant(ConstantExpression::Text {
				fragment: internal_fragment(&value),
			}),
			JsonExpression::Temporal {
				value,
			} => Expression::Constant(ConstantExpression::Temporal {
				fragment: internal_fragment(&value),
			}),

			// Identifiers
			JsonExpression::Column {
				namespace,
				source,
				name,
			} => Expression::Column(ColumnExpression(ColumnIdentifier {
				source: ColumnSource::Source {
					namespace: internal_fragment(&namespace),
					source: internal_fragment(&source),
				},
				name: internal_fragment(&name),
			})),
			JsonExpression::AccessSource {
				namespace,
				source,
				name,
			} => Expression::AccessSource(AccessSourceExpression {
				column: ColumnIdentifier {
					source: ColumnSource::Source {
						namespace: internal_fragment(&namespace),
						source: internal_fragment(&source),
					},
					name: internal_fragment(&name),
				},
			}),
			JsonExpression::Variable {
				name,
			} => Expression::Variable(VariableExpression {
				fragment: internal_fragment(&format!("${}", name)),
			}),
			JsonExpression::ParameterPositional {
				position,
			} => Expression::Parameter(ParameterExpression::Positional {
				fragment: internal_fragment(&format!("${}", position)),
			}),
			JsonExpression::ParameterNamed {
				name,
			} => Expression::Parameter(ParameterExpression::Named {
				fragment: internal_fragment(&format!("${}", name)),
			}),

			// Comparison
			JsonExpression::GreaterThan {
				left,
				right,
			} => Expression::GreaterThan(GreaterThanExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::GreaterThanEqual {
				left,
				right,
			} => Expression::GreaterThanEqual(GreaterThanEqExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::LessThan {
				left,
				right,
			} => Expression::LessThan(LessThanExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::LessThanEqual {
				left,
				right,
			} => Expression::LessThanEqual(LessThanEqExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::Equal {
				left,
				right,
			} => Expression::Equal(EqExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::NotEqual {
				left,
				right,
			} => Expression::NotEqual(NotEqExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),

			// Logical
			JsonExpression::And {
				left,
				right,
			} => Expression::And(AndExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::Or {
				left,
				right,
			} => Expression::Or(OrExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::Xor {
				left,
				right,
			} => Expression::Xor(XorExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),

			// Arithmetic
			JsonExpression::Add {
				left,
				right,
			} => Expression::Add(AddExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::Sub {
				left,
				right,
			} => Expression::Sub(SubExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::Mul {
				left,
				right,
			} => Expression::Mul(MulExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::Div {
				left,
				right,
			} => Expression::Div(DivExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::Rem {
				left,
				right,
			} => Expression::Rem(RemExpression {
				left: Box::new((*left).try_into()?),
				right: Box::new((*right).try_into()?),
				fragment: Fragment::None,
			}),

			// Complex
			JsonExpression::Alias {
				alias,
				expression,
			} => Expression::Alias(AliasExpression {
				alias: IdentExpression(internal_fragment(&alias)),
				expression: Box::new((*expression).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::Cast {
				expression,
				to,
			} => {
				let ty = parse_type(&to)?;
				Expression::Cast(CastExpression {
					expression: Box::new((*expression).try_into()?),
					to: TypeExpression {
						ty,
						fragment: internal_fragment(&to),
					},
					fragment: Fragment::None,
				})
			}
			JsonExpression::Call {
				function,
				args,
			} => Expression::Call(CallExpression {
				func: IdentExpression(internal_fragment(&function)),
				args: args.into_iter().map(|a| a.try_into()).collect::<Result<Vec<_>, _>>()?,
				fragment: Fragment::None,
			}),
			JsonExpression::Tuple {
				expressions,
			} => Expression::Tuple(TupleExpression {
				expressions: expressions
					.into_iter()
					.map(|a| a.try_into())
					.collect::<Result<Vec<_>, _>>()?,
				fragment: Fragment::None,
			}),
			JsonExpression::Prefix {
				operator,
				expression,
			} => {
				let op = match operator.as_str() {
					"-" => PrefixOperator::Minus(Fragment::None),
					"+" => PrefixOperator::Plus(Fragment::None),
					"not" => PrefixOperator::Not(Fragment::None),
					_ => {
						return Err(reifydb_type::Error(reifydb_type::internal!(
							"Unknown prefix operator: {}",
							operator
						)));
					}
				};
				Expression::Prefix(PrefixExpression {
					operator: op,
					expression: Box::new((*expression).try_into()?),
					fragment: Fragment::None,
				})
			}
			JsonExpression::Between {
				value,
				lower,
				upper,
			} => Expression::Between(BetweenExpression {
				value: Box::new((*value).try_into()?),
				lower: Box::new((*lower).try_into()?),
				upper: Box::new((*upper).try_into()?),
				fragment: Fragment::None,
			}),
			JsonExpression::In {
				value,
				list,
				negated,
			} => Expression::In(InExpression {
				value: Box::new((*value).try_into()?),
				list: Box::new((*list).try_into()?),
				negated,
				fragment: Fragment::None,
			}),
			JsonExpression::If {
				condition,
				then,
				else_ifs,
				else_expr,
			} => {
				let converted_else: Option<Box<Expression<'static>>> = match else_expr {
					Some(ee) => Some(Box::new((*ee).try_into()?)),
					None => None,
				};
				Expression::If(IfExpression {
					condition: Box::new((*condition).try_into()?),
					then_expr: Box::new((*then).try_into()?),
					else_ifs: else_ifs
						.into_iter()
						.map(|ei| {
							Ok(ElseIfExpression {
								condition: Box::new((*ei.condition).try_into()?),
								then_expr: Box::new((*ei.then).try_into()?),
								fragment: Fragment::None,
							})
						})
						.collect::<Result<Vec<_>, Self::Error>>()?,
					else_expr: converted_else,
					fragment: Fragment::None,
				})
			}
			JsonExpression::Map {
				expressions,
			} => Expression::Map(MapExpression {
				expressions: expressions
					.into_iter()
					.map(|a| a.try_into())
					.collect::<Result<Vec<_>, _>>()?,
				fragment: Fragment::None,
			}),
			JsonExpression::Extend {
				expressions,
			} => Expression::Extend(ExtendExpression {
				expressions: expressions
					.into_iter()
					.map(|a| a.try_into())
					.collect::<Result<Vec<_>, _>>()?,
				fragment: Fragment::None,
			}),
			JsonExpression::Type {
				ty,
			} => {
				let parsed_ty = parse_type(&ty)?;
				Expression::Type(TypeExpression {
					ty: parsed_ty,
					fragment: internal_fragment(&ty),
				})
			}
		})
	}
}

// Helper to parse type strings back to Type enum
fn parse_type(s: &str) -> reifydb_core::Result<reifydb_type::Type> {
	use reifydb_type::Type;

	// Handle type debug representations
	let ty = match s.to_lowercase().as_str() {
		"boolean" => Type::Boolean,
		"bool" => Type::Boolean,
		"int1" => Type::Int1,
		"int2" => Type::Int2,
		"int4" => Type::Int4,
		"int8" => Type::Int8,
		"int16" => Type::Int16,
		"int32" => Type::Int4,
		"int64" => Type::Int8,
		"uint1" => Type::Uint1,
		"uint2" => Type::Uint2,
		"uint4" => Type::Uint4,
		"uint8" => Type::Uint8,
		"uint16" => Type::Uint16,
		"float4" => Type::Float4,
		"float8" => Type::Float8,
		"float32" => Type::Float4,
		"float64" => Type::Float8,
		"utf8" => Type::Utf8,
		"string" => Type::Utf8,
		"text" => Type::Utf8,
		"blob" => Type::Blob,
		"uuid4" => Type::Uuid4,
		"uuid7" => Type::Uuid7,
		"date" => Type::Date,
		"time" => Type::Time,
		"datetime" => Type::DateTime,
		"duration" => Type::Duration,
		"rownumber" => Type::RowNumber,
		"identityid" => Type::IdentityId,
		"int" => Type::Int,
		"uint" => Type::Uint,
		"decimal" => Type::Decimal,
		_ => {
			return Err(reifydb_type::Error(reifydb_type::internal!("Unknown type: {}", s)));
		}
	};

	Ok(ty)
}

// ============================================================================
// Public API
// ============================================================================

/// Serialize an Expression to a JSON string.
///
/// The output skips Fragment source location metadata and preserves only
/// semantic data suitable for frontend query builders.
pub fn to_json(expr: &Expression) -> String {
	let json_expr: JsonExpression = expr.into();
	serde_json::to_string(&json_expr).expect("JsonExpression should always serialize")
}

/// Serialize an Expression to a pretty-printed JSON string.
pub fn to_json_pretty(expr: &Expression) -> String {
	let json_expr: JsonExpression = expr.into();
	serde_json::to_string_pretty(&json_expr).expect("JsonExpression should always serialize")
}

/// Deserialize an Expression from a JSON string.
pub fn from_json(json: &str) -> reifydb_core::Result<Expression<'static>> {
	let json_expr: JsonExpression = from_str(json).map_err(|e| {
		reifydb_type::Error(reifydb_type::diagnostic::serde::serde_deserialize_error(e.to_string()))
	})?;
	json_expr.try_into()
}

#[cfg(test)]
mod tests {
	use super::*;

	// Helper functions to create test expressions
	fn column_expr(name: &str) -> Expression<'static> {
		Expression::Column(ColumnExpression(ColumnIdentifier {
			source: ColumnSource::Source {
				namespace: internal_fragment("_context"),
				source: internal_fragment("_context"),
			},
			name: internal_fragment(name),
		}))
	}

	fn constant_number(val: &str) -> Expression<'static> {
		Expression::Constant(ConstantExpression::Number {
			fragment: internal_fragment(val),
		})
	}

	fn constant_text(val: &str) -> Expression<'static> {
		Expression::Constant(ConstantExpression::Text {
			fragment: internal_fragment(val),
		})
	}

	fn constant_bool(val: &str) -> Expression<'static> {
		Expression::Constant(ConstantExpression::Bool {
			fragment: internal_fragment(val),
		})
	}

	// =========================================================================
	// Constant tests
	// =========================================================================

	#[test]
	fn test_undefined() {
		let expr = Expression::Constant(ConstantExpression::Undefined {
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"undefined"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_bool() {
		let expr = constant_bool("true");

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"bool","value":"true"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_number() {
		let expr = constant_number("42");

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"number","value":"42"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_text() {
		let expr = constant_text("hello world");

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"text","value":"hello world"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_temporal() {
		let expr = Expression::Constant(ConstantExpression::Temporal {
			fragment: internal_fragment("2024-01-15T10:30:00"),
		});

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"temporal","value":"2024-01-15T10:30:00"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	// =========================================================================
	// Identifier tests
	// =========================================================================

	#[test]
	fn test_column() {
		let expr = column_expr("age");

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"column","namespace":"_context","source":"_context","name":"age"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_variable() {
		let expr = Expression::Variable(VariableExpression {
			fragment: internal_fragment("$my_var"),
		});

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"variable","name":"my_var"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_parameter_positional() {
		let expr = Expression::Parameter(ParameterExpression::Positional {
			fragment: internal_fragment("$1"),
		});

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"parameter_positional","position":"1"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_parameter_named() {
		let expr = Expression::Parameter(ParameterExpression::Named {
			fragment: internal_fragment("$name"),
		});

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"parameter_named","name":"name"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	// =========================================================================
	// Comparison tests
	// =========================================================================

	#[test]
	fn test_greater_than() {
		let expr = Expression::GreaterThan(GreaterThanExpression {
			left: Box::new(column_expr("age")),
			right: Box::new(constant_number("18")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"greater_than","left":{"type":"column","namespace":"_context","source":"_context","name":"age"},"right":{"type":"number","value":"18"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_greater_than_equal() {
		let expr = Expression::GreaterThanEqual(GreaterThanEqExpression {
			left: Box::new(column_expr("price")),
			right: Box::new(constant_number("100")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"greater_than_equal","left":{"type":"column","namespace":"_context","source":"_context","name":"price"},"right":{"type":"number","value":"100"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_less_than() {
		let expr = Expression::LessThan(LessThanExpression {
			left: Box::new(column_expr("count")),
			right: Box::new(constant_number("10")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"less_than","left":{"type":"column","namespace":"_context","source":"_context","name":"count"},"right":{"type":"number","value":"10"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_less_than_equal() {
		let expr = Expression::LessThanEqual(LessThanEqExpression {
			left: Box::new(column_expr("quantity")),
			right: Box::new(constant_number("5")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"less_than_equal","left":{"type":"column","namespace":"_context","source":"_context","name":"quantity"},"right":{"type":"number","value":"5"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_equal() {
		let expr = Expression::Equal(EqExpression {
			left: Box::new(column_expr("status")),
			right: Box::new(constant_text("active")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"equal","left":{"type":"column","namespace":"_context","source":"_context","name":"status"},"right":{"type":"text","value":"active"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_not_equal() {
		let expr = Expression::NotEqual(NotEqExpression {
			left: Box::new(column_expr("type")),
			right: Box::new(constant_text("deleted")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"not_equal","left":{"type":"column","namespace":"_context","source":"_context","name":"type"},"right":{"type":"text","value":"deleted"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	// =========================================================================
	// Logical tests
	// =========================================================================

	#[test]
	fn test_and() {
		let expr = Expression::And(AndExpression {
			left: Box::new(Expression::GreaterThan(GreaterThanExpression {
				left: Box::new(column_expr("age")),
				right: Box::new(constant_number("18")),
				fragment: Fragment::None,
			})),
			right: Box::new(Expression::Equal(EqExpression {
				left: Box::new(column_expr("active")),
				right: Box::new(constant_bool("true")),
				fragment: Fragment::None,
			})),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"and","left":{"type":"greater_than","left":{"type":"column","namespace":"_context","source":"_context","name":"age"},"right":{"type":"number","value":"18"}},"right":{"type":"equal","left":{"type":"column","namespace":"_context","source":"_context","name":"active"},"right":{"type":"bool","value":"true"}}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_or() {
		let expr = Expression::Or(OrExpression {
			left: Box::new(column_expr("a")),
			right: Box::new(column_expr("b")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"or","left":{"type":"column","namespace":"_context","source":"_context","name":"a"},"right":{"type":"column","namespace":"_context","source":"_context","name":"b"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_xor() {
		let expr = Expression::Xor(XorExpression {
			left: Box::new(column_expr("x")),
			right: Box::new(column_expr("y")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"xor","left":{"type":"column","namespace":"_context","source":"_context","name":"x"},"right":{"type":"column","namespace":"_context","source":"_context","name":"y"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	// =========================================================================
	// Arithmetic tests
	// =========================================================================

	#[test]
	fn test_add() {
		let expr = Expression::Add(AddExpression {
			left: Box::new(column_expr("price")),
			right: Box::new(constant_number("10")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"add","left":{"type":"column","namespace":"_context","source":"_context","name":"price"},"right":{"type":"number","value":"10"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_sub() {
		let expr = Expression::Sub(SubExpression {
			left: Box::new(column_expr("total")),
			right: Box::new(constant_number("5")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"sub","left":{"type":"column","namespace":"_context","source":"_context","name":"total"},"right":{"type":"number","value":"5"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_mul() {
		let expr = Expression::Mul(MulExpression {
			left: Box::new(column_expr("qty")),
			right: Box::new(constant_number("2")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"mul","left":{"type":"column","namespace":"_context","source":"_context","name":"qty"},"right":{"type":"number","value":"2"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_div() {
		let expr = Expression::Div(DivExpression {
			left: Box::new(column_expr("amount")),
			right: Box::new(constant_number("4")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"div","left":{"type":"column","namespace":"_context","source":"_context","name":"amount"},"right":{"type":"number","value":"4"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_rem() {
		let expr = Expression::Rem(RemExpression {
			left: Box::new(column_expr("num")),
			right: Box::new(constant_number("3")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"rem","left":{"type":"column","namespace":"_context","source":"_context","name":"num"},"right":{"type":"number","value":"3"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	// =========================================================================
	// Complex expression tests
	// =========================================================================

	#[test]
	fn test_alias() {
		let expr = Expression::Alias(AliasExpression {
			alias: IdentExpression(internal_fragment("user_name")),
			expression: Box::new(column_expr("name")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"alias","alias":"user_name","expression":{"type":"column","namespace":"_context","source":"_context","name":"name"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_cast() {
		let expr = Expression::Cast(CastExpression {
			expression: Box::new(column_expr("value")),
			to: TypeExpression {
				ty: reifydb_type::Type::Int4,
				fragment: internal_fragment("Int4"),
			},
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"cast","expression":{"type":"column","namespace":"_context","source":"_context","name":"value"},"to":"Int4"}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_call() {
		let expr = Expression::Call(CallExpression {
			func: IdentExpression(internal_fragment("math::avg")),
			args: vec![column_expr("price")],
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"call","function":"math::avg","args":[{"type":"column","namespace":"_context","source":"_context","name":"price"}]}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_tuple() {
		let expr = Expression::Tuple(TupleExpression {
			expressions: vec![constant_number("1"), constant_number("2"), constant_number("3")],
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"tuple","expressions":[{"type":"number","value":"1"},{"type":"number","value":"2"},{"type":"number","value":"3"}]}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_prefix_minus() {
		let expr = Expression::Prefix(PrefixExpression {
			operator: PrefixOperator::Minus(Fragment::None),
			expression: Box::new(column_expr("value")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"prefix","operator":"-","expression":{"type":"column","namespace":"_context","source":"_context","name":"value"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_prefix_not() {
		let expr = Expression::Prefix(PrefixExpression {
			operator: PrefixOperator::Not(Fragment::None),
			expression: Box::new(column_expr("flag")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"prefix","operator":"not","expression":{"type":"column","namespace":"_context","source":"_context","name":"flag"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_between() {
		let expr = Expression::Between(BetweenExpression {
			value: Box::new(column_expr("age")),
			lower: Box::new(constant_number("18")),
			upper: Box::new(constant_number("65")),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"between","value":{"type":"column","namespace":"_context","source":"_context","name":"age"},"lower":{"type":"number","value":"18"},"upper":{"type":"number","value":"65"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_in() {
		let expr = Expression::In(InExpression {
			value: Box::new(column_expr("status")),
			list: Box::new(Expression::Tuple(TupleExpression {
				expressions: vec![constant_text("active"), constant_text("pending")],
				fragment: Fragment::None,
			})),
			negated: false,
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"in","value":{"type":"column","namespace":"_context","source":"_context","name":"status"},"list":{"type":"tuple","expressions":[{"type":"text","value":"active"},{"type":"text","value":"pending"}]},"negated":false}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_not_in() {
		let expr = Expression::In(InExpression {
			value: Box::new(column_expr("type")),
			list: Box::new(Expression::Tuple(TupleExpression {
				expressions: vec![constant_text("deleted"), constant_text("archived")],
				fragment: Fragment::None,
			})),
			negated: true,
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"in","value":{"type":"column","namespace":"_context","source":"_context","name":"type"},"list":{"type":"tuple","expressions":[{"type":"text","value":"deleted"},{"type":"text","value":"archived"}]},"negated":true}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_if_simple() {
		let expr = Expression::If(IfExpression {
			condition: Box::new(Expression::GreaterThan(GreaterThanExpression {
				left: Box::new(column_expr("age")),
				right: Box::new(constant_number("18")),
				fragment: Fragment::None,
			})),
			then_expr: Box::new(constant_text("adult")),
			else_ifs: vec![],
			else_expr: Some(Box::new(constant_text("minor"))),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"if","condition":{"type":"greater_than","left":{"type":"column","namespace":"_context","source":"_context","name":"age"},"right":{"type":"number","value":"18"}},"then":{"type":"text","value":"adult"},"else_ifs":[],"else_expr":{"type":"text","value":"minor"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_if_with_else_if() {
		let expr = Expression::If(IfExpression {
			condition: Box::new(Expression::GreaterThan(GreaterThanExpression {
				left: Box::new(column_expr("score")),
				right: Box::new(constant_number("90")),
				fragment: Fragment::None,
			})),
			then_expr: Box::new(constant_text("A")),
			else_ifs: vec![ElseIfExpression {
				condition: Box::new(Expression::GreaterThan(GreaterThanExpression {
					left: Box::new(column_expr("score")),
					right: Box::new(constant_number("80")),
					fragment: Fragment::None,
				})),
				then_expr: Box::new(constant_text("B")),
				fragment: Fragment::None,
			}],
			else_expr: Some(Box::new(constant_text("C"))),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"if","condition":{"type":"greater_than","left":{"type":"column","namespace":"_context","source":"_context","name":"score"},"right":{"type":"number","value":"90"}},"then":{"type":"text","value":"A"},"else_ifs":[{"condition":{"type":"greater_than","left":{"type":"column","namespace":"_context","source":"_context","name":"score"},"right":{"type":"number","value":"80"}},"then":{"type":"text","value":"B"}}],"else_expr":{"type":"text","value":"C"}}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_map() {
		let expr = Expression::Map(MapExpression {
			expressions: vec![
				Expression::Alias(AliasExpression {
					alias: IdentExpression(internal_fragment("user_name")),
					expression: Box::new(column_expr("name")),
					fragment: Fragment::None,
				}),
				column_expr("id"),
			],
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"map","expressions":[{"type":"alias","alias":"user_name","expression":{"type":"column","namespace":"_context","source":"_context","name":"name"}},{"type":"column","namespace":"_context","source":"_context","name":"id"}]}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_extend() {
		let expr = Expression::Extend(ExtendExpression {
			expressions: vec![Expression::Alias(AliasExpression {
				alias: IdentExpression(internal_fragment("full_name")),
				expression: Box::new(Expression::Add(AddExpression {
					left: Box::new(column_expr("first")),
					right: Box::new(column_expr("last")),
					fragment: Fragment::None,
				})),
				fragment: Fragment::None,
			})],
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		assert_eq!(
			json,
			r#"{"type":"extend","expressions":[{"type":"alias","alias":"full_name","expression":{"type":"add","left":{"type":"column","namespace":"_context","source":"_context","name":"first"},"right":{"type":"column","namespace":"_context","source":"_context","name":"last"}}}]}"#
		);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	#[test]
	fn test_type_expression() {
		let expr = Expression::Type(TypeExpression {
			ty: reifydb_type::Type::Utf8,
			fragment: internal_fragment("Utf8"),
		});

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"type","ty":"Utf8"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}

	// =========================================================================
	// Complex nested expression test
	// =========================================================================

	#[test]
	fn test_complex_nested_expression() {
		// Build: (age > 18 AND status = 'active') OR (role = 'admin')
		let expr = Expression::Or(OrExpression {
			left: Box::new(Expression::And(AndExpression {
				left: Box::new(Expression::GreaterThan(GreaterThanExpression {
					left: Box::new(column_expr("age")),
					right: Box::new(constant_number("18")),
					fragment: Fragment::None,
				})),
				right: Box::new(Expression::Equal(EqExpression {
					left: Box::new(column_expr("status")),
					right: Box::new(constant_text("active")),
					fragment: Fragment::None,
				})),
				fragment: Fragment::None,
			})),
			right: Box::new(Expression::Equal(EqExpression {
				left: Box::new(column_expr("role")),
				right: Box::new(constant_text("admin")),
				fragment: Fragment::None,
			})),
			fragment: Fragment::None,
		});

		let json = to_json(&expr);
		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);

		// Verify pretty print works
		let pretty = to_json_pretty(&expr);
		assert!(pretty.contains('\n'));
		assert!(pretty.contains("greater_than"));
	}

	#[test]
	fn test_access_source() {
		let expr = Expression::AccessSource(AccessSourceExpression {
			column: ColumnIdentifier {
				source: ColumnSource::Source {
					namespace: internal_fragment("public"),
					source: internal_fragment("users"),
				},
				name: internal_fragment("email"),
			},
		});

		let json = to_json(&expr);
		assert_eq!(json, r#"{"type":"access_source","namespace":"public","source":"users","name":"email"}"#);

		let recovered = from_json(&json).unwrap();
		assert_eq!(to_json(&recovered), json);
	}
}
