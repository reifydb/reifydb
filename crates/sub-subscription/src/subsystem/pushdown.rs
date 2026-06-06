// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::FlowNodeId;
use reifydb_rql::{
	expression::{ColumnExpression, ConstantExpression, Expression},
	flow::{flow::FlowDag, node::FlowNodeType},
};

pub(super) struct SourcePushdown {
	parts: Vec<String>,
}

pub(super) fn append_pushdown(q: &mut String, pd: SourcePushdown) {
	for part in pd.parts {
		q.push_str(" | ");
		q.push_str(&part);
	}
}

pub(super) fn walk_for_source_pushdown(flow: &FlowDag, source_id: &FlowNodeId) -> SourcePushdown {
	let mut parts: Vec<String> = Vec::new();
	let mut current = *source_id;
	while let Some(node) = flow.get_node(&current) {
		if node.outputs.len() != 1 {
			break;
		}
		let next_id = node.outputs[0];
		let next = match flow.get_node(&next_id) {
			Some(n) => n,
			None => break,
		};
		match &next.ty {
			FlowNodeType::Filter {
				conditions,
			} => match render_filter_clause(conditions) {
				Some(clause) => parts.push(clause),
				None => {
					return SourcePushdown {
						parts: Vec::new(),
					};
				}
			},
			FlowNodeType::Take {
				limit,
			} => {
				parts.push(format!("take {}", limit));
			}
			_ => break,
		}
		current = next_id;
	}
	SourcePushdown {
		parts,
	}
}

fn render_filter_clause(conditions: &[Expression]) -> Option<String> {
	if conditions.is_empty() {
		return None;
	}
	let mut rendered: Vec<String> = Vec::with_capacity(conditions.len());
	for c in conditions {
		rendered.push(render_expr_rql(c)?);
	}
	Some(format!("filter {{ {} }}", rendered.join(" and ")))
}

fn render_expr_rql(expr: &Expression) -> Option<String> {
	match expr {
		Expression::Constant(c) => Some(render_constant_rql(c)),
		Expression::Column(ColumnExpression(col)) => Some(col.name.text().to_string()),
		Expression::Equal(e) => {
			Some(format!("({} == {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::NotEqual(e) => {
			Some(format!("({} != {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::GreaterThan(e) => {
			Some(format!("({} > {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::GreaterThanEqual(e) => {
			Some(format!("({} >= {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::LessThan(e) => {
			Some(format!("({} < {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::LessThanEqual(e) => {
			Some(format!("({} <= {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::And(e) => {
			Some(format!("({} and {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::Or(e) => Some(format!("({} or {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?)),
		_ => None,
	}
}

fn render_constant_rql(c: &ConstantExpression) -> String {
	match c {
		ConstantExpression::None {
			..
		} => "none".to_string(),
		ConstantExpression::Bool {
			fragment,
		} => fragment.text().to_string(),
		ConstantExpression::Number {
			fragment,
		} => fragment.text().to_string(),
		ConstantExpression::Text {
			fragment,
		} => format!("'{}'", fragment.text()),
		ConstantExpression::Temporal {
			fragment,
		} => fragment.text().to_string(),
	}
}

#[cfg(test)]
mod tests {
	use std::slice;

	use reifydb_rql::expression::parse_expression;

	use super::*;

	fn parse_one(rql: &str) -> Expression {
		parse_expression(rql).expect("parse").into_iter().next().expect("one expression")
	}

	#[test]
	fn render_filter_clause_emits_valid_rql_for_equality() {
		let expr = parse_one("kind == 'b'");
		let rendered = render_filter_clause(slice::from_ref(&expr)).expect("renders");
		assert_eq!(rendered, "filter { (kind == 'b') }");
	}

	#[test]
	fn render_filter_clause_emits_valid_rql_for_conjunction() {
		let expr = parse_one("kind == 'b' and value > 50");
		let rendered = render_filter_clause(slice::from_ref(&expr)).expect("renders");
		assert_eq!(rendered, "filter { ((kind == 'b') and (value > 50)) }");
	}

	#[test]
	fn render_filter_clause_joins_multiple_conditions_with_and() {
		let exprs = vec![parse_one("kind == 'b'"), parse_one("value > 50")];
		let rendered = render_filter_clause(&exprs).expect("renders");
		assert_eq!(rendered, "filter { (kind == 'b') and (value > 50) }");
	}

	#[test]
	fn render_filter_clause_renders_text_constant_with_single_quotes() {
		// Input uses double quotes; output must use RQL-parseable quotes (single).
		let expr = parse_one("base_mint == \"So11111111111111111111111111111111111111112\"");
		let rendered = render_filter_clause(slice::from_ref(&expr)).expect("renders");
		assert_eq!(rendered, "filter { (base_mint == 'So11111111111111111111111111111111111111112') }");
	}

	#[test]
	fn render_filter_clause_returns_none_for_unsupported_expression() {
		let expr = parse_one("upper(kind) == 'B'");
		assert!(render_filter_clause(slice::from_ref(&expr)).is_none());
	}

	#[test]
	fn render_filter_clause_returns_none_for_empty_conditions() {
		assert!(render_filter_clause(&[]).is_none());
	}

	#[test]
	fn render_constant_handles_each_constant_kind() {
		let bool_e = parse_one("true");
		let num_e = parse_one("42");
		let text_e = parse_one("'hello'");

		assert_eq!(render_expr_rql(&bool_e).unwrap(), "true");
		assert_eq!(render_expr_rql(&num_e).unwrap(), "42");
		assert_eq!(render_expr_rql(&text_e).unwrap(), "'hello'");
	}

	#[test]
	fn render_filter_clause_round_trips_through_rql_parser() {
		// The whole point of the renderer is that the result parses again as RQL.
		let expr = parse_one("base_mint == 'So11111111111111111111111111111111111111112'");
		let rendered = render_filter_clause(slice::from_ref(&expr)).expect("renders");
		// Strip the leading "filter { " and trailing " }" to get just the conditions.
		let inner = rendered.strip_prefix("filter { ").and_then(|s| s.strip_suffix(" }")).expect("structure");
		parse_expression(inner).expect("rendered RQL must reparse");
	}
}
