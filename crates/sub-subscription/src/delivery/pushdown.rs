// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_engine::subscription::HydrationBound;
use reifydb_rql::{
	expression::{ColumnExpression, ConstantExpression, Expression, PrefixOperator},
	flow::{flow::FlowDag, operator::OperatorDef},
};

pub(super) struct SourcePushdown {
	parts: Vec<String>,
	bound: HydrationBound,
}

pub(super) fn append_pushdown(q: &mut String, pd: SourcePushdown) -> HydrationBound {
	for part in pd.parts {
		q.push_str(" | ");
		q.push_str(&part);
	}
	pd.bound
}

pub(super) fn walk_for_source_pushdown(flow: &FlowDag, source_id: &OperatorId) -> SourcePushdown {
	let mut parts: Vec<String> = Vec::new();
	let mut filters_renderable = true;
	let mut pushed_take = false;
	let mut blocker: Option<String> = None;
	let mut take_below_blocker = false;
	let mut current = *source_id;
	while let Some(operator) = flow.get_operator(&current) {
		if operator.outputs.len() != 1 {
			break;
		}
		let next_id = operator.outputs[0];
		let next = match flow.get_operator(&next_id) {
			Some(n) => n,
			None => break,
		};
		if blocker.is_some() {
			if matches!(&next.ty, OperatorDef::Take { .. }) {
				take_below_blocker = true;
			}
			current = next_id;
			continue;
		}
		match &next.ty {
			OperatorDef::Filter {
				conditions,
			} => {
				if !filters_renderable {
					blocker = Some(next.ty.label());
				} else {
					match render_filter_clause(conditions) {
						Some(clause) => parts.push(clause),
						None => blocker = Some(next.ty.label()),
					}
				}
			}
			OperatorDef::Take {
				limit,
			} => {
				parts.push(format!("sort {{created_at:DESC, rownum:DESC}} | take {}", limit));
				pushed_take = true;
			}
			OperatorDef::Map {
				..
			}
			| OperatorDef::Extend {
				..
			} => {
				filters_renderable = false;
			}
			_ => blocker = Some(next.ty.label()),
		}
		current = next_id;
	}
	let bound = match (pushed_take, blocker) {
		(true, _) => HydrationBound::Pushed,
		(false, Some(operator)) if take_below_blocker => HydrationBound::Blocked {
			operator,
		},
		(false, _) => HydrationBound::Absent,
	};
	SourcePushdown {
		parts,
		bound,
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
		Expression::Xor(e) => {
			Some(format!("({} xor {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?))
		}
		Expression::Add(e) => Some(format!("({} + {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?)),
		Expression::Sub(e) => Some(format!("({} - {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?)),
		Expression::Mul(e) => Some(format!("({} * {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?)),
		Expression::Div(e) => Some(format!("({} / {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?)),
		Expression::Rem(e) => Some(format!("({} % {})", render_expr_rql(&e.left)?, render_expr_rql(&e.right)?)),
		Expression::Between(e) => Some(format!(
			"({} between {} and {})",
			render_expr_rql(&e.value)?,
			render_expr_rql(&e.lower)?,
			render_expr_rql(&e.upper)?
		)),
		Expression::In(e) => Some(format!(
			"({} {} {})",
			render_expr_rql(&e.value)?,
			if e.negated {
				"not in"
			} else {
				"in"
			},
			render_expr_rql(&e.list)?
		)),
		Expression::List(l) => {
			let mut items = Vec::with_capacity(l.expressions.len());
			for item in &l.expressions {
				items.push(render_expr_rql(item)?);
			}
			Some(format!("[{}]", items.join(", ")))
		}
		Expression::Prefix(p) => {
			Some(format!("({}{})", render_prefix_operator(&p.operator), render_expr_rql(&p.expression)?))
		}
		Expression::Cast(c) => {
			Some(format!("cast({}, {})", render_expr_rql(&c.expression)?, c.to.fragment.text()))
		}
		_ => None,
	}
}

fn render_prefix_operator(operator: &PrefixOperator) -> &'static str {
	match operator {
		PrefixOperator::Minus(_) => "-",
		PrefixOperator::Plus(_) => "+",
		PrefixOperator::Not(_) => "not ",
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
		ConstantExpression::Duration {
			fragment,
		} => fragment.text().to_string(),
	}
}

#[cfg(test)]
mod tests {
	use std::slice;

	use reifydb_core::{
		common::TimeDomain,
		interface::catalog::{
			flow::{FlowEdgeId, FlowId},
			id::TableId,
		},
	};
	use reifydb_rql::{
		expression::parse_expression,
		flow::operator::{FlowEdge, FlowNode},
	};

	use super::*;

	fn parse_one(rql: &str) -> Expression {
		parse_expression(rql).expect("parse").into_iter().next().expect("one expression")
	}

	fn source() -> OperatorDef {
		OperatorDef::SourceTable {
			table: TableId(1),
			time_domain: TimeDomain::None,
		}
	}

	fn filter_on(rql: &str) -> OperatorDef {
		OperatorDef::Filter {
			conditions: vec![parse_one(rql)],
		}
	}

	fn chain(defs: Vec<OperatorDef>) -> (FlowDag, OperatorId) {
		let count = defs.len();
		let mut builder = FlowDag::builder(FlowId(1));
		for (i, def) in defs.into_iter().enumerate() {
			builder.add_node(FlowNode::new(OperatorId(i as u64 + 1), def));
		}
		for i in 1..count {
			builder.add_edge(FlowEdge::new(
				FlowEdgeId(i as u64),
				OperatorId(i as u64),
				OperatorId(i as u64 + 1),
			))
			.expect("chained edge");
		}
		(builder.build(), OperatorId(1))
	}

	fn walk(defs: Vec<OperatorDef>) -> Vec<String> {
		let (flow, source_id) = chain(defs);
		walk_for_source_pushdown(&flow, &source_id).parts
	}

	fn bound(defs: Vec<OperatorDef>) -> HydrationBound {
		let (flow, source_id) = chain(defs);
		walk_for_source_pushdown(&flow, &source_id).bound
	}

	#[test]
	fn bound_is_absent_when_the_query_carries_no_take() {
		let parts = bound(vec![source(), filter_on("kind == 'b'")]);

		assert_eq!(parts, HydrationBound::Absent);
	}

	#[test]
	fn bound_is_pushed_when_the_take_reached_the_source() {
		let parts = bound(vec![
			source(),
			OperatorDef::Take {
				limit: 5,
			},
		]);

		assert_eq!(parts, HydrationBound::Pushed);
	}

	#[test]
	fn bound_names_the_operator_that_blocked_a_take_below_it() {
		// Advising the user to add a take is wrong when they wrote one and an operator stopped it reaching the
		// source.
		let parts = bound(vec![
			source(),
			OperatorDef::Distinct {
				expressions: vec![parse_one("id")],
			},
			OperatorDef::Take {
				limit: 5,
			},
		]);

		assert_eq!(
			parts,
			HydrationBound::Blocked {
				operator: "Distinct".to_string(),
			}
		);
	}

	#[test]
	fn bound_is_absent_when_a_blocker_has_no_take_below_it() {
		// A gate with nothing below it leaves the pull genuinely unbounded, so the add-a-take advice still
		// applies.
		let parts = bound(vec![
			source(),
			OperatorDef::Gate {
				conditions: vec![parse_one("kind == 'b'")],
			},
		]);

		assert_eq!(parts, HydrationBound::Absent);
	}

	#[test]
	fn walk_keeps_parts_accumulated_before_an_unrenderable_filter() {
		// The take sits above the blocker, so it is already earned; discarding it pulls the whole source.
		let parts = walk(vec![
			source(),
			OperatorDef::Take {
				limit: 100,
			},
			filter_on("upper(kind) == 'B'"),
		]);

		assert_eq!(parts, vec!["sort {created_at:DESC, rownum:DESC} | take 100".to_string()]);
	}

	#[test]
	fn walk_stops_at_the_first_unrenderable_filter() {
		// A take may never be pushed below a filter that was not itself pushed, or the snapshot is
		// under-complete.
		let parts = walk(vec![
			source(),
			filter_on("kind == 'b'"),
			filter_on("upper(kind) == 'B'"),
			OperatorDef::Take {
				limit: 5,
			},
		]);

		assert_eq!(parts, vec!["filter { (kind == 'b') }".to_string()]);
	}

	#[test]
	fn walk_pushes_take_through_a_row_preserving_operator() {
		// Map is one row in, one row out and order preserving, so a take below it selects the same rows.
		let parts = walk(vec![
			source(),
			OperatorDef::Map {
				expressions: vec![parse_one("id")],
			},
			OperatorDef::Take {
				limit: 5,
			},
		]);

		assert_eq!(parts, vec!["sort {created_at:DESC, rownum:DESC} | take 5".to_string()]);
	}

	#[test]
	fn walk_pushes_take_through_extend() {
		let parts = walk(vec![
			source(),
			OperatorDef::Extend {
				expressions: vec![parse_one("id")],
			},
			OperatorDef::Take {
				limit: 5,
			},
		]);

		assert_eq!(parts, vec!["sort {created_at:DESC, rownum:DESC} | take 5".to_string()]);
	}

	#[test]
	fn walk_does_not_render_a_filter_after_a_map() {
		// Map may rename, drop or add columns, so the filter can reference a column the source does not have.
		let parts = walk(vec![
			source(),
			OperatorDef::Map {
				expressions: vec![parse_one("id")],
			},
			filter_on("kind == 'b'"),
		]);

		assert!(parts.is_empty(), "filter after a map must not reach the source query, got {:?}", parts);
	}

	#[test]
	fn walk_does_not_push_take_below_distinct() {
		// Distinct changes cardinality, so the source take would cut rows distinct had not yet collapsed.
		let parts = walk(vec![
			source(),
			OperatorDef::Distinct {
				expressions: vec![parse_one("id")],
			},
			OperatorDef::Take {
				limit: 5,
			},
		]);

		assert!(parts.is_empty(), "take must not be pushed below distinct, got {:?}", parts);
	}

	#[test]
	fn walk_does_not_push_take_below_gate() {
		// Gate is opaque to the renderer and may withhold rows, so a source take selects the wrong set.
		let parts = walk(vec![
			source(),
			OperatorDef::Gate {
				conditions: vec![parse_one("kind == 'b'")],
			},
			OperatorDef::Take {
				limit: 5,
			},
		]);

		assert!(parts.is_empty(), "take must not be pushed below gate, got {:?}", parts);
	}

	#[test]
	fn walk_stops_at_a_fan_out() {
		// Each consumer of a fan-out needs its own row set, so a bound earned past the split would starve the
		// other branch.
		let mut builder = FlowDag::builder(FlowId(1));
		builder.add_node(FlowNode::new(OperatorId(1), source()));
		builder.add_node(FlowNode::new(OperatorId(2), filter_on("kind == 'b'")));
		builder.add_node(FlowNode::new(
			OperatorId(3),
			OperatorDef::Take {
				limit: 5,
			},
		));
		builder.add_node(FlowNode::new(
			OperatorId(4),
			OperatorDef::Map {
				expressions: vec![parse_one("id")],
			},
		));
		builder.add_edge(FlowEdge::new(FlowEdgeId(1), OperatorId(1), OperatorId(2))).expect("source edge");
		builder.add_edge(FlowEdge::new(FlowEdgeId(2), OperatorId(2), OperatorId(3))).expect("take branch");
		builder.add_edge(FlowEdge::new(FlowEdgeId(3), OperatorId(2), OperatorId(4))).expect("map branch");

		let pd = walk_for_source_pushdown(&builder.build(), &OperatorId(1));

		assert_eq!(pd.parts, vec!["filter { (kind == 'b') }".to_string()]);
		assert_eq!(pd.bound, HydrationBound::Absent, "a take past the split must not count as a pushed bound");
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

	fn render_and_reparse(rql: &str) -> String {
		let expr = parse_one(rql);
		let rendered = render_expr_rql(&expr).expect("renders");
		parse_expression(&rendered).expect("rendered rql must parse back, or the source query is malformed");
		rendered
	}

	#[test]
	fn render_reparses_arithmetic() {
		assert_eq!(render_and_reparse("qty + 1 > 10"), "((qty + 1) > 10)");
		assert_eq!(render_and_reparse("qty - 1 > 10"), "((qty - 1) > 10)");
		assert_eq!(render_and_reparse("qty * 2 > 10"), "((qty * 2) > 10)");
		assert_eq!(render_and_reparse("qty / 2 > 10"), "((qty / 2) > 10)");
		assert_eq!(render_and_reparse("qty % 2 > 10"), "((qty % 2) > 10)");
	}

	#[test]
	fn render_reparses_prefix() {
		assert_eq!(render_and_reparse("not active"), "(not active)");
		assert_eq!(render_and_reparse("-qty > 10"), "((-qty) > 10)");
	}

	#[test]
	fn render_reparses_between_and_in() {
		assert_eq!(render_and_reparse("qty between 1 and 5"), "(qty between 1 and 5)");
		assert_eq!(render_and_reparse("id in [1, 2, 3]"), "(id in [1, 2, 3])");
	}

	#[test]
	fn render_reparses_xor() {
		assert_eq!(render_and_reparse("active xor pending"), "(active xor pending)");
	}

	#[test]
	fn render_keeps_the_cast_it_was_given() {
		// Dropping the cast would push a filter that compares a different type than the operator does.
		assert_eq!(render_and_reparse("cast(id, int4) == 1"), "(cast(id, int4) == 1)");
	}

	#[test]
	fn render_filter_clause_still_returns_none_for_a_call() {
		// A function's behaviour in the query engine is not something the renderer can promise matches the
		// operator.
		let expr = parse_one("upper(kind) == 'B'");
		assert!(render_filter_clause(slice::from_ref(&expr)).is_none());
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
		// The renderer exists so its output parses again as RQL.
		let expr = parse_one("base_mint == 'So11111111111111111111111111111111111111112'");
		let rendered = render_filter_clause(slice::from_ref(&expr)).expect("renders");
		let inner = rendered.strip_prefix("filter { ").and_then(|s| s.strip_suffix(" }")).expect("structure");
		parse_expression(inner).expect("rendered RQL must reparse");
	}
}
