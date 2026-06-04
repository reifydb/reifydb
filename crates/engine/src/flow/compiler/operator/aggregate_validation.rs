// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::error::diagnostic::flow::flow_unsupported_aggregate_expression;
use reifydb_rql::expression::{Expression, name::display_label};
use reifydb_value::{Result, error::Error};

pub(crate) fn validate_flow_aggregations(aggregations: &[Expression]) -> Result<()> {
	for expr in aggregations {
		if !is_representable(expr) {
			let output = display_label(expr).text().to_string();
			return Err(Error(Box::new(flow_unsupported_aggregate_expression(&output))));
		}
	}
	Ok(())
}

fn is_aggregate_call(expr: &Expression) -> bool {
	let inner = match expr {
		Expression::Alias(alias) => alias.expression.as_ref(),
		other => other,
	};
	let Expression::Call(call) = inner else {
		return false;
	};
	let name = call.func.0.text();
	let short = name.rsplit("::").next().unwrap_or(name);
	let argc = call.args.len();
	match short {
		"count" => argc <= 1,
		"sum" | "avg" | "min" | "max" => argc == 1,
		_ => false,
	}
}

fn is_representable(expr: &Expression) -> bool {
	if is_aggregate_call(expr) {
		return true;
	}
	match expr {
		Expression::Alias(a) => is_representable(a.expression.as_ref()),
		Expression::Cast(c) => is_representable(c.expression.as_ref()),
		Expression::Prefix(p) => is_representable(p.expression.as_ref()),
		Expression::Add(e) => is_representable(e.left.as_ref()) && is_representable(e.right.as_ref()),
		Expression::Sub(e) => is_representable(e.left.as_ref()) && is_representable(e.right.as_ref()),
		Expression::Mul(e) => is_representable(e.left.as_ref()) && is_representable(e.right.as_ref()),
		Expression::Div(e) => is_representable(e.left.as_ref()) && is_representable(e.right.as_ref()),
		Expression::Rem(e) => is_representable(e.left.as_ref()) && is_representable(e.right.as_ref()),
		Expression::Constant(_) => true,
		_ => false,
	}
}
