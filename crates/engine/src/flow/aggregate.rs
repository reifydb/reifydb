// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnShape};
use reifydb_routine::routine::registry::Routines;
use reifydb_rql::expression::{ColumnExpression, Expression};
use reifydb_value::fragment::Fragment;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotKind {
	Count {
		count_star: bool,
	},
	Sum,
	Avg,
	Min,
	Max,
}

pub enum SlotArg {
	Star,
	Column(String),
	Expr(Expression),
}

pub fn synthetic_aggregate_column_name(idx: usize) -> String {
	format!("__aggregate{idx}")
}

pub fn synthetic_aggregate_column(idx: usize) -> Expression {
	let name = synthetic_aggregate_column_name(idx);
	Expression::Column(ColumnExpression(ColumnIdentifier {
		shape: ColumnShape::Alias(Fragment::internal(name.clone())),
		name: Fragment::internal(name),
	}))
}

pub fn classify_slot(routines: &Routines, expr: &Expression) -> Option<(SlotKind, SlotArg)> {
	let inner = match expr {
		Expression::Alias(alias) => alias.expression.as_ref(),
		other => other,
	};
	let call = match inner {
		Expression::Call(c) => c,
		_ => return None,
	};
	let name = call.func.0.text().to_string();
	routines.get_aggregate_function(&name)?;
	let arg = match call.args.as_slice() {
		[] => SlotArg::Star,
		[Expression::Column(col)] => SlotArg::Column(col.0.name.text().to_string()),
		[single] => SlotArg::Expr(single.clone()),
		_ => return None,
	};
	let is_star = matches!(arg, SlotArg::Star);
	let short = name.rsplit("::").next().unwrap_or(&name);
	let kind = match short {
		"count" => SlotKind::Count {
			count_star: is_star,
		},
		"sum" if !is_star => SlotKind::Sum,
		"avg" if !is_star => SlotKind::Avg,
		"min" if !is_star => SlotKind::Min,
		"max" if !is_star => SlotKind::Max,
		_ => return None,
	};
	Some((kind, arg))
}

pub fn rewrite_aggregates(routines: &Routines, expr: &mut Expression, slots: &mut Vec<(SlotKind, SlotArg)>) -> bool {
	if let Some((kind, arg)) = classify_slot(routines, expr) {
		let idx = slots.len();
		slots.push((kind, arg));
		*expr = synthetic_aggregate_column(idx);
		return true;
	}
	match expr {
		Expression::Alias(a) => rewrite_aggregates(routines, a.expression.as_mut(), slots),
		Expression::Cast(c) => rewrite_aggregates(routines, c.expression.as_mut(), slots),
		Expression::Prefix(p) => rewrite_aggregates(routines, p.expression.as_mut(), slots),
		Expression::Add(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots);
			l && r
		}
		Expression::Sub(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots);
			l && r
		}
		Expression::Mul(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots);
			l && r
		}
		Expression::Div(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots);
			l && r
		}
		Expression::Rem(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots);
			l && r
		}
		Expression::Constant(_) => true,
		_ => false,
	}
}

pub fn is_representable(routines: &Routines, expr: &Expression) -> bool {
	let mut cloned = expr.clone();
	let mut slots: Vec<(SlotKind, SlotArg)> = Vec::new();
	rewrite_aggregates(routines, &mut cloned, &mut slots)
}
