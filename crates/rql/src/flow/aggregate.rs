// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnObject};
use reifydb_routine_abi::registry::Routines;
use reifydb_value::fragment::Fragment;

use crate::expression::{ColumnExpression, Expression};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SlotKind {
	Count {
		count_star: bool,
	},
	Sum,
	Avg,
	Min,
	Max,
	First,
	Last,
	WindowStart,
	WindowEnd,
	WindowDuration,
	WindowLast,
}

impl SlotKind {
	pub fn requires_span(self) -> bool {
		matches!(self, SlotKind::WindowStart | SlotKind::WindowEnd | SlotKind::WindowDuration)
	}

	pub fn requires_event_time(self) -> bool {
		matches!(self, SlotKind::WindowLast)
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateContext {
	Windowed,
	Grouped,
}

pub enum SlotArg {
	Star,
	Column(String),
	Expr(Expression),
	EventTime,
}

fn window_slot_kind(name: &str) -> Option<SlotKind> {
	match name {
		"window::start" => Some(SlotKind::WindowStart),
		"window::end" => Some(SlotKind::WindowEnd),
		"window::duration" => Some(SlotKind::WindowDuration),
		"window::last" => Some(SlotKind::WindowLast),
		_ => None,
	}
}

pub fn synthetic_aggregate_column_name(idx: usize) -> String {
	format!("__aggregate{idx}")
}

pub fn synthetic_aggregate_column(idx: usize) -> Expression {
	let name = synthetic_aggregate_column_name(idx);
	Expression::Column(ColumnExpression(ColumnIdentifier {
		object: ColumnObject::Alias(Fragment::internal(name.clone())),
		name: Fragment::internal(name),
	}))
}

pub fn classify_slot(routines: &Routines, expr: &Expression, context: AggregateContext) -> Option<(SlotKind, SlotArg)> {
	let inner = match expr {
		Expression::Alias(alias) => alias.expression.as_ref(),
		other => other,
	};
	let call = match inner {
		Expression::Call(c) => c,
		_ => return None,
	};
	let name = call.func.0.text().to_string();
	if let Some(kind) = window_slot_kind(&name) {
		if context == AggregateContext::Grouped || !call.args.is_empty() {
			return None;
		}
		let arg = if kind.requires_event_time() {
			SlotArg::EventTime
		} else {
			SlotArg::Star
		};
		return Some((kind, arg));
	}
	let short = name.rsplit("::").next().unwrap_or(&name);
	let is_first_or_last = matches!(short, "first" | "last");
	if is_first_or_last {
		if context == AggregateContext::Grouped {
			return None;
		}
	} else {
		routines.get_aggregate_function(&name)?;
	}
	let arg = match call.args.as_slice() {
		[] => SlotArg::Star,
		[Expression::Column(col)] => SlotArg::Column(col.0.name.text().to_string()),
		[single] => SlotArg::Expr(single.clone()),
		_ => return None,
	};
	let is_star = matches!(arg, SlotArg::Star);
	let kind = match short {
		"count" => SlotKind::Count {
			count_star: is_star,
		},
		"sum" if !is_star => SlotKind::Sum,
		"avg" if !is_star => SlotKind::Avg,
		"min" if !is_star => SlotKind::Min,
		"max" if !is_star => SlotKind::Max,
		"first" if !is_star => SlotKind::First,
		"last" if !is_star => SlotKind::Last,
		_ => return None,
	};
	Some((kind, arg))
}

pub fn rewrite_aggregates(
	routines: &Routines,
	expr: &mut Expression,
	slots: &mut Vec<(SlotKind, SlotArg)>,
	context: AggregateContext,
) -> bool {
	if let Some((kind, arg)) = classify_slot(routines, expr, context) {
		let idx = slots.len();
		slots.push((kind, arg));
		*expr = synthetic_aggregate_column(idx);
		return true;
	}
	match expr {
		Expression::Alias(a) => rewrite_aggregates(routines, a.expression.as_mut(), slots, context),
		Expression::Cast(c) => rewrite_aggregates(routines, c.expression.as_mut(), slots, context),
		Expression::Prefix(p) => rewrite_aggregates(routines, p.expression.as_mut(), slots, context),
		Expression::Add(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots, context);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots, context);
			l && r
		}
		Expression::Sub(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots, context);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots, context);
			l && r
		}
		Expression::Mul(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots, context);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots, context);
			l && r
		}
		Expression::Div(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots, context);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots, context);
			l && r
		}
		Expression::Rem(e) => {
			let l = rewrite_aggregates(routines, e.left.as_mut(), slots, context);
			let r = rewrite_aggregates(routines, e.right.as_mut(), slots, context);
			l && r
		}
		Expression::Constant(_) => true,
		_ => false,
	}
}

pub fn collect_slots(
	routines: &Routines,
	expr: &Expression,
	context: AggregateContext,
) -> Option<Vec<(SlotKind, SlotArg)>> {
	let mut cloned = expr.clone();
	let mut slots: Vec<(SlotKind, SlotArg)> = Vec::new();
	rewrite_aggregates(routines, &mut cloned, &mut slots, context).then_some(slots)
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnObject};

	use super::*;
	use crate::expression::{CallExpression, ColumnExpression, IdentExpression};

	fn call(name: &str, args: Vec<Expression>) -> Expression {
		Expression::Call(CallExpression {
			func: IdentExpression(Fragment::internal(name.to_string())),
			args,
			fragment: Fragment::internal(name.to_string()),
		})
	}

	fn column(name: &str) -> Expression {
		Expression::Column(ColumnExpression(ColumnIdentifier {
			object: ColumnObject::Alias(Fragment::internal(name.to_string())),
			name: Fragment::internal(name.to_string()),
		}))
	}

	fn classify(name: &str, args: Vec<Expression>) -> Option<(SlotKind, SlotArg)> {
		classify_slot(&Routines::empty(), &call(name, args), AggregateContext::Windowed)
	}

	#[test]
	fn window_last_is_not_swallowed_by_the_bare_last_aggregate() {
		// classify_slot shortens a namespaced name to its final segment, so "window::last" arrives at the
		// name match as "last". Reaching that match with zero arguments falls through to the catch-all and
		// returns None, which the compiler reads as "not an aggregate" rather than as an error: the whole
		// window would be rejected with a misleading diagnostic. The window branch must run on the full
		// name, before the shortening.
		let (kind, arg) = classify("window::last", vec![]).expect("window::last must classify");
		assert_eq!(kind, SlotKind::WindowLast);
		assert!(matches!(arg, SlotArg::EventTime), "window::last must read the row event time");
	}

	#[test]
	fn bare_last_keeps_its_own_slot_kind_and_column_argument() {
		// window::last and last(col) share a final segment but not a meaning: one reports when the newest
		// row landed, the other reports a value from it. Collapsing them would silently answer the wrong
		// question.
		let (kind, arg) = classify("last", vec![column("price")]).expect("last(price) must classify");
		assert_eq!(kind, SlotKind::Last);
		match arg {
			SlotArg::Column(name) => assert_eq!(name, "price"),
			_ => panic!("last(price) must read its column"),
		}
	}

	#[test]
	fn bare_last_without_an_argument_is_still_rejected() {
		// The window branch must not widen the bare form; last() has nothing to report a value from.
		assert!(classify("last", vec![]).is_none());
	}

	#[test]
	fn every_window_function_maps_to_its_own_slot_kind() {
		assert_eq!(classify("window::start", vec![]).unwrap().0, SlotKind::WindowStart);
		assert_eq!(classify("window::end", vec![]).unwrap().0, SlotKind::WindowEnd);
		assert_eq!(classify("window::duration", vec![]).unwrap().0, SlotKind::WindowDuration);
		assert_eq!(classify("window::last", vec![]).unwrap().0, SlotKind::WindowLast);
	}

	#[test]
	fn the_span_functions_take_no_input_at_all() {
		// A span slot is filled from the window boundary at emit, never from a row. Handing it a row input
		// would make it accumulate, and the accumulated value would then be overwritten at emit.
		for name in ["window::start", "window::end", "window::duration"] {
			let (_, arg) = classify(name, vec![]).unwrap();
			assert!(matches!(arg, SlotArg::Star), "{name} must take no input");
		}
	}

	#[test]
	fn a_window_function_given_an_argument_is_rejected() {
		// Accepting and discarding an argument would let window::start(price) read as if it were scoped to
		// a column.
		for name in ["window::start", "window::end", "window::duration", "window::last"] {
			assert!(classify(name, vec![column("price")]).is_none(), "{name} must reject an argument");
		}
	}

	#[test]
	fn window_functions_are_rejected_in_a_grouped_aggregate() {
		// A grouped aggregate has no window, so there is no boundary and no bucket to report. Classifying
		// one here would emit a none column instead of failing at define time.
		for name in ["window::start", "window::end", "window::duration", "window::last"] {
			let expr = call(name, vec![]);
			assert!(
				classify_slot(&Routines::empty(), &expr, AggregateContext::Grouped).is_none(),
				"{name} must not classify in a grouped aggregate"
			);
		}
	}

	#[test]
	fn an_aliased_window_function_classifies_through_its_alias() {
		// Every real use is aliased (bucket_start: window::start()); missing the alias unwrap would reject
		// the only form anyone writes.
		use crate::expression::AliasExpression;
		let expr = Expression::Alias(AliasExpression {
			alias: IdentExpression(Fragment::internal("bucket_start".to_string())),
			expression: Box::new(call("window::start", vec![])),
			fragment: Fragment::internal("bucket_start".to_string()),
		});
		let (kind, _) = classify_slot(&Routines::empty(), &expr, AggregateContext::Windowed)
			.expect("aliased window::start must classify");
		assert_eq!(kind, SlotKind::WindowStart);
	}

	#[test]
	fn only_the_span_functions_require_a_boundary() {
		// window::last needs an event time, not a boundary; grouping it with the span slots would reject it
		// on rolling windows, where it is well defined.
		assert!(SlotKind::WindowStart.requires_span());
		assert!(SlotKind::WindowEnd.requires_span());
		assert!(SlotKind::WindowDuration.requires_span());
		assert!(!SlotKind::WindowLast.requires_span());
		assert!(SlotKind::WindowLast.requires_event_time());
		assert!(!SlotKind::WindowStart.requires_event_time());
	}
}
