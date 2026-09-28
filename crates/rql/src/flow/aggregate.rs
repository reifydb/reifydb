// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{HashMap, HashSet},
	iter::once,
};

use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnObject};
use reifydb_routine_abi::registry::Routines;
use reifydb_value::{
	fragment::Fragment,
	value::digest::{
		DigestError,
		literal::{parse_accuracy, parse_percentile},
	},
};

use crate::expression::{
	CallExpression, ColumnExpression, ConstantExpression, Expression, IdentExpression, PrefixExpression,
	PrefixOperator,
};

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
	Digest {
		accuracy: Option<u32>,
	},
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

pub const DIGEST_FUNCTION: &str = "stats::digest";

pub const PERCENTILE_FUNCTION: &str = "stats::approx_percentile";

fn slot_call(expr: &Expression) -> Option<&CallExpression> {
	let inner = match expr {
		Expression::Alias(alias) => alias.expression.as_ref(),
		other => other,
	};
	match inner {
		Expression::Call(call) => Some(call),
		_ => None,
	}
}

fn input_arg(input: &Expression) -> SlotArg {
	match input {
		Expression::Column(col) => SlotArg::Column(col.0.name.text().to_string()),
		other => SlotArg::Expr(other.clone()),
	}
}

fn number_literal(argument: &Expression) -> Option<String> {
	match argument {
		Expression::Constant(ConstantExpression::Number {
			fragment,
		}) => Some(fragment.text().to_string()),
		Expression::Prefix(PrefixExpression {
			operator: PrefixOperator::Minus(_),
			expression,
			..
		}) => match expression.as_ref() {
			Expression::Constant(ConstantExpression::Number {
				fragment,
			}) => Some(format!("-{}", fragment.text())),
			_ => None,
		},
		_ => None,
	}
}

fn is_literal(argument: &Expression) -> bool {
	matches!(argument, Expression::Constant(_)) || number_literal(argument).is_some()
}

fn accuracy_literal(argument: &Expression) -> Result<u32, DigestError> {
	number_literal(argument).map_or(Err(DigestError::AccuracyNotANumber), |text| parse_accuracy(&text))
}

fn digest_accuracy_error(expr: &Expression) -> Option<DigestError> {
	let call = slot_call(expr)?;
	match (call.func.0.text(), call.args.as_slice()) {
		(DIGEST_FUNCTION, [_, accuracy]) => accuracy_literal(accuracy).err(),
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
	let call = slot_call(expr)?;
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
	if name == DIGEST_FUNCTION {
		return match call.args.as_slice() {
			[input] => Some((
				SlotKind::Digest {
					accuracy: None,
				},
				input_arg(input),
			)),
			[input, accuracy] => Some((
				SlotKind::Digest {
					accuracy: Some(accuracy_literal(accuracy).ok()?),
				},
				input_arg(input),
			)),
			_ => None,
		};
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
		[single] => input_arg(single),
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

#[derive(Debug)]
pub enum PercentileCallError {
	ArgumentCount {
		function: Fragment,
		actual: usize,
	},
	NotLiteral {
		argument: Fragment,
		position: usize,
	},
	PercentileNotANumber {
		argument: Fragment,
	},
	PercentileOutOfRange {
		argument: Fragment,
	},
	Accuracy {
		argument: Fragment,
		error: DigestError,
	},
}

#[derive(Debug, Default)]
pub struct DigestSlots {
	shared: HashMap<(String, Option<u32>), usize>,
	percentile: HashSet<usize>,
}

impl DigestSlots {
	pub fn function_written(&self, slot: usize) -> &'static str {
		if self.percentile.contains(&slot) {
			PERCENTILE_FUNCTION
		} else {
			DIGEST_FUNCTION
		}
	}
}

#[derive(Debug)]
pub enum AggregateCallError {
	DigestAccuracy(DigestError),
	Percentile(PercentileCallError),
}

pub fn rewrite_aggregates(
	routines: &Routines,
	expr: &mut Expression,
	slots: &mut Vec<(SlotKind, SlotArg)>,
	digests: &mut DigestSlots,
	context: AggregateContext,
) -> Result<bool, AggregateCallError> {
	let mut accuracy = None;
	let rewritten = rewrite_aggregate_calls(
		expr,
		&mut |node, _| {
			if accuracy.is_none() {
				accuracy = digest_accuracy_error(node);
			}
			classify_slot(routines, node, context)
		},
		slots,
		digests,
	);
	if let Some(error) = accuracy {
		return Err(AggregateCallError::DigestAccuracy(error));
	}
	rewritten.map(|reads| reads.is_some()).map_err(AggregateCallError::Percentile)
}

pub fn rewrite_aggregate_calls<T>(
	expr: &mut Expression,
	classify: &mut impl FnMut(&Expression, Option<&CallExpression>) -> Option<T>,
	slots: &mut Vec<T>,
	digests: &mut DigestSlots,
) -> Result<Option<usize>, PercentileCallError> {
	if let Expression::Call(call) = expr
		&& call.func.0.text() == PERCENTILE_FUNCTION
	{
		let Some(read) = percentile_read(call, classify, slots, digests)? else {
			return Ok(None);
		};
		*expr = read;
		return Ok(Some(1));
	}
	if let Some(slot) = classify(expr, None) {
		let idx = slots.len();
		slots.push(slot);
		*expr = synthetic_aggregate_column(idx);
		return Ok(Some(1));
	}
	match expr {
		Expression::Alias(a) => rewrite_aggregate_calls(a.expression.as_mut(), classify, slots, digests),
		Expression::Cast(c) => rewrite_aggregate_calls(c.expression.as_mut(), classify, slots, digests),
		Expression::Prefix(p) => rewrite_aggregate_calls(p.expression.as_mut(), classify, slots, digests),
		Expression::Add(e) => rewrite_operands(e.left.as_mut(), e.right.as_mut(), classify, slots, digests),
		Expression::Sub(e) => rewrite_operands(e.left.as_mut(), e.right.as_mut(), classify, slots, digests),
		Expression::Mul(e) => rewrite_operands(e.left.as_mut(), e.right.as_mut(), classify, slots, digests),
		Expression::Div(e) => rewrite_operands(e.left.as_mut(), e.right.as_mut(), classify, slots, digests),
		Expression::Rem(e) => rewrite_operands(e.left.as_mut(), e.right.as_mut(), classify, slots, digests),
		Expression::Constant(_) => Ok(Some(0)),
		_ => Ok(None),
	}
}

fn rewrite_operands<T>(
	left: &mut Expression,
	right: &mut Expression,
	classify: &mut impl FnMut(&Expression, Option<&CallExpression>) -> Option<T>,
	slots: &mut Vec<T>,
	digests: &mut DigestSlots,
) -> Result<Option<usize>, PercentileCallError> {
	let left = rewrite_aggregate_calls(left, classify, slots, digests)?;
	let right = rewrite_aggregate_calls(right, classify, slots, digests)?;
	Ok(left.zip(right).map(|(left, right)| left + right))
}

fn percentile_read<T>(
	call: &CallExpression,
	classify: &mut impl FnMut(&Expression, Option<&CallExpression>) -> Option<T>,
	slots: &mut Vec<T>,
	digests: &mut DigestSlots,
) -> Result<Option<Expression>, PercentileCallError> {
	let (input, percentile, accuracy) = match call.args.as_slice() {
		[input, percentile] => (input, percentile, None),
		[input, percentile, accuracy] => (input, percentile, Some(accuracy)),
		_ => {
			return Err(PercentileCallError::ArgumentCount {
				function: call.func.0.clone(),
				actual: call.args.len(),
			});
		}
	};
	check_percentile_literal(percentile)?;
	let ppm = accuracy.map(percentile_accuracy).transpose()?;
	let key = match input {
		Expression::Column(column) => Some((column.0.name.text().to_string(), ppm)),
		_ => None,
	};
	let idx = match key.as_ref().and_then(|key| digests.shared.get(key)) {
		Some(idx) => *idx,
		None => {
			let digest = Expression::Call(CallExpression {
				func: IdentExpression(Fragment::internal(DIGEST_FUNCTION)),
				args: once(input.clone()).chain(accuracy.cloned()).collect(),
				fragment: call.fragment.clone(),
			});
			let Some(slot) = classify(&digest, Some(call)) else {
				return Ok(None);
			};
			let idx = slots.len();
			slots.push(slot);
			digests.percentile.insert(idx);
			if let Some(key) = key {
				digests.shared.insert(key, idx);
			}
			idx
		}
	};
	Ok(Some(Expression::Call(CallExpression {
		func: call.func.clone(),
		args: vec![synthetic_aggregate_column(idx), percentile.clone()],
		fragment: call.fragment.clone(),
	})))
}

fn check_percentile_literal(argument: &Expression) -> Result<(), PercentileCallError> {
	let fragment = argument.full_fragment_owned();
	let Some(text) = number_literal(argument) else {
		return Err(match argument {
			Expression::Constant(_) => PercentileCallError::PercentileNotANumber {
				argument: fragment,
			},
			_ => PercentileCallError::NotLiteral {
				argument: fragment,
				position: 2,
			},
		});
	};
	match parse_percentile(Fragment::internal(text)) {
		Ok(_) => Ok(()),
		Err(DigestError::PercentileOutOfRange) => Err(PercentileCallError::PercentileOutOfRange {
			argument: fragment,
		}),
		Err(_) => Err(PercentileCallError::PercentileNotANumber {
			argument: fragment,
		}),
	}
}

fn percentile_accuracy(argument: &Expression) -> Result<u32, PercentileCallError> {
	let fragment = argument.full_fragment_owned();
	if !is_literal(argument) {
		return Err(PercentileCallError::NotLiteral {
			argument: fragment,
			position: 3,
		});
	}
	accuracy_literal(argument).map_err(|error| PercentileCallError::Accuracy {
		argument: fragment,
		error,
	})
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::identifier::{ColumnIdentifier, ColumnObject};

	use super::*;
	use crate::expression::{
		AddExpression, AliasExpression, CallExpression, ColumnExpression, IdentExpression, PrefixExpression,
		PrefixOperator, SubExpression,
	};

	fn call(name: &str, args: Vec<Expression>) -> Expression {
		Expression::Call(CallExpression {
			func: IdentExpression(Fragment::internal(name)),
			args,
			fragment: Fragment::internal(name),
		})
	}

	fn column(name: &str) -> Expression {
		Expression::Column(ColumnExpression(ColumnIdentifier {
			object: ColumnObject::Alias(Fragment::internal(name)),
			name: Fragment::internal(name),
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
		let expr = Expression::Alias(AliasExpression {
			alias: IdentExpression(Fragment::internal("bucket_start")),
			expression: Box::new(call("window::start", vec![])),
			fragment: Fragment::internal("bucket_start"),
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

	fn number(text: &str) -> Expression {
		Expression::Constant(ConstantExpression::Number {
			fragment: Fragment::internal(text),
		})
	}

	fn negative(text: &str) -> Expression {
		Expression::Prefix(PrefixExpression {
			operator: PrefixOperator::Minus(Fragment::internal("-")),
			expression: Box::new(number(text)),
			fragment: Fragment::internal("-"),
		})
	}

	fn digest(args: Vec<Expression>) -> Expression {
		call("stats::digest", args)
	}

	#[test]
	fn stats_digest_with_a_literal_accuracy_is_a_digest_slot_holding_ppm() {
		// Accuracy is part of the digest type, so two spellings of one accuracy must build the same slot kind.
		for text in ["0.01", "0.010", "1e-2"] {
			let (kind, arg) = classify("stats::digest", vec![column("latency"), number(text)])
				.unwrap_or_else(|| panic!("stats::digest(latency, {text}) must classify"));
			assert_eq!(
				kind,
				SlotKind::Digest {
					accuracy: Some(10_000)
				},
				"{text}"
			);
			assert!(
				matches!(arg, SlotArg::Column(name) if name == "latency"),
				"the input must stay the column"
			);
		}
		let (kind, _) = classify("stats::digest", vec![column("latency"), number("0.001001")]).unwrap();
		assert_eq!(
			kind,
			SlotKind::Digest {
				accuracy: Some(1_001)
			},
			"an accuracy that is not a round float must keep its exact ppm"
		);
	}

	#[test]
	fn stats_digest_without_an_accuracy_is_a_digest_slot_with_no_accuracy() {
		// A digest input carries its own accuracy; inventing one would reject every stored digest of another.
		let (kind, arg) =
			classify("stats::digest", vec![column("lat")]).expect("stats::digest(lat) must classify");
		assert_eq!(
			kind,
			SlotKind::Digest {
				accuracy: None
			}
		);
		assert!(matches!(arg, SlotArg::Column(name) if name == "lat"));
	}

	#[test]
	fn stats_digest_over_an_expression_keeps_the_expression_as_its_input() {
		// Reading only a column name would drop the arithmetic and digest the wrong values.
		let input = Expression::Add(AddExpression {
			left: Box::new(column("latency")),
			right: Box::new(column("queue")),
			fragment: Fragment::internal("+"),
		});
		let (_, arg) = classify("stats::digest", vec![input, number("0.05")])
			.expect("an expression input must classify");
		assert!(matches!(arg, SlotArg::Expr(Expression::Add(_))), "the input must stay the expression");
	}

	#[test]
	fn stats_digest_is_a_slot_in_a_grouped_aggregate_too() {
		// A plain aggregate view is the simplest rollup; refusing the digest there leaves only windows.
		let (kind, _) = classify_slot(
			&Routines::empty(),
			&digest(vec![column("latency"), number("0.01")]),
			AggregateContext::Grouped,
		)
		.expect("stats::digest must classify in a grouped aggregate");
		assert_eq!(
			kind,
			SlotKind::Digest {
				accuracy: Some(10_000)
			}
		);
	}

	type AccuracyCase = (Expression, fn(&DigestError) -> bool);

	#[test]
	fn a_bad_accuracy_is_not_a_slot_and_names_its_own_error() {
		// Folding every bad accuracy into one generic rejection hides which rule the literal broke.
		let cases: [AccuracyCase; 7] = [
			(number("0.5"), |e| matches!(e, DigestError::AccuracyOutOfRange)),
			(number("0.0009"), |e| matches!(e, DigestError::AccuracyOutOfRange)),
			(negative("0.01"), |e| matches!(e, DigestError::AccuracyOutOfRange)),
			(number("0.0100005"), |e| matches!(e, DigestError::AccuracyNotWholePpm)),
			(number("0x10"), |e| matches!(e, DigestError::AccuracyNotANumber)),
			(column("acc"), |e| matches!(e, DigestError::AccuracyNotANumber)),
			(negative("acc"), |e| matches!(e, DigestError::AccuracyNotANumber)),
		];
		for (accuracy, expected) in cases {
			let expr = digest(vec![column("latency"), accuracy]);
			assert!(classify_slot(&Routines::empty(), &expr, AggregateContext::Windowed).is_none());
			let error = digest_accuracy_error(&expr).expect("a bad accuracy must name an error");
			assert!(expected(&error), "unexpected error {error:?}");
		}
		assert!(digest_accuracy_error(&digest(vec![column("latency"), number("0.1")])).is_none());
		assert!(digest_accuracy_error(&call("math::sum", vec![column("latency"), number("0.5")])).is_none());
	}

	#[test]
	fn stats_digest_with_no_input_or_an_extra_argument_is_rejected() {
		// Accepting and discarding an argument would silently build a digest the caller did not ask for.
		assert!(classify("stats::digest", vec![]).is_none());
		assert!(classify("stats::digest", vec![column("latency"), number("0.01"), number("0.5")]).is_none());
	}

	fn percentile(args: Vec<Expression>) -> Expression {
		call("stats::approx_percentile", args)
	}

	fn sub(left: Expression, right: Expression) -> Expression {
		Expression::Sub(SubExpression {
			left: Box::new(left),
			right: Box::new(right),
			fragment: Fragment::internal("-"),
		})
	}

	fn text(value: &str) -> Expression {
		Expression::Constant(ConstantExpression::Text {
			fragment: Fragment::internal(value),
		})
	}

	type RenderedSlot = (String, Option<String>);

	fn render_call(node: &Expression, origin: Option<&CallExpression>) -> Option<RenderedSlot> {
		match node {
			Expression::Call(call) => {
				Some((call.to_string(), origin.map(|origin| origin.func.0.text().to_string())))
			}
			_ => None,
		}
	}

	fn rewrite_node(
		expr: &mut Expression,
		slots: &mut Vec<RenderedSlot>,
		digests: &mut DigestSlots,
	) -> Result<Option<usize>, PercentileCallError> {
		rewrite_aggregate_calls(expr, &mut render_call, slots, digests)
	}

	fn rewrite_outputs(mut exprs: Vec<Expression>) -> (Vec<String>, Vec<RenderedSlot>) {
		let mut slots = Vec::new();
		let mut digests = DigestSlots::default();
		let outputs = exprs
			.iter_mut()
			.map(|expr| {
				let reads = rewrite_node(expr, &mut slots, &mut digests)
					.unwrap_or_else(|error| panic!("{expr} must rewrite, got {error:?}"));
				assert!(reads.is_some_and(|reads| reads > 0), "{expr} must read a slot");
				expr.to_string()
			})
			.collect();
		(outputs, slots)
	}

	fn rewrite_failure(mut expr: Expression) -> PercentileCallError {
		let mut slots = Vec::new();
		let failure = rewrite_node(&mut expr, &mut slots, &mut DigestSlots::default())
			.expect_err("a bad percentile argument must fail the rewrite");
		assert!(slots.is_empty(), "a bad argument must fail before a slot is built, got {slots:?}");
		failure
	}

	fn digest_slot(rendered: &str) -> RenderedSlot {
		(rendered.to_string(), Some("stats::approx_percentile".to_string()))
	}

	#[test]
	fn percentiles_on_one_column_and_one_accuracy_read_one_digest_slot() {
		// A digest per percentile on one column doubles the state and changes no answer, so sharing must not
		// break.
		let (outputs, slots) = rewrite_outputs(vec![
			percentile(vec![column("latency"), number("0.5"), number("0.01")]),
			percentile(vec![column("latency"), number("0.99"), number("0.010")]),
		]);

		assert_eq!(slots, vec![digest_slot("stats::digest(latency, Constant(0.01))")]);
		assert_eq!(
			outputs,
			vec![
				"stats::approx_percentile(__aggregate0, Constant(0.5))",
				"stats::approx_percentile(__aggregate0, Constant(0.99))"
			]
		);
	}

	#[test]
	fn percentiles_on_one_expression_read_a_digest_slot_each() {
		// Expressions are never matched, so two percentiles over latency - queue must build two digests.
		let (outputs, slots) = rewrite_outputs(vec![
			percentile(vec![sub(column("latency"), column("queue")), number("0.5"), number("0.01")]),
			percentile(vec![sub(column("latency"), column("queue")), number("0.99"), number("0.01")]),
		]);

		assert_eq!(
			slots,
			vec![
				digest_slot("stats::digest((latency - queue), Constant(0.01))"),
				digest_slot("stats::digest((latency - queue), Constant(0.01))")
			]
		);
		assert_eq!(
			outputs,
			vec![
				"stats::approx_percentile(__aggregate0, Constant(0.5))",
				"stats::approx_percentile(__aggregate1, Constant(0.99))"
			]
		);
	}

	#[test]
	fn the_frozen_sharing_rule_maps_a_fixed_call_set_to_a_pinned_slot_layout() {
		// Flow slot values are stored by position, so any change to this layout reads old state against new
		// slots.
		let (outputs, slots) = rewrite_outputs(vec![
			percentile(vec![column("latency"), number("0.5"), number("0.01")]),
			call("math::sum", vec![column("latency")]),
			percentile(vec![column("latency"), number("0.9"), number("0.05")]),
			percentile(vec![column("queue"), number("0.9"), number("0.01")]),
			percentile(vec![column("lat"), number("0.5")]),
			percentile(vec![sub(column("latency"), column("queue")), number("0.99"), number("0.01")]),
			percentile(vec![column("lat"), number("0.99")]),
			digest(vec![column("latency"), number("0.01")]),
			Expression::Add(AddExpression {
				left: Box::new(percentile(vec![column("latency"), number("0.99"), number("0.01")])),
				right: Box::new(percentile(vec![
					sub(column("latency"), column("queue")),
					number("0.99"),
					number("0.01"),
				])),
				fragment: Fragment::internal("+"),
			}),
			percentile(vec![column("latency"), number("0.75"), number("0.05")]),
		]);

		assert_eq!(
			slots,
			vec![
				digest_slot("stats::digest(latency, Constant(0.01))"),
				("math::sum(latency)".to_string(), None),
				digest_slot("stats::digest(latency, Constant(0.05))"),
				digest_slot("stats::digest(queue, Constant(0.01))"),
				digest_slot("stats::digest(lat)"),
				digest_slot("stats::digest((latency - queue), Constant(0.01))"),
				("stats::digest(latency, Constant(0.01))".to_string(), None),
				digest_slot("stats::digest((latency - queue), Constant(0.01))"),
			]
		);
		assert_eq!(
			outputs,
			vec![
				"stats::approx_percentile(__aggregate0, Constant(0.5))",
				"__aggregate1",
				"stats::approx_percentile(__aggregate2, Constant(0.9))",
				"stats::approx_percentile(__aggregate3, Constant(0.9))",
				"stats::approx_percentile(__aggregate4, Constant(0.5))",
				"stats::approx_percentile(__aggregate5, Constant(0.99))",
				"stats::approx_percentile(__aggregate4, Constant(0.99))",
				"__aggregate6",
				"(stats::approx_percentile(__aggregate0, Constant(0.99)) + stats::approx_percentile(__aggregate7, Constant(0.99)))",
				"stats::approx_percentile(__aggregate2, Constant(0.75))",
			]
		);
	}

	#[test]
	fn a_percentile_inside_an_alias_and_arithmetic_is_still_rewritten() {
		// Missing the recursion would leave the raw call for classify, which builds no digest for it.
		let mut expr = Expression::Alias(AliasExpression {
			alias: IdentExpression(Fragment::internal("p99_ms")),
			expression: Box::new(sub(
				percentile(vec![column("latency"), number("0.99"), number("0.01")]),
				number("1"),
			)),
			fragment: Fragment::internal("p99_ms"),
		});
		let mut slots = Vec::new();

		let reads = rewrite_node(&mut expr, &mut slots, &mut DigestSlots::default()).unwrap();

		assert_eq!(reads, Some(1));
		assert_eq!(slots, vec![digest_slot("stats::digest(latency, Constant(0.01))")]);
		assert_eq!(
			expr.to_string(),
			"(stats::approx_percentile(__aggregate0, Constant(0.99)) - Constant(1)) as p99_ms"
		);
	}

	#[test]
	fn constants_read_no_slot_and_a_bare_column_is_not_representable() {
		// A map entry of constants only must stay distinguishable from one that reads an aggregate.
		let mut slots = Vec::new();
		let mut digests = DigestSlots::default();

		assert_eq!(rewrite_node(&mut number("1"), &mut slots, &mut digests).unwrap(), Some(0));
		assert_eq!(rewrite_node(&mut column("latency"), &mut slots, &mut digests).unwrap(), None);
		assert!(slots.is_empty());
	}

	#[test]
	fn a_bad_p_fails_the_rewrite_with_its_own_error_before_any_slot_is_built() {
		// p is fixed per call inside an aggregate, so a bad p must fail before rows instead of on a read.
		let p = |value: Expression| percentile(vec![column("latency"), value, number("0.01")]);

		assert!(matches!(
			rewrite_failure(p(number("1.5"))),
			PercentileCallError::PercentileOutOfRange { argument } if argument.text() == "1.5"
		));
		assert!(matches!(
			rewrite_failure(p(negative("0.5"))),
			PercentileCallError::PercentileOutOfRange { .. }
		));
		assert!(matches!(
			rewrite_failure(p(column("target"))),
			PercentileCallError::NotLiteral { argument, position: 2 } if argument.text() == "target"
		));
		assert!(matches!(rewrite_failure(p(text("0.5"))), PercentileCallError::PercentileNotANumber { .. }));
		assert!(matches!(rewrite_failure(p(number("nan"))), PercentileCallError::PercentileNotANumber { .. }));
		assert!(matches!(
			rewrite_failure(percentile(vec![column("latency"), number("1.5"), number("0.5")])),
			PercentileCallError::PercentileOutOfRange { .. }
		));
	}

	#[test]
	fn a_bad_accuracy_or_argument_count_fails_the_rewrite_with_its_own_error() {
		// Each broken rule must name itself, and a percentile read must never pick an accuracy nobody wrote.
		let accuracy = |value: Expression| percentile(vec![column("latency"), number("0.5"), value]);

		assert!(matches!(
			rewrite_failure(accuracy(column("acc"))),
			PercentileCallError::NotLiteral { argument, position: 3 } if argument.text() == "acc"
		));
		assert!(matches!(
			rewrite_failure(accuracy(number("0.5"))),
			PercentileCallError::Accuracy {
				error: DigestError::AccuracyOutOfRange,
				..
			}
		));
		assert!(matches!(
			rewrite_failure(accuracy(number("0.0000005"))),
			PercentileCallError::Accuracy {
				error: DigestError::AccuracyNotWholePpm,
				..
			}
		));
		assert!(matches!(
			rewrite_failure(accuracy(text("0.01"))),
			PercentileCallError::Accuracy {
				error: DigestError::AccuracyNotANumber,
				..
			}
		));
		assert!(matches!(
			rewrite_failure(percentile(vec![column("latency")])),
			PercentileCallError::ArgumentCount {
				actual: 1,
				..
			}
		));
		assert!(matches!(
			rewrite_failure(percentile(vec![
				column("latency"),
				number("0.5"),
				number("0.01"),
				number("0.1")
			])),
			PercentileCallError::ArgumentCount {
				actual: 4,
				..
			}
		));
	}

	fn flow_slot(slot: &(SlotKind, SlotArg)) -> (SlotKind, String) {
		let input = match &slot.1 {
			SlotArg::Star => "*".to_string(),
			SlotArg::Column(name) => name.clone(),
			SlotArg::Expr(expr) => expr.to_string(),
			SlotArg::EventTime => "event time".to_string(),
		};
		(slot.0, input)
	}

	type FlowRewrite = (Vec<String>, Vec<(SlotKind, String)>);

	fn flow_rewrite(exprs: Vec<Expression>) -> Result<FlowRewrite, AggregateCallError> {
		let mut slots = Vec::new();
		let mut digests = DigestSlots::default();
		let mut outputs = Vec::new();
		for mut expr in exprs {
			let representable = rewrite_aggregates(
				&Routines::empty(),
				&mut expr,
				&mut slots,
				&mut digests,
				AggregateContext::Windowed,
			)?;
			assert!(representable, "{expr} must be representable");
			outputs.push(expr.to_string());
		}
		Ok((outputs, slots.iter().map(flow_slot).collect()))
	}

	fn digest_kind(ppm: Option<u32>) -> SlotKind {
		SlotKind::Digest {
			accuracy: ppm,
		}
	}

	#[test]
	fn the_flow_rewrite_shares_one_digest_slot_between_percentile_outputs_of_one_node() {
		// A flow slot per percentile output doubles the stored state of a view and changes no answer.
		let (outputs, slots) = flow_rewrite(vec![
			percentile(vec![column("latency"), number("0.5"), number("0.01")]),
			percentile(vec![column("latency"), number("0.99"), number("0.01")]),
		])
		.unwrap();

		assert_eq!(slots, vec![(digest_kind(Some(10_000)), "latency".to_string())]);
		assert_eq!(
			outputs,
			vec![
				"stats::approx_percentile(__aggregate0, Constant(0.5))",
				"stats::approx_percentile(__aggregate0, Constant(0.99))"
			]
		);
	}

	#[test]
	fn the_flow_rewrite_gives_an_expression_input_and_another_accuracy_their_own_digest_slots() {
		// A shared slot for another accuracy or an expression would answer from a digest the call never asked
		// for.
		let (outputs, slots) = flow_rewrite(vec![
			percentile(vec![column("latency"), number("0.5"), number("0.01")]),
			percentile(vec![column("latency"), number("0.5"), number("0.05")]),
			percentile(vec![sub(column("latency"), column("queue")), number("0.5"), number("0.01")]),
			percentile(vec![sub(column("latency"), column("queue")), number("0.9"), number("0.01")]),
			percentile(vec![column("lat"), number("0.5")]),
			digest(vec![column("latency"), number("0.01")]),
		])
		.unwrap();

		assert_eq!(
			slots,
			vec![
				(digest_kind(Some(10_000)), "latency".to_string()),
				(digest_kind(Some(50_000)), "latency".to_string()),
				(digest_kind(Some(10_000)), "(latency - queue)".to_string()),
				(digest_kind(Some(10_000)), "(latency - queue)".to_string()),
				(digest_kind(None), "lat".to_string()),
				(digest_kind(Some(10_000)), "latency".to_string()),
			]
		);
		assert_eq!(
			outputs,
			vec![
				"stats::approx_percentile(__aggregate0, Constant(0.5))",
				"stats::approx_percentile(__aggregate1, Constant(0.5))",
				"stats::approx_percentile(__aggregate2, Constant(0.5))",
				"stats::approx_percentile(__aggregate3, Constant(0.9))",
				"stats::approx_percentile(__aggregate4, Constant(0.5))",
				"__aggregate5",
			]
		);
	}

	#[test]
	fn the_flow_rewrite_reports_the_first_bad_call_of_an_output_in_source_order() {
		// Reporting a later call first sends the user to fix an argument that is not the first one broken.
		let bad_digest = || digest(vec![column("latency"), number("0.5")]);
		let bad_percentile = || percentile(vec![column("latency"), number("1.5"), number("0.01")]);
		let add = |left: Expression, right: Expression| {
			Expression::Add(AddExpression {
				left: Box::new(left),
				right: Box::new(right),
				fragment: Fragment::internal("+"),
			})
		};

		assert!(matches!(
			flow_rewrite(vec![add(bad_digest(), bad_percentile())]),
			Err(AggregateCallError::DigestAccuracy(DigestError::AccuracyOutOfRange))
		));
		assert!(matches!(
			flow_rewrite(vec![add(bad_percentile(), bad_digest())]),
			Err(AggregateCallError::Percentile(PercentileCallError::PercentileOutOfRange { .. }))
		));
		assert!(matches!(
			flow_rewrite(vec![percentile(vec![column("latency"), number("0.5"), number("0.0000005")])]),
			Err(AggregateCallError::Percentile(PercentileCallError::Accuracy {
				error: DigestError::AccuracyNotWholePpm,
				..
			}))
		));
	}
}
