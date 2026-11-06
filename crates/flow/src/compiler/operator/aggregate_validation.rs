// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{OnEmpty, WindowKind},
	error::diagnostic::flow::{
		flow_digest_accuracy_not_a_literal, flow_digest_accuracy_not_whole_ppm,
		flow_digest_accuracy_out_of_range, flow_percentile_argument_count, flow_percentile_not_a_literal,
		flow_percentile_out_of_range, flow_rolling_zero_column_not_option,
		flow_unsupported_aggregate_expression, flow_window_span_unavailable,
	},
	expression::{Expression, name::display_label},
	interface::catalog::view::View,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_value::{
	Result,
	error::{Diagnostic, Error},
	value::digest::DigestError,
};

use crate::aggregate::{
	AggregateCallError, AggregateContext, DIGEST_FUNCTION, DigestSlots, PERCENTILE_FUNCTION, PercentileCallError,
	SlotKind, rewrite_aggregates,
};

pub(crate) fn validate_flow_aggregations(
	routines: &Routines,
	aggregations: &[Expression],
	context: AggregateContext,
	window: Option<&WindowKind>,
	sink: Option<&View>,
) -> Result<()> {
	if aggregations.is_empty() {
		return Err(Error(Box::new(flow_unsupported_aggregate_expression("<none>"))));
	}
	let mut slots = Vec::new();
	let mut digests = DigestSlots::default();
	for expr in aggregations {
		let output = display_label(expr).text().to_string();
		let first = slots.len();
		let representable = rewrite_aggregates(routines, &mut expr.clone(), &mut slots, &mut digests, context)
			.map_err(|error| Error(Box::new(aggregate_call_diagnostic(&output, error))))?;
		if !representable {
			return Err(Error(Box::new(flow_unsupported_aggregate_expression(&output))));
		}
		let added = &slots[first..];
		let needs_span = added.iter().any(|(kind, _)| kind.requires_span());
		let needs_event_time = added.iter().any(|(kind, _)| kind.requires_event_time());
		if (needs_span && !bounded_span(window)) || (needs_event_time && !time_based(window)) {
			return Err(Error(Box::new(flow_window_span_unavailable(
				&output,
				window.map(WindowKind::name).unwrap_or("grouped"),
			))));
		}
		if zeroes_on_empty(window)
			&& let Some(kind) = added.iter().map(|(kind, _)| *kind).find(|kind| !has_zero(*kind))
			&& let Some(column) =
				sink.and_then(|sink| sink.columns().iter().find(|column| column.name == output))
			&& !column.constraint.get_type().is_option()
		{
			return Err(Error(Box::new(flow_rolling_zero_column_not_option(&output, slot_name(kind)))));
		}
	}
	Ok(())
}

fn zeroes_on_empty(window: Option<&WindowKind>) -> bool {
	matches!(
		window,
		Some(WindowKind::Rolling {
			on_empty: OnEmpty::Zero,
			..
		})
	)
}

fn has_zero(kind: SlotKind) -> bool {
	matches!(kind, SlotKind::Count { .. } | SlotKind::Sum | SlotKind::WindowLast)
}

fn slot_name(kind: SlotKind) -> &'static str {
	match kind {
		SlotKind::Count {
			..
		} => "count",
		SlotKind::Sum => "sum",
		SlotKind::Avg => "avg",
		SlotKind::Min => "min",
		SlotKind::Max => "max",
		SlotKind::First => "first",
		SlotKind::Last => "last",
		SlotKind::WindowStart => "window::start",
		SlotKind::WindowEnd => "window::end",
		SlotKind::WindowDuration => "window::duration",
		SlotKind::WindowLast => "window::last",
		SlotKind::Digest {
			..
		} => "digest",
	}
}

pub fn aggregate_call_diagnostic(output: &str, error: AggregateCallError) -> Diagnostic {
	match error {
		AggregateCallError::DigestAccuracy(error) => accuracy_diagnostic(output, DIGEST_FUNCTION, error),
		AggregateCallError::Percentile(PercentileCallError::ArgumentCount {
			function,
			actual,
		}) => flow_percentile_argument_count(output, function.text(), actual),
		AggregateCallError::Percentile(PercentileCallError::NotLiteral {
			position: 2,
			..
		})
		| AggregateCallError::Percentile(PercentileCallError::PercentileNotANumber {
			..
		}) => flow_percentile_not_a_literal(output, PERCENTILE_FUNCTION),
		AggregateCallError::Percentile(PercentileCallError::PercentileOutOfRange {
			..
		}) => flow_percentile_out_of_range(output, PERCENTILE_FUNCTION),
		AggregateCallError::Percentile(PercentileCallError::NotLiteral {
			position: 3,
			..
		}) => flow_digest_accuracy_not_a_literal(output, PERCENTILE_FUNCTION),
		AggregateCallError::Percentile(PercentileCallError::NotLiteral {
			position,
			..
		}) => unreachable!("a percentile call has no literal argument at position {position}"),
		AggregateCallError::Percentile(PercentileCallError::Accuracy {
			error,
			..
		}) => accuracy_diagnostic(output, PERCENTILE_FUNCTION, error),
	}
}

fn accuracy_diagnostic(output: &str, function: &str, error: DigestError) -> Diagnostic {
	match error {
		DigestError::AccuracyOutOfRange => flow_digest_accuracy_out_of_range(output, function),
		DigestError::AccuracyNotWholePpm => flow_digest_accuracy_not_whole_ppm(output, function),
		DigestError::AccuracyNotANumber => flow_digest_accuracy_not_a_literal(output, function),
		other => unreachable!("the accuracy literal parser reported {other:?}, which is not an accuracy error"),
	}
}

fn time_based(window: Option<&WindowKind>) -> bool {
	window.is_some()
}

fn bounded_span(window: Option<&WindowKind>) -> bool {
	!matches!(window, Some(WindowKind::Rolling { .. })) && time_based(window)
}
