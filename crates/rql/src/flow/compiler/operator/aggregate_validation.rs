// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{WindowKind, WindowSize},
	error::diagnostic::flow::{
		flow_digest_accuracy_not_a_literal, flow_digest_accuracy_not_whole_ppm,
		flow_digest_accuracy_out_of_range, flow_percentile_argument_count, flow_percentile_not_a_literal,
		flow_percentile_out_of_range, flow_unsupported_aggregate_expression, flow_window_span_unavailable,
	},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_value::{
	Result,
	error::{Diagnostic, Error},
	value::digest::DigestError,
};

use crate::{
	expression::{Expression, name::display_label},
	flow::aggregate::{
		AggregateCallError, AggregateContext, DIGEST_FUNCTION, DigestSlots, PERCENTILE_FUNCTION,
		PercentileCallError, rewrite_aggregates,
	},
};

pub(crate) fn validate_flow_aggregations(
	routines: &Routines,
	aggregations: &[Expression],
	context: AggregateContext,
	window: Option<&WindowKind>,
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
				window.map(span_label).unwrap_or("grouped"),
			))));
		}
	}
	Ok(())
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
	match window {
		Some(WindowKind::Tumbling {
			size,
		}) => matches!(size, WindowSize::Duration(_)),
		Some(WindowKind::Sliding {
			size,
			..
		}) => matches!(size, WindowSize::Duration(_)),
		Some(WindowKind::Rolling {
			size,
			..
		}) => matches!(size, WindowSize::Duration(_)),
		Some(WindowKind::Session {
			..
		}) => true,
		None => false,
	}
}

fn bounded_span(window: Option<&WindowKind>) -> bool {
	!matches!(window, Some(WindowKind::Rolling { .. })) && time_based(window)
}

fn span_label(window: &WindowKind) -> &'static str {
	match window {
		WindowKind::Tumbling {
			..
		} => "row-counted tumbling",
		WindowKind::Sliding {
			..
		} => "row-counted sliding",
		WindowKind::Rolling {
			size: WindowSize::Count(_),
			..
		} => "row-counted rolling",
		WindowKind::Session {
			..
		} => "session",
		WindowKind::Rolling {
			..
		} => "rolling",
	}
}
