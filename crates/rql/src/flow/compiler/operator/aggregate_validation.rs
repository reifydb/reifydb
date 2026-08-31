// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::{WindowKind, WindowSize},
	error::diagnostic::flow::{flow_unsupported_aggregate_expression, flow_window_span_unavailable},
};
use reifydb_routine_abi::registry::Routines;
use reifydb_value::{Result, error::Error};

use crate::{
	expression::{Expression, name::display_label},
	flow::aggregate::{AggregateContext, collect_slots},
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
	for expr in aggregations {
		let output = display_label(expr).text().to_string();
		let Some(slots) = collect_slots(routines, expr, context) else {
			return Err(Error(Box::new(flow_unsupported_aggregate_expression(&output))));
		};
		let needs_span = slots.iter().any(|(kind, _)| kind.requires_span());
		let needs_event_time = slots.iter().any(|(kind, _)| kind.requires_event_time());
		if (needs_span && !bounded_span(window)) || (needs_event_time && !time_based(window)) {
			return Err(Error(Box::new(flow_window_span_unavailable(
				&output,
				window.map(span_label).unwrap_or("grouped"),
			))));
		}
	}
	Ok(())
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
	!matches!(
		window,
		Some(WindowKind::Rolling {
			..
		})
	) && time_based(window)
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
