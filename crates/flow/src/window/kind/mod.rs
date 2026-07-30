// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod rolling;
pub mod session;
pub mod sliding;
pub mod tumbling;

use reifydb_value::value::datetime::DateTime;

use crate::window::span::{WindowCoord, WindowSpan};

pub fn ordinal_window_span(window_id: u64) -> WindowSpan<DateTime> {
	WindowSpan::new(
		<DateTime as WindowCoord>::from_order(window_id),
		<DateTime as WindowCoord>::from_order(window_id + 1),
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn an_ordinal_span_is_exactly_one_unit_wide_and_never_empty() {
		// Count windows and sessions are both identified by an ordinal, but the engine
		// keys everything by WindowSpan<DateTime>, so the ordinal has to be carried inside a span
		// that is only ever compared, never measured. A zero-width span (start == end) collapses
		// under the engine's half-open [start, end) comparisons and two adjacent ordinals would
		// alias onto one another.
		for window_id in [0u64, 1, 41, 1_000_000_000_000] {
			let span = ordinal_window_span(window_id);
			assert!(span.start < span.end, "ordinal {window_id} produced an empty span");
			assert_ne!(span, ordinal_window_span(window_id + 1));
		}
	}
}
