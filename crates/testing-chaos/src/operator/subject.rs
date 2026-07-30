// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::change::Change;
use reifydb_value::Result;

/// The operator under test, reduced to the two things a chaos driver needs from it.
///
/// Both sides of the ABI satisfy this: a host operator returns its `Change` from `on_timer` directly,
/// and a guest operator pushes rows into sinks that the host wrapper drains into a `Change` at the
/// same two boundaries. Implementing it once per side is what lets one driver test both.
///
/// `tick` takes a plain instant rather than a timer, because the driver only ever fires a seal at a
/// coordinate; the timer kind and key are transport detail each side fills in for itself. That also
/// keeps this crate free of any timer type, so it depends on neither the flow crate nor the sdk.
pub trait Subject {
	fn apply(&mut self, change: Change) -> Result<Change>;

	/// Advances to `at_ms` and returns whatever the operator emitted, or `None` when it emitted
	/// nothing.
	///
	/// Returning the emission is load-bearing, not a convenience: the driver's drain loop decides it
	/// has reached quiescence by comparing the view before and after a tick, and its final assertion
	/// compares the drained view against the model. An implementation that performed the tick but
	/// dropped the emission would make both of those unfalsifiable.
	fn tick(&mut self, at_ms: u64) -> Result<Option<Change>>;
}
