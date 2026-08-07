// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::factory::time::at_millis;

use crate::window::coord::EventCoord;

pub fn event_coord_at_millis(value: u64) -> EventCoord {
	EventCoord::of(&at_millis(value))
}

#[cfg(test)]
mod tests {
	use reifydb_value::factory::time::at_nanos;

	use super::*;

	#[test]
	fn the_coordinate_is_the_instant_its_name_promises() {
		// `EventCoord` hides the instant behind an opaque wrapper, so a caller cannot see which unit
		// the integer was read as. Rewriting the body to `at_nanos` would leave every windowing test
		// that builds fixtures through this helper green while moving every coordinate it produces by
		// a factor of a million, so pin it against the value-crate factory of the same name.
		assert_eq!(event_coord_at_millis(1_000).at(), at_millis(1_000));
		assert_ne!(event_coord_at_millis(1_000).at(), at_nanos(1_000));
	}

	#[test]
	fn the_coordinate_is_the_instant_itself_and_not_a_floored_one() {
		// Sliding and session assignment compare a coordinate against a span boundary, so a helper
		// that quietly floored its argument would make every off-boundary fixture land on a boundary
		// and hide exactly the assignment errors those tests exist to catch.
		assert_eq!(event_coord_at_millis(1_337).at(), at_millis(1_337));
		assert!(event_coord_at_millis(999) < event_coord_at_millis(1_000));
	}
}
