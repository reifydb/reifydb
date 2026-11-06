// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::factory::time::at_millis;

use crate::window::coord::DateTimeCoord;

pub fn event_coord_at_millis(value: i64) -> DateTimeCoord {
	DateTimeCoord::of(&at_millis(value))
}

#[cfg(test)]
mod tests {
	use reifydb_value::factory::time::at_nanos;

	use super::*;

	#[test]
	fn the_coordinate_is_the_instant_its_name_promises() {
		// Rewriting the body to `at_nanos` would move every fixture coordinate by a factor of a million while
		// tests stay green.
		assert_eq!(event_coord_at_millis(1_000).0, at_millis(1_000));
		assert_ne!(event_coord_at_millis(1_000).0, at_nanos(1_000));
	}

	#[test]
	fn the_coordinate_is_the_instant_itself_and_not_a_floored_one() {
		// Sliding and session assignment compare a coordinate against a span boundary, so a helper
		// that quietly floored its argument would make every off-boundary fixture land on a boundary
		// and hide exactly the assignment errors those tests exist to catch.
		assert_eq!(event_coord_at_millis(1_337).0, at_millis(1_337));
		assert!(event_coord_at_millis(999) < event_coord_at_millis(1_000));
	}
}
