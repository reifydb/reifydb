// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::WindowKind;

use crate::operator::state::seal::coord::Coord;

pub struct WindowSettings<C: Coord> {
	pub kind: WindowKind,
	pub size: C::Span,
	pub pane: Option<C::Span>,
	pub slide: Option<C::Span>,
	pub lateness: C::Span,
	pub immutable: Option<C::Span>,
}

#[cfg(test)]
mod tests {
	use reifydb_core::common::{WindowKind, WindowSize};
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::WindowSettings;

	fn settings(pane: Option<Duration>) -> WindowSettings<DateTime> {
		let hour = Duration::from_hours(1).unwrap();
		WindowSettings {
			kind: WindowKind::Rolling {
				size: WindowSize::Duration(hour),
				lag: None,
				pane,
			},
			size: hour,
			pane,
			slide: None,
			lateness: Duration::from_seconds(30).unwrap(),
			immutable: None,
		}
	}

	#[test]
	fn settings_carry_the_pane_as_none_and_as_some() {
		// A pane collapsed to a default would make a paneless window fold at a resolution nobody declared.
		assert_eq!(settings(None).pane, None);
		let one_second = Duration::from_seconds(1).unwrap();
		assert_eq!(settings(Some(one_second)).pane, Some(one_second));
	}
}
