// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::window::coord::WindowDomain;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Polarity {
	Insert,
	Remove,
}

impl Polarity {
	pub fn is_insert(self) -> bool {
		matches!(self, Polarity::Insert)
	}

	pub fn inverted(self) -> Self {
		match self {
			Polarity::Insert => Polarity::Remove,
			Polarity::Remove => Polarity::Insert,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowEvent<D: WindowDomain, A> {
	pub coord: D::Coord,
	pub polarity: Polarity,
	pub contribution: A,
}

impl<D: WindowDomain, A> WindowEvent<D, A> {
	pub fn insert(coord: D::Coord, contribution: A) -> Self {
		Self {
			coord,
			polarity: Polarity::Insert,
			contribution,
		}
	}

	pub fn remove(coord: D::Coord, contribution: A) -> Self {
		Self {
			coord,
			polarity: Polarity::Remove,
			contribution,
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::datetime::DateTime;

	use super::*;
	use crate::window::coord::{EventTime, Ordinal};

	#[test]
	fn an_update_decomposes_into_an_inverted_pair() {
		// An Update is a Remove of the pre image plus an Insert of the post, and
		// the accumulators are invertible on exactly that basis. Five guest drivers got
		// this wrong today by carrying no real `pre`, so the Remove half was skipped and
		// the accumulator drifted by one row per update, forever.
		assert_eq!(Polarity::Insert.inverted(), Polarity::Remove);
		assert_eq!(Polarity::Remove.inverted(), Polarity::Insert);
		assert_eq!(Polarity::Insert.inverted().inverted(), Polarity::Insert);
	}

	#[test]
	fn an_event_carries_the_coordinate_of_its_own_domain() {
		// The domain decides the coordinate TYPE, so a count window cannot be fed
		// an instant and a time window cannot be fed a row index. Before WindowDomain the
		// coordinate was a bare u64 order on both paths, and the only thing keeping them
		// apart was which driver you happened to call.
		let timed: WindowEvent<EventTime, u8> = WindowEvent::insert(DateTime::from_millis(5_000), 1);
		let counted: WindowEvent<Ordinal, u8> = WindowEvent::remove(42, 1);

		assert_eq!(timed.coord, DateTime::from_millis(5_000));
		assert!(timed.polarity.is_insert());
		assert_eq!(counted.coord, 42);
		assert!(!counted.polarity.is_insert());
	}
}
