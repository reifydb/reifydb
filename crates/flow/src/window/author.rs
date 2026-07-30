// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;

use crate::window::{coord::SlotTie, span::Stamped};

pub trait WindowAuthor {
	type Contribution;
	type Tie: SlotTie;

	fn tie(contribution: &Self::Contribution) -> Self::Tie;
}

pub type AuthorSlot<A> = Stamped<DateTime, <A as WindowAuthor>::Tie>;

#[cfg(test)]
mod tests {
	use super::*;
	use crate::window::coord::NoTie;

	struct Untied;

	impl WindowAuthor for Untied {
		type Contribution = u64;
		type Tie = NoTie;

		fn tie(_contribution: &Self::Contribution) -> Self::Tie {
			NoTie
		}
	}

	struct SlotTied;

	impl WindowAuthor for SlotTied {
		type Contribution = (u64, u64);
		type Tie = u64;

		fn tie(contribution: &Self::Contribution) -> Self::Tie {
			contribution.1
		}
	}

	#[test]
	fn a_tie_is_derived_from_the_contribution_and_never_from_a_coordinate() {
		// The 13 chaindex operators keyed by Stamped<DateTime, u64> need a Solana
		// slot in the slot key, which the shell cannot know. `tie` receives the
		// contribution ONLY - not the row, not a coordinate - so an author can decorate
		// the slot but has no way to replace the temporal coordinate the shell computed.
		// Widening tie() to take the coordinate would let an author key by anything.
		assert_eq!(SlotTied::tie(&(5_000, 42)), 42);
		assert_eq!(Untied::tie(&7), NoTie);
	}

	#[test]
	fn an_untied_author_slot_orders_purely_by_instant() {
		// A NoTie author must behave exactly like a bare instant, or moving an existing
		// single-coordinate operator onto the shell would silently reorder its slots.
		let early: AuthorSlot<Untied> = Stamped::new(DateTime::from_millis(1), NoTie);
		let late: AuthorSlot<Untied> = Stamped::new(DateTime::from_millis(2), NoTie);

		assert!(early < late);
	}

	#[test]
	fn a_tie_breaks_only_within_one_instant() {
		// The tie must be strictly subordinate to the coordinate. If it were not,
		// a high slot number on an early timestamp would sort after a low slot number on a
		// late one, and the window would bucket by Solana slot instead of by time.
		let early_high: AuthorSlot<SlotTied> = Stamped::new(DateTime::from_millis(1), 999);
		let late_low: AuthorSlot<SlotTied> = Stamped::new(DateTime::from_millis(2), 0);
		let same_instant_low: AuthorSlot<SlotTied> = Stamped::new(DateTime::from_millis(1), 1);

		assert!(early_high < late_low);
		assert!(same_instant_low < early_high);
	}
}
