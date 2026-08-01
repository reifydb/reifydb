// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn an_update_decomposes_into_an_inverted_pair() {
		// An Update is a Remove of the pre plus an Insert of the post, which is the basis for the
		// accumulators being invertible. A driver that skips the Remove half drifts by one row per
		// update, forever.
		assert_eq!(Polarity::Insert.inverted(), Polarity::Remove);
		assert_eq!(Polarity::Remove.inverted(), Polarity::Insert);
		assert_eq!(Polarity::Insert.inverted().inverted(), Polarity::Insert);
	}
}
