// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_codec::key::encoded::EncodedKeyBuilder;
use reifydb_value::value::{datetime::DateTime, duration::Duration};

use crate::window::coord::DateTimeCoord;

pub trait Coord: Copy + Ord + Debug {
	type Span: Copy + Ord + Debug + IsZero + Default + Send + Sync;

	const MAX: Self;

	fn saturating_sub_span(self, span: Self::Span) -> Self;

	fn checked_sub_span(self, span: Self::Span) -> Option<Self>;

	fn add_span(self, span: Self::Span) -> Self;

	fn floor_to(self, span: Self::Span) -> Self;

	fn span_since(self, earlier: Self) -> Self::Span;

	fn to_order(self) -> u64;

	fn from_order(order: u64) -> Self;

	fn extend_key(self, builder: EncodedKeyBuilder) -> EncodedKeyBuilder;
}

impl Coord for DateTimeCoord {
	type Span = Duration;

	const MAX: Self = Self(DateTime::MAX);

	fn saturating_sub_span(self, span: Duration) -> Self {
		Self(self.0.saturating_sub(span))
	}

	fn checked_sub_span(self, span: Duration) -> Option<Self> {
		self.0.checked_sub(span).map(Self)
	}

	fn add_span(self, span: Duration) -> Self {
		Self(self.0 + span)
	}

	fn floor_to(self, span: Duration) -> Self {
		Self(self.0 - (self.0 % span))
	}

	fn span_since(self, earlier: Self) -> Duration {
		self.0 - earlier.0
	}

	fn to_order(self) -> u64 {
		DateTime::to_order(&self.0)
	}

	fn from_order(order: u64) -> Self {
		Self(DateTime::from_order(order))
	}

	fn extend_key(self, builder: EncodedKeyBuilder) -> EncodedKeyBuilder {
		builder.datetime(&self.0)
	}
}

pub trait IsZero {
	fn is_zero(&self) -> bool;
}

impl IsZero for Duration {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == Duration::zero()
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_value::value::{datetime::DateTime, duration::Duration};

	use super::Coord;
	use crate::window::coord::DateTimeCoord;

	#[test]
	fn floor_to_rounds_a_pre_epoch_instant_down_not_toward_zero() {
		// A truncating remainder would floor -1ns to 0 instead of the second below it.
		let span = Duration::from_seconds(1).unwrap();
		assert_eq!(
			DateTimeCoord(DateTime::from_nanos(-1)).floor_to(span),
			DateTimeCoord(DateTime::from_nanos(-1_000_000_000))
		);
	}

	#[test]
	fn datetime_extend_key_matches_the_datetime_key_encoding() {
		// The engine key must stay byte-identical to the hand-written datetime encoding, or every stored window
		// is orphaned.
		let dt = DateTime::from_ymd_hms(2024, 1, 15, 10, 30, 25).unwrap();

		let bare = DateTimeCoord(dt).extend_key(EncodedKey::builder()).build();
		assert_eq!(bare, EncodedKey::builder().datetime(&dt).build());

		let prefixed = DateTimeCoord(dt).extend_key(EncodedKey::builder().u32(7u32)).build();
		assert_eq!(prefixed, EncodedKey::builder().u32(7u32).datetime(&dt).build());
		assert_eq!(prefixed.len(), bare.len() + 4, "the prefix must not be re-encoded or dropped");
	}
}
