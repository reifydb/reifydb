// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::Debug;

use reifydb_value::value::{date::Date, datetime::DateTime, duration::Duration, time::Time};

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

	fn span_millis(span: Self::Span) -> Option<u64>;
}

impl Coord for DateTime {
	type Span = Duration;

	const MAX: Self = DateTime::MAX;

	fn saturating_sub_span(self, span: Duration) -> Self {
		self.saturating_sub(span)
	}

	fn checked_sub_span(self, span: Duration) -> Option<Self> {
		self.checked_sub(span)
	}

	fn add_span(self, span: Duration) -> Self {
		self + span
	}

	fn floor_to(self, span: Duration) -> Self {
		self - (self % span)
	}

	fn span_since(self, earlier: Self) -> Duration {
		self - earlier
	}

	fn to_order(self) -> u64 {
		self.to_epoch_millis() as u64
	}

	fn from_order(order: u64) -> Self {
		DateTime::from_epoch_millis(order).unwrap_or(DateTime::MAX)
	}

	fn span_millis(span: Duration) -> Option<u64> {
		span.milliseconds().ok().and_then(|ms| u64::try_from(ms).ok())
	}
}

pub trait IsZero {
	fn is_zero(&self) -> bool;
}

impl IsZero for u64 {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == 0
	}
}

impl IsZero for Duration {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == Duration::zero()
	}
}

impl IsZero for DateTime {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == DateTime::default()
	}
}

impl IsZero for Date {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == Date::default()
	}
}

impl IsZero for Time {
	#[inline]
	fn is_zero(&self) -> bool {
		*self == Time::default()
	}
}
