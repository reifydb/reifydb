// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Range;

#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct Derived {
	pub close: Vec<i64>,
	pub gap: Vec<i64>,
	pub upper: Vec<u64>,
	pub lower: Vec<u64>,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct Ohlc {
	pub open: Vec<i64>,
	pub high: Vec<i64>,
	pub low: Vec<i64>,
	pub close: Vec<i64>,
}

fn overflow(what: &str, row: usize) -> String {
	format!("{what} overflows at row {row}")
}

pub fn forward(bars: &Ohlc, segments: &[Range<usize>]) -> Result<Derived, String> {
	let mut out = Derived::default();
	for segment in segments {
		let Some(first) = segment.clone().next() else {
			continue;
		};
		let mut prev_close = bars.close[first];
		for i in segment.clone() {
			let (open, high, low, close) = (bars.open[i], bars.high[i], bars.low[i], bars.close[i]);
			let gap = open.checked_sub(prev_close).ok_or_else(|| overflow("gap", i))?;
			let upper = high.checked_sub(open.max(close)).ok_or_else(|| overflow("upper", i))?;
			let lower = open.min(close).checked_sub(low).ok_or_else(|| overflow("lower", i))?;
			out.close.push(close);
			out.gap.push(gap);
			out.upper.push(u64::try_from(upper).map_err(|_| format!("high below body at row {i}"))?);
			out.lower.push(u64::try_from(lower).map_err(|_| format!("low above body at row {i}"))?);
			prev_close = close;
		}
	}
	Ok(out)
}

pub fn inverse(derived: &Derived, segments: &[Range<usize>]) -> Result<Ohlc, String> {
	let mut out = Ohlc::default();
	for segment in segments {
		let Some(first) = segment.clone().next() else {
			continue;
		};
		let mut prev_close = derived.close[first];
		for i in segment.clone() {
			let close = derived.close[i];
			let open = prev_close.checked_add(derived.gap[i]).ok_or_else(|| overflow("open", i))?;
			let upper = i64::try_from(derived.upper[i]).map_err(|_| overflow("upper", i))?;
			let lower = i64::try_from(derived.lower[i]).map_err(|_| overflow("lower", i))?;
			out.open.push(open);
			out.high.push(open.max(close).checked_add(upper).ok_or_else(|| overflow("high", i))?);
			out.low.push(open.min(close).checked_sub(lower).ok_or_else(|| overflow("low", i))?);
			out.close.push(close);
			prev_close = close;
		}
	}
	Ok(out)
}

#[cfg(test)]
mod tests {
	use std::ops::Range;

	use super::{Ohlc, forward, inverse};
	use crate::equity::{EquityConfig, generate};

	#[test]
	fn the_transform_round_trips_the_equity_bars() {
		// Storing the derived columns is only valid if the original bars come back exactly, highs and lows
		// included.
		let bars = generate(&EquityConfig::default());
		let ohlc = Ohlc {
			open: bars.open,
			high: bars.high,
			low: bars.low,
			close: bars.close,
		};
		let segments = vec![0..100, 100..390, 390..ohlc.close.len()];
		let derived = forward(&ohlc, &segments).unwrap();
		assert_eq!(inverse(&derived, &segments).unwrap(), ohlc);
	}

	#[test]
	fn the_first_gap_of_each_segment_is_against_its_own_close() {
		// A segment must decode without its predecessor, or a block could not be read on its own.
		let ohlc = Ohlc {
			open: vec![10, 20, 30],
			high: vec![15, 25, 35],
			low: vec![5, 15, 25],
			close: vec![12, 22, 32],
		};
		let derived = forward(&ohlc, &[0..1, 1..3]).unwrap();
		assert_eq!(derived.gap, vec![-2, -2, 8]);
		assert_eq!(derived.upper, vec![3, 3, 3]);
		assert_eq!(derived.lower, vec![5, 5, 5]);
		assert_eq!(
			inverse(
				&derived,
				&[Range {
					start: 1,
					end: 3
				}]
			)
			.unwrap()
			.open,
			vec![20, 30]
		);
	}

	#[test]
	fn an_inconsistent_bar_is_rejected() {
		// A high below the body would wrap to a huge unsigned wick and decode to a wrong bar silently.
		let ohlc = Ohlc {
			open: vec![10],
			high: vec![9],
			low: vec![5],
			close: vec![8],
		};
		assert!(forward(
			&ohlc,
			&[Range {
				start: 0,
				end: 1
			}]
		)
		.is_err());
	}

	#[test]
	fn extreme_values_fail_loudly_instead_of_wrapping() {
		// Wrapping arithmetic would turn an overflow into a plausible but wrong price.
		let ohlc = Ohlc {
			open: vec![i64::MAX, i64::MIN],
			high: vec![i64::MAX, i64::MIN],
			low: vec![i64::MAX, i64::MIN],
			close: vec![i64::MAX, i64::MIN],
		};
		assert!(forward(
			&ohlc,
			&[Range {
				start: 0,
				end: 2
			}]
		)
		.is_err());
	}
}
