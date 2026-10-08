// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f64::consts::TAU;

pub struct EquityConfig {
	pub days: usize,
	pub bars_per_day: usize,
	pub bar_ns: i64,
	pub tick: i64,
	pub start_price: i64,
	pub base_volume: f64,
	pub seed: u64,
}

impl Default for EquityConfig {
	fn default() -> Self {
		Self {
			days: 252,
			bars_per_day: 390,
			bar_ns: 60_000_000_000,
			tick: 25,
			start_price: 500_000,
			base_volume: 900.0,
			seed: 42,
		}
	}
}

#[derive(Default)]
pub struct Bars {
	pub ts: Vec<i64>,
	pub open: Vec<i64>,
	pub high: Vec<i64>,
	pub low: Vec<i64>,
	pub close: Vec<i64>,
	pub volume: Vec<u64>,
}

pub struct Rng(pub u64);

impl Rng {
	pub fn next_u64(&mut self) -> u64 {
		self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
		let mut z = self.0;
		z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
		z ^ (z >> 31)
	}

	pub fn uniform(&mut self) -> f64 {
		(self.next_u64() >> 11) as f64 * (1.0 / (1u64 << 53) as f64)
	}

	pub fn normal(&mut self) -> f64 {
		let u1 = 1.0 - self.uniform();
		let u2 = self.uniform();
		(-2.0 * u1.ln()).sqrt() * (TAU * u2).cos()
	}

	fn step(&mut self) -> i64 {
		match self.uniform() {
			u if u < 0.46 => 0,
			u if u < 0.71 => 1,
			u if u < 0.96 => -1,
			u if u < 0.98 => 2,
			_ => -2,
		}
	}
}

pub fn generate(cfg: &EquityConfig) -> Bars {
	const SESSION_OPEN_S: i64 = 1_735_828_200;
	const DAY_NS: i64 = 86_400_000_000_000;

	let mut rng = Rng(cfg.seed);
	let mut out = Bars::default();
	let mut price = cfg.start_price;
	let mut cal_day: i64 = 0;

	for d in 0..cfg.days {
		while (cal_day + 3) % 7 >= 5 {
			cal_day += 1;
		}
		let session_start = SESSION_OPEN_S * 1_000_000_000 + cal_day * DAY_NS;
		if d > 0 {
			price += (rng.normal() * 6.0).round() as i64 * cfg.tick;
		}
		for i in 0..cfg.bars_per_day {
			let open = price;
			let (mut hi, mut lo) = (open, open);
			for _ in 0..30 {
				price += rng.step() * cfg.tick;
				hi = hi.max(price);
				lo = lo.min(price);
			}
			let x = i as f64 / (cfg.bars_per_day.max(2) - 1) as f64;
			let u_shape = 1.0 + 3.0 * (2.0 * x - 1.0).powi(2);
			let range_ticks = ((hi - lo) / cfg.tick) as f64;
			let vol = cfg.base_volume * u_shape * (1.0 + 0.08 * range_ticks) * (0.6 * rng.normal()).exp();
			out.ts.push(session_start + i as i64 * cfg.bar_ns);
			out.open.push(open);
			out.high.push(hi);
			out.low.push(lo);
			out.close.push(price);
			out.volume.push(vol.round().max(1.0) as u64);
		}
		cal_day += 1;
	}
	out
}

#[cfg(test)]
mod tests {
	use super::{EquityConfig, generate};

	#[test]
	fn the_port_matches_the_reference_generator() {
		// Phase 1 numbers are only comparable with the reference harness if both generate the same bars.
		let bars = generate(&EquityConfig::default());
		assert_eq!(bars.ts.len(), 98_280);
		assert_eq!(bars.ts[0], 1_735_828_200_000_000_000);
		assert_eq!(
			(bars.open[0], bars.high[0], bars.low[0], bars.close[0]),
			(500_000, 500_125, 499_925, 500_075)
		);
		let checksum: i128 = bars.close.iter().map(|&v| v as i128).sum::<i128>()
			+ bars.volume.iter().map(|&v| v as i128).sum::<i128>()
			+ bars.high.iter().map(|&v| v as i128).sum::<i128>();
		assert_eq!(checksum, 100_646_503_893);
	}

	#[test]
	fn every_bar_is_consistent_ohlc() {
		// The wick transform stores high - max(open, close) as unsigned; an inconsistent bar would overflow it.
		let bars = generate(&EquityConfig::default());
		for i in 0..bars.ts.len() {
			assert!(bars.low[i] <= bars.open[i].min(bars.close[i]), "bar {i}");
			assert!(bars.high[i] >= bars.open[i].max(bars.close[i]), "bar {i}");
		}
	}
}
