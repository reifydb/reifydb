// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeSet, f64::consts::LN_10};

use crate::{
	equity::Rng,
	solana::{Bars, Pool, log_uniform, poisson, student_t3},
};

pub struct SecondConfig {
	pub frequent: u32,
	pub infrequent: u32,
	pub sample_frequent: u32,
	pub sample_infrequent: u32,
	pub days: u32,
	pub start: u64,
	pub skew: f64,
	pub extra_trades: f64,
	pub infrequent_seconds: (u32, u32),
	pub seed: u64,
}

impl Default for SecondConfig {
	fn default() -> Self {
		Self {
			frequent: 10_000,
			infrequent: 100_000,
			sample_frequent: 500,
			sample_infrequent: 5_000,
			days: 7,
			start: 1_778_112_000,
			skew: 0.27,
			extra_trades: 2.0,
			infrequent_seconds: (20, 100),
			seed: 11,
		}
	}
}

pub const DAY_SECONDS: u64 = 86_400;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Class {
	Frequent,
	Infrequent,
}

#[derive(Clone, Copy, Debug)]
pub struct Sample {
	pub class: Class,
	pub rank: u32,
	pub weight: f64,
}

const MAX_PRICE: f64 = 1e17;
const JUMP_PER_SECOND: f64 = 1.0 / 3_000_000.0;
const GAP_VOLATILITY: f64 = 0.3;

pub fn activity(cfg: &SecondConfig, rank: u32) -> f64 {
	(rank as f64).powf(-cfg.skew)
}

fn stratum_bounds(population: u32, strata: u32) -> Vec<u32> {
	let strata = strata.min(population);
	let mut bounds = vec![1u32];
	for k in 1..=strata {
		let geometric = ((population + 1) as f64).powf(k as f64 / strata as f64).round() as u32;
		let previous = *bounds.last().expect("bounds start with one entry");
		let remaining = strata - k;
		bounds.push(geometric.max(previous + 1).min(population + 1 - remaining));
	}
	bounds
}

pub fn samples(cfg: &SecondConfig) -> Vec<Sample> {
	let mut rng = Rng(cfg.seed ^ 0x9E37_79B9_7F4A_7C15);
	let bounds = stratum_bounds(cfg.frequent, cfg.sample_frequent);
	let mut out: Vec<Sample> = bounds
		.windows(2)
		.map(|pair| {
			let size = pair[1] - pair[0];
			Sample {
				class: Class::Frequent,
				rank: pair[0] + ((rng.uniform() * size as f64) as u32).min(size - 1),
				weight: size as f64,
			}
		})
		.collect();
	let picked = cfg.sample_infrequent.min(cfg.infrequent);
	let weight = cfg.infrequent as f64 / picked.max(1) as f64;
	out.extend((1..=picked).map(|rank| Sample {
		class: Class::Infrequent,
		rank,
		weight,
	}));
	out
}

fn active_seconds(cfg: &SecondConfig, sample: &Sample, rng: &mut Rng) -> Vec<u64> {
	let total = cfg.days as u64 * DAY_SECONDS;
	match sample.class {
		Class::Frequent => {
			let p = activity(cfg, sample.rank);
			(0..total).filter(|_| rng.uniform() < p).map(|s| cfg.start + s).collect()
		}
		Class::Infrequent => {
			let (lo, hi) = cfg.infrequent_seconds;
			let mut out = Vec::new();
			for day in 0..cfg.days as u64 {
				let count = lo as u64 + (rng.uniform() * (hi - lo + 1) as f64) as u64;
				let mut picked = BTreeSet::new();
				while (picked.len() as u64) < count.min(DAY_SECONDS) {
					picked.insert(
						((rng.uniform() * DAY_SECONDS as f64) as u64).min(DAY_SECONDS - 1)
					);
				}
				out.extend(picked.into_iter().map(|s| cfg.start + day * DAY_SECONDS + s));
			}
			out
		}
	}
}

pub fn asset(cfg: &SecondConfig, sample: &Sample) -> Pool {
	let tag = match sample.class {
		Class::Frequent => 1u64,
		Class::Infrequent => 2u64,
	};
	let mut rng = Rng(cfg.seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ (tag << 32 | sample.rank as u64));
	let initial = log_uniform(1e-9, 1e3, rng.uniform());
	let scale = (8.0 - initial.ln() / LN_10).round().clamp(0.0, 18.0) as u32;
	let decimals = if rng.uniform() < 0.5 {
		6
	} else {
		9
	};
	let (sigma, extra) = match sample.class {
		Class::Frequent => {
			let rank_frac = (sample.rank as f64).ln() / (cfg.frequent.max(2) as f64).ln();
			(
				log_uniform(0.0002, 0.01, 0.5 * rank_frac + 0.5 * rng.uniform()),
				cfg.extra_trades * activity(cfg, sample.rank),
			)
		}
		Class::Infrequent => (log_uniform(0.002, 0.03, rng.uniform()), 0.5),
	};
	let factor = 10f64.powi(scale as i32);
	let units = 10f64.powi(decimals as i32);

	let seconds = active_seconds(cfg, sample, &mut rng);
	let mut log_price = initial.ln();
	let mut previous = cfg.start;
	let mut bars = Bars::default();
	for second in seconds {
		let elapsed = (second + 1 - previous) as f64;
		if rng.uniform() < JUMP_PER_SECOND * elapsed {
			log_price += if rng.uniform() < 0.5 {
				0.1f64.ln()
			} else {
				3.0f64.ln()
			};
		}
		log_price += rng.normal() * sigma * GAP_VOLATILITY * (elapsed - 1.0).sqrt();
		previous = second + 1;
		let trades = 1 + poisson(&mut rng, extra);
		let (mut open, mut high, mut low, mut close) = (0, i64::MIN, i64::MAX, 0);
		let mut base = 0.0;
		for trade in 0..trades {
			log_price += student_t3(&mut rng) * sigma;
			let price = log_price.exp();
			let fixed = (price * factor).round().clamp(1.0, MAX_PRICE) as i64;
			if trade == 0 {
				open = fixed;
			}
			high = high.max(fixed);
			low = low.min(fixed);
			close = fixed;
			let size = 0.5 * (1.6 * rng.normal()).exp();
			base += size / price;
		}
		bars.slot.push(second);
		bars.open.push(open);
		bars.high.push(high);
		bars.low.push(low);
		bars.close.push(close);
		bars.volume.push((base * units).round().clamp(1.0, u64::MAX as f64) as u64);
	}
	Pool {
		pool_id: sample.rank,
		scale,
		decimals,
		bars,
	}
}

#[cfg(test)]
mod tests {
	use super::{Class, DAY_SECONDS, SecondConfig, activity, asset, samples, stratum_bounds};

	fn small() -> SecondConfig {
		SecondConfig {
			frequent: 200,
			infrequent: 1_000,
			sample_frequent: 20,
			sample_infrequent: 10,
			days: 1,
			..SecondConfig::default()
		}
	}

	#[test]
	fn the_start_aligns_with_every_bucket_width() {
		// Buckets are floor(key / width) * width; a misaligned start splits the first day across two buckets
		// and makes widths incomparable.
		let cfg = SecondConfig::default();
		for days in 1..=7 {
			assert_eq!(cfg.start % (days * DAY_SECONDS), 0, "{days} days");
		}
	}

	#[test]
	fn the_activity_skew_matches_the_target() {
		// The bench assumes the top asset trades almost every second and the median frequent asset about one in
		// ten; a different skew changes every extrapolated total.
		let cfg = SecondConfig::default();
		assert!((activity(&cfg, 1) - 1.0).abs() < 1e-12);
		let median = activity(&cfg, cfg.frequent / 2);
		assert!((0.09..0.11).contains(&median), "{median}");
	}

	#[test]
	fn strata_cover_every_frequent_rank_exactly_once() {
		// Extrapolation multiplies each sampled asset by its stratum size; gaps or overlaps would miscount the
		// population.
		for (population, strata) in [(10_000, 500), (200, 20), (5, 10), (1, 1)] {
			let bounds = stratum_bounds(population, strata);
			assert_eq!(bounds[0], 1);
			assert_eq!(*bounds.last().unwrap(), population + 1);
			assert!(bounds.windows(2).all(|w| w[0] < w[1]));
			assert_eq!(bounds.len() as u32 - 1, strata.min(population));
		}
		let cfg = SecondConfig::default();
		let picked = samples(&cfg);
		let weight = |class| picked.iter().filter(|s| s.class == class).map(|s| s.weight).sum::<f64>();
		assert_eq!(weight(Class::Frequent), cfg.frequent as f64);
		assert_eq!(weight(Class::Infrequent), cfg.infrequent as f64);
	}

	#[test]
	fn every_bar_is_consistent_ohlc_on_strictly_increasing_seconds() {
		// Keys must be unique seconds inside the window, and the wick transform stores unsigned wicks.
		let cfg = small();
		for sample in samples(&cfg) {
			let pool = asset(&cfg, &sample);
			let b = &pool.bars;
			assert!(b.slot.windows(2).all(|w| w[0] < w[1]), "{sample:?}");
			for i in 0..b.slot.len() {
				assert!(b.slot[i] >= cfg.start && b.slot[i] < cfg.start + DAY_SECONDS, "{sample:?}");
				assert!(b.low[i] >= 1, "{sample:?} bar {i}");
				assert!(b.low[i] <= b.open[i].min(b.close[i]), "{sample:?} bar {i}");
				assert!(b.high[i] >= b.open[i].max(b.close[i]), "{sample:?} bar {i}");
			}
		}
	}

	#[test]
	fn infrequent_assets_trade_within_the_configured_seconds_per_day() {
		// The infrequent class is defined by 20 to 100 active seconds per day; more would blur the two classes.
		let cfg = small();
		for sample in samples(&cfg).iter().filter(|s| s.class == Class::Infrequent) {
			let rows = asset(&cfg, sample).bars.slot.len() as u32;
			assert!((20..=100).contains(&rows), "{sample:?} {rows}");
		}
	}

	#[test]
	fn the_top_frequent_asset_trades_every_second() {
		// Without a dense head the per-asset blocks of the busiest assets would not be measured at all.
		let cfg = small();
		let top = samples(&cfg).into_iter().find(|s| s.class == Class::Frequent && s.rank == 1).unwrap();
		assert_eq!(asset(&cfg, &top).bars.slot.len() as u64, DAY_SECONDS);
	}

	#[test]
	fn the_generator_is_deterministic() {
		// Profiles and widths are compared on the same data; a different draw per run would mix noise into the
		// comparison.
		let cfg = small();
		for sample in samples(&cfg) {
			let (a, b) = (asset(&cfg, &sample), asset(&cfg, &sample));
			assert_eq!(a.bars.slot, b.bars.slot);
			assert_eq!(a.bars.close, b.bars.close);
			assert_eq!(a.bars.volume, b.bars.volume);
		}
	}
}
