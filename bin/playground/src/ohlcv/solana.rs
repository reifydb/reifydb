// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::f64::consts::LN_10;

use crate::equity::Rng;

pub struct SolanaConfig {
	pub pools: u32,
	pub days: u32,
	pub slot_ms: u64,
	pub bar_slots: u64,
	pub genesis_slot: u64,
	pub zipf_s: f64,
	pub top_rate: f64,
	pub seed: u64,
}

impl Default for SolanaConfig {
	fn default() -> Self {
		Self {
			pools: 2_000,
			days: 7,
			slot_ms: 400,
			bar_slots: 150,
			genesis_slot: 370_000_000,
			zipf_s: 1.1,
			top_rate: 3.0,
			seed: 7,
		}
	}
}

impl SolanaConfig {
	pub fn bars_per_day(&self) -> u64 {
		86_400_000 / (self.slot_ms * self.bar_slots)
	}

	pub fn day_slots(&self) -> u64 {
		self.bars_per_day() * self.bar_slots
	}
}

#[derive(Default)]
pub struct Bars {
	pub slot: Vec<u64>,
	pub open: Vec<i64>,
	pub high: Vec<i64>,
	pub low: Vec<i64>,
	pub close: Vec<i64>,
	pub volume: Vec<u64>,
}

pub struct Pool {
	pub pool_id: u32,
	pub scale: u32,
	pub decimals: u32,
	pub bars: Bars,
}

const MAX_PRICE: f64 = 1e17;
const JUMP_PROBABILITY: f64 = 1e-4;

fn log_uniform(lo: f64, hi: f64, u: f64) -> f64 {
	(lo.ln() + (hi.ln() - lo.ln()) * u).exp()
}

fn poisson(rng: &mut Rng, mean: f64) -> u64 {
	if mean > 50.0 {
		return (mean + mean.sqrt() * rng.normal()).round().max(0.0) as u64;
	}
	let limit = (-mean).exp();
	let mut product = rng.uniform();
	let mut count = 0;
	while product > limit {
		product *= rng.uniform();
		count += 1;
	}
	count
}

fn student_t3(rng: &mut Rng) -> f64 {
	let z = rng.normal();
	let chi = (0..3).map(|_| rng.normal().powi(2)).sum::<f64>();
	z / (chi / 3.0).sqrt()
}

fn pool(cfg: &SolanaConfig, rank: u32) -> Pool {
	let mut rng = Rng(cfg.seed.wrapping_mul(0x2545_F491_4F6C_DD1D) ^ rank as u64);
	let r = rank as f64;
	let mean = cfg.top_rate / r.powf(cfg.zipf_s) * cfg.bar_slots as f64;
	let initial = log_uniform(1e-9, 1e3, rng.uniform());
	let scale = (8.0 - initial.ln() / LN_10).round().clamp(0.0, 18.0) as u32;
	let decimals = if rng.uniform() < 0.5 {
		6
	} else {
		9
	};
	let rank_frac = if cfg.pools > 1 {
		r.ln() / (cfg.pools as f64).ln()
	} else {
		0.0
	};
	let sigma = log_uniform(0.0005, 0.03, 0.5 * rank_frac + 0.5 * rng.uniform());
	let factor = 10f64.powi(scale as i32);
	let units = 10f64.powi(decimals as i32);

	let mut log_price = initial.ln();
	let mut bars = Bars::default();
	for bar in 0..cfg.days as u64 * cfg.bars_per_day() {
		if rng.uniform() < JUMP_PROBABILITY {
			log_price += if rng.uniform() < 0.5 {
				0.1f64.ln()
			} else {
				3.0f64.ln()
			};
		}
		let trades = poisson(&mut rng, mean);
		if trades == 0 {
			continue;
		}
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
		bars.slot.push(cfg.genesis_slot + bar * cfg.bar_slots);
		bars.open.push(open);
		bars.high.push(high);
		bars.low.push(low);
		bars.close.push(close);
		bars.volume.push((base * units).round().clamp(1.0, u64::MAX as f64) as u64);
	}
	Pool {
		pool_id: rank,
		scale,
		decimals,
		bars,
	}
}

pub fn generate(cfg: &SolanaConfig) -> Vec<Pool> {
	(1..=cfg.pools).map(|rank| pool(cfg, rank)).collect()
}

pub fn summary(pools: &[Pool]) -> String {
	let mut counts: Vec<usize> = pools.iter().map(|p| p.bars.slot.len()).collect();
	counts.sort_unstable();
	let total: usize = counts.iter().sum();
	let at = |q: f64| counts.get(((counts.len() as f64 - 1.0) * q).round() as usize).copied().unwrap_or(0);
	let scales = pools.iter().map(|p| p.scale);
	let nine = pools.iter().filter(|p| p.decimals == 9).count();
	format!(
		"solana: {} pools, {} bars, bars per pool p50 {} p90 {} p99 {} max {}, scale {}..={}, {} pools with 9 decimals",
		pools.len(),
		total,
		at(0.5),
		at(0.9),
		at(0.99),
		counts.last().copied().unwrap_or(0),
		scales.clone().min().unwrap_or(0),
		scales.max().unwrap_or(0),
		nine
	)
}

#[cfg(test)]
mod tests {
	use super::{SolanaConfig, generate};

	fn small() -> SolanaConfig {
		SolanaConfig {
			pools: 50,
			days: 1,
			..SolanaConfig::default()
		}
	}

	#[test]
	fn every_bar_is_consistent_ohlc() {
		// The wick transform stores high - max(open, close) as unsigned; an inconsistent bar would overflow it.
		for pool in generate(&small()) {
			let b = &pool.bars;
			for i in 0..b.slot.len() {
				assert!(b.low[i] >= 1, "pool {} bar {i}", pool.pool_id);
				assert!(b.low[i] <= b.open[i].min(b.close[i]), "pool {} bar {i}", pool.pool_id);
				assert!(b.high[i] >= b.open[i].max(b.close[i]), "pool {} bar {i}", pool.pool_id);
			}
		}
	}

	#[test]
	fn slots_are_strictly_increasing_bar_boundaries() {
		// Series keys must be ordered and aligned to bars, or daily buckets would split or merge bars.
		let cfg = small();
		for pool in generate(&cfg) {
			for pair in pool.bars.slot.windows(2) {
				assert!(pair[0] < pair[1], "pool {}", pool.pool_id);
			}
			for slot in &pool.bars.slot {
				assert_eq!((slot - cfg.genesis_slot) % cfg.bar_slots, 0, "pool {}", pool.pool_id);
				assert!(*slot < cfg.genesis_slot + cfg.days as u64 * cfg.day_slots());
			}
		}
	}

	#[test]
	fn activity_is_skewed_towards_top_ranks() {
		// The long tail being sparse is the property that makes per-pool blocks tiny; without it L1 measures
		// nothing interesting.
		let cfg = small();
		let pools = generate(&cfg);
		assert_eq!(pools[0].bars.slot.len() as u64, cfg.bars_per_day());
		assert!(pools[49].bars.slot.len() < pools[0].bars.slot.len());
	}

	#[test]
	fn the_generator_is_deterministic() {
		// Results must be reproducible across runs to compare profiles and layouts.
		let a = generate(&small());
		let b = generate(&small());
		for (x, y) in a.iter().zip(&b) {
			assert_eq!(x.bars.close, y.bars.close);
			assert_eq!(x.bars.volume, y.bars.volume);
		}
	}
}
