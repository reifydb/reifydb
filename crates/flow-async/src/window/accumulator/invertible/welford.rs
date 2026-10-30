// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::metrics::heap::HeapSize;
use reifydb_macro::operator_state;

#[operator_state]
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Welford {
	n: u64,
	mean: f64,
	m2: f64,
}

impl Welford {
	pub fn push(&mut self, x: f64) {
		self.n += 1;
		let delta = x - self.mean;
		self.mean += delta / self.n as f64;
		self.m2 += delta * (x - self.mean);
	}

	pub fn merge(&mut self, other: &Welford) {
		if other.n == 0 {
			return;
		}
		if self.n == 0 {
			*self = *other;
			return;
		}
		let n = self.n + other.n;
		let delta = other.mean - self.mean;
		self.mean += delta * other.n as f64 / n as f64;
		self.m2 += other.m2 + delta * delta * self.n as f64 * other.n as f64 / n as f64;
		self.n = n;
	}

	pub fn count(&self) -> u64 {
		self.n
	}

	pub fn is_empty(&self) -> bool {
		self.n == 0
	}

	pub fn mean(&self) -> Option<f64> {
		(self.n > 0).then_some(self.mean)
	}

	pub fn variance_sample(&self) -> Option<f64> {
		(self.n > 1).then(|| (self.m2 / (self.n - 1) as f64).max(0.0))
	}

	pub fn stddev_sample(&self) -> Option<f64> {
		self.variance_sample().map(f64::sqrt)
	}
}

impl HeapSize for Welford {
	fn heap_size(&self) -> usize {
		0
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn two_pass(samples: &[f64]) -> (f64, f64) {
		let n = samples.len() as f64;
		let mean = samples.iter().sum::<f64>() / n;
		let var = samples.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
		(mean, var.sqrt())
	}

	fn close(a: f64, b: f64) -> bool {
		(a - b).abs() <= 1e-9 * b.abs().max(1.0)
	}

	fn pushed(samples: &[f64]) -> Welford {
		let mut w = Welford::default();
		for &x in samples {
			w.push(x);
		}
		w
	}

	#[test]
	fn two_samples_give_their_mean_and_sample_stddev() {
		let w = pushed(&[10.0, 20.0]);
		assert_eq!(w.mean(), Some(15.0));
		assert!(close(w.stddev_sample().unwrap(), 50.0_f64.sqrt()));
	}

	#[test]
	fn one_sample_has_a_mean_but_no_sample_stddev() {
		// n - 1 = 0: a sample stddev of one value is undefined, never zero by accident of the formula.
		let w = pushed(&[42.0]);
		assert_eq!(w.mean(), Some(42.0));
		assert_eq!(w.stddev_sample(), None);
		assert_eq!(Welford::default().mean(), None);
	}

	#[test]
	fn large_values_with_a_small_spread_keep_their_stddev() {
		// Sum-of-squares loses this spread entirely at 1e9; the stddev must still match a two-pass.
		let samples: Vec<f64> = (0..11).map(|i| 1e9 + i as f64).collect();
		let w = pushed(&samples);
		let (mean, stddev) = two_pass(&samples);
		assert!(close(w.mean().unwrap(), mean));
		assert!(close(w.stddev_sample().unwrap(), stddev), "{} vs {stddev}", w.stddev_sample().unwrap());
	}

	#[test]
	fn every_split_merges_to_the_two_pass_result() {
		let samples = [3.5, -1.0, 8.25, 0.0, 12.0, 7.5, 7.5];
		let (mean, stddev) = two_pass(&samples);
		for cut in 0..=samples.len() {
			let mut older = pushed(&samples[..cut]);
			older.merge(&pushed(&samples[cut..]));
			assert_eq!(older.count(), samples.len() as u64);
			assert!(close(older.mean().unwrap(), mean), "cut {cut}");
			assert!(close(older.stddev_sample().unwrap(), stddev), "cut {cut}");
		}
	}
}
