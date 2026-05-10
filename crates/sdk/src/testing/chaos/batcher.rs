// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::mem::take;

use rand::{RngExt, SeedableRng, rngs::StdRng};
use reifydb_core::interface::change::Change;

use super::{
	config::BatchSizeDist,
	event::{ChaosBatch, ChaosEvent},
	generator::Generator,
};
use crate::testing::builders::TestChangeBuilder;

pub struct Batcher {
	dist: BatchSizeDist,
	rng: StdRng,

	logical_log: Vec<ChaosBatch>,
}

impl Batcher {
	pub fn new(dist: BatchSizeDist, seed: u64) -> Self {
		Self {
			dist,
			rng: StdRng::seed_from_u64(seed),
			logical_log: Vec::new(),
		}
	}

	pub fn logical_log(&self) -> &[ChaosBatch] {
		&self.logical_log
	}

	pub fn take_logical_log(&mut self) -> Vec<ChaosBatch> {
		take(&mut self.logical_log)
	}

	pub fn next_change(&mut self, generator: &mut Generator) -> Option<Change> {
		let target = self.sample_batch_size();
		let mut builder = TestChangeBuilder::new();
		let mut batch_events: Vec<ChaosEvent> = Vec::new();
		for _ in 0..target {
			let Some(ev) = generator.next_event() else {
				break;
			};
			match &ev {
				ChaosEvent::Insert {
					row,
					..
				} => {
					builder = builder.insert(row.clone());
				}
				ChaosEvent::Update {
					pre,
					post,
					..
				} => {
					builder = builder.update(pre.clone(), post.clone());
				}
				ChaosEvent::Remove {
					row,
					..
				} => {
					builder = builder.remove(row.clone());
				}
			}
			batch_events.push(ev);
		}
		if batch_events.is_empty() {
			return None;
		}
		self.logical_log.push(ChaosBatch::new(batch_events));
		Some(builder.build())
	}

	fn sample_batch_size(&mut self) -> usize {
		match self.dist {
			BatchSizeDist::Constant(n) => n.max(1),
			BatchSizeDist::Uniform {
				min,
				max,
			} => {
				let lo = min.max(1);
				let hi = max.max(lo);
				if lo == hi {
					lo
				} else {
					self.rng.random_range(lo..=hi)
				}
			}
			BatchSizeDist::Geometric(p) => {
				let p = p.clamp(1e-6, 1.0);
				let mut n = 1usize;
				const CAP: usize = 4096;
				while n < CAP {
					let u: f64 = self.rng.random_range(0.0..1.0);
					if u < p {
						break;
					}
					n += 1;
				}
				n
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use reifydb_core::encoded::shape::{RowShape, RowShapeField};
	use reifydb_type::value::r#type::Type;

	use super::{
		super::{
			config::{ChaosConfig, SupportedOps},
			schema::{ChaosSchema, KeyStrategy},
			strategy::{ColumnRegistry, samplers},
		},
		*,
	};

	fn schema_basic() -> Arc<ChaosSchema> {
		let s = RowShape::new(vec![
			RowShapeField::unconstrained("k", Type::Uint8),
			RowShapeField::unconstrained("v", Type::Float8),
		]);
		Arc::new(ChaosSchema {
			input_shape: s.clone(),
			output_shape: s,
			key_strategy: KeyStrategy::Sequential,
			output_key_columns: vec!["k".into()],
		})
	}

	fn registry() -> Arc<ColumnRegistry> {
		let mut r = ColumnRegistry::new();
		r.register("k", samplers::u64_range(1..1000));
		r.register("v", samplers::f64_range(0.0..100.0));
		Arc::new(r)
	}

	fn cfg(num_ops: usize, dist: BatchSizeDist) -> ChaosConfig {
		ChaosConfig {
			num_ops,
			max_live_rows: 200,
			duplicate_update_burst: 0.0,
			update_as_remove_insert: 0.0,
			batch_size: dist,
			supported_ops: SupportedOps::all(),
		}
	}

	#[test]
	fn constant_batch_size_one_emits_one_diff_per_change() {
		let mut g = Generator::new(schema_basic(), registry(), cfg(50, BatchSizeDist::Constant(1)), 0);
		let mut b = Batcher::new(BatchSizeDist::Constant(1), 0);
		let mut total_diffs = 0;
		let mut total_changes = 0;
		while let Some(change) = b.next_change(&mut g) {
			assert_eq!(change.diffs.len(), 1, "expected exactly 1 diff per Change at Constant(1)");
			total_diffs += change.diffs.len();
			total_changes += 1;
		}
		assert_eq!(total_diffs, 50);
		assert_eq!(total_changes, 50);
	}

	#[test]
	fn constant_batch_size_n_packs_into_two_changes_when_total_eq_2n() {
		// 100 ops in batches of 50 -> exactly 2 Changes.
		let mut g = Generator::new(schema_basic(), registry(), cfg(100, BatchSizeDist::Constant(50)), 0);
		let mut b = Batcher::new(BatchSizeDist::Constant(50), 0);
		let mut count = 0;
		while let Some(change) = b.next_change(&mut g) {
			assert_eq!(change.diffs.len(), 50);
			count += 1;
		}
		assert_eq!(count, 2);
	}

	#[test]
	fn partial_final_batch_is_emitted() {
		// 47 ops in batches of 10 -> 4 full batches of 10 + 1 batch of 7.
		let mut g = Generator::new(schema_basic(), registry(), cfg(47, BatchSizeDist::Constant(10)), 0);
		let mut b = Batcher::new(BatchSizeDist::Constant(10), 0);
		let mut sizes = Vec::new();
		while let Some(change) = b.next_change(&mut g) {
			sizes.push(change.diffs.len());
		}
		assert_eq!(sizes, vec![10, 10, 10, 10, 7]);
	}

	#[test]
	fn uniform_batch_size_in_bounds() {
		let mut g = Generator::new(
			schema_basic(),
			registry(),
			cfg(
				500,
				BatchSizeDist::Uniform {
					min: 5,
					max: 15,
				},
			),
			0,
		);
		let mut b = Batcher::new(
			BatchSizeDist::Uniform {
				min: 5,
				max: 15,
			},
			0,
		);
		let mut total = 0;
		while let Some(change) = b.next_change(&mut g) {
			let n = change.diffs.len();
			// Final batch may be partial (less than min); skip
			// bound check on last.
			total += n;
			if total < 500 {
				assert!((5..=15).contains(&n), "batch size {n} out of [5,15]");
			}
		}
		assert_eq!(total, 500);
	}

	#[test]
	fn geometric_batch_size_distribution_is_reasonable() {
		// p=0.4 -> mean ~ 1/0.4 = 2.5. Verify the average over many
		// batches sits in a sane envelope.
		let mut g = Generator::new(schema_basic(), registry(), cfg(2000, BatchSizeDist::Geometric(0.4)), 1);
		let mut b = Batcher::new(BatchSizeDist::Geometric(0.4), 1);
		let mut sizes = Vec::new();
		while let Some(change) = b.next_change(&mut g) {
			sizes.push(change.diffs.len());
		}
		let total: usize = sizes.iter().sum();
		assert_eq!(total, 2000);
		let mean = total as f64 / sizes.len() as f64;
		// Theoretical mean is 2.5; allow a wide envelope.
		assert!(mean > 1.5 && mean < 4.5, "geometric mean out of envelope: {mean}");
	}

	#[test]
	fn logical_log_matches_emitted_events() {
		// The logical_log accumulates exactly the events the batcher
		// pulled from the generator, batched the same way the operator
		// sees them. With duplicate-burst off and rewrite off, the
		// flattened log equals what the operator sees diff-for-diff.
		let mut g = Generator::new(schema_basic(), registry(), cfg(20, BatchSizeDist::Constant(3)), 7);
		let mut b = Batcher::new(BatchSizeDist::Constant(3), 7);
		while b.next_change(&mut g).is_some() {}
		let log = b.logical_log();
		// 20 ops in batches of 3 -> 7 batches: [3,3,3,3,3,3,2].
		assert_eq!(log.len(), 7);
		let total_events: usize = log.iter().map(|b| b.len()).sum();
		assert_eq!(total_events, 20);
	}

	#[test]
	fn same_seed_pair_produces_same_changes() {
		// Both generator and batcher seeded the same way -> same output.
		fn run(seed: u64) -> Vec<usize> {
			let mut g = Generator::new(
				schema_basic(),
				registry(),
				cfg(50, BatchSizeDist::Geometric(0.4)),
				seed,
			);
			let mut b = Batcher::new(BatchSizeDist::Geometric(0.4), seed);
			let mut sizes = Vec::new();
			while let Some(c) = b.next_change(&mut g) {
				sizes.push(c.diffs.len());
			}
			sizes
		}
		assert_eq!(run(42), run(42));
		assert_ne!(run(42), run(43));
	}
}
