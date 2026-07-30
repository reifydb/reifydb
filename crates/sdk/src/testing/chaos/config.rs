// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_testing_chaos::operator::scenario::{BatchSize, Scenario};

#[derive(Debug, Clone, Copy)]
pub struct SupportedOps {
	pub insert: bool,
	pub update: bool,
	pub remove: bool,
}

impl Default for SupportedOps {
	fn default() -> Self {
		Self::all()
	}
}

impl SupportedOps {
	pub const fn all() -> Self {
		Self {
			insert: true,
			update: true,
			remove: true,
		}
	}

	pub const fn insert_only() -> Self {
		Self {
			insert: true,
			update: false,
			remove: false,
		}
	}

	pub const fn no_remove() -> Self {
		Self {
			insert: true,
			update: true,
			remove: false,
		}
	}

	pub const fn no_update() -> Self {
		Self {
			insert: true,
			update: false,
			remove: true,
		}
	}
}

#[derive(Debug, Clone, Copy)]
pub enum BatchSizeDist {
	Constant(usize),

	Uniform {
		min: usize,
		max: usize,
	},

	Geometric(f64),
}

impl Default for BatchSizeDist {
	fn default() -> Self {
		Self::Geometric(0.4)
	}
}

#[derive(Debug, Clone, Copy)]
pub struct ChaosConfig {
	pub num_ops: usize,
	pub max_live_rows: usize,
	pub duplicate_update_burst: f64,
	pub update_as_remove_insert: f64,
	pub batch_size: BatchSizeDist,
	pub supported_ops: SupportedOps,
}

impl Default for ChaosConfig {
	fn default() -> Self {
		Self {
			num_ops: 200,
			max_live_rows: 50,
			duplicate_update_burst: 0.3,
			update_as_remove_insert: 0.1,
			batch_size: BatchSizeDist::default(),
			supported_ops: SupportedOps::default(),
		}
	}
}

impl ChaosConfig {
	/// Projects this configuration onto the shared chaos scenario.
	///
	/// The scenario is the authoritative description of a corpus, and both chaos families now express
	/// themselves in its terms. Guest configurations carry no clock dimension, so `tick_pct` is zero and
	/// the coordinate span is unused; what they do carry - the mutation primitives and the live-row cap -
	/// are the same knobs the shared generator reads, so a scenario written here means exactly what it
	/// means when a window family writes it.
	///
	/// The operation mix comes from `supported_ops`, which gates whether a step may remove or update at
	/// all rather than giving a share to each.
	pub fn to_scenario(&self, coord_span_ms: u64, drain_at_ms: u64) -> Scenario {
		let max_batch = match self.batch_size {
			BatchSizeDist::Constant(n) => n.max(1) as u32,
			BatchSizeDist::Uniform { max, .. } => max.max(1) as u32,
			BatchSizeDist::Geometric(_) => 8,
		};
		let batch = match self.batch_size {
			BatchSizeDist::Constant(n) => BatchSize::Constant(n.max(1) as u32),
			BatchSizeDist::Uniform { max, .. } => BatchSize::Uniform(max.max(1) as u32),
			BatchSizeDist::Geometric(p) => BatchSize::Geometric { p, max: max_batch },
		};
		Scenario {
			batch,
			..Scenario::windowed(self.num_ops as u32, max_batch, coord_span_ms, drain_at_ms)
				.with_mix(
					if self.supported_ops.remove { 25 } else { 0 },
					if self.supported_ops.update { 30 } else { 0 },
					0,
				)
				.with_max_live(self.max_live_rows.max(1))
				.with_duplicate_update_burst(self.duplicate_update_burst)
				.with_update_as_remove_insert(self.update_as_remove_insert)
		}
	}
}

#[cfg(test)]
mod scenario_projection_tests {
	use super::*;

	#[test]
	fn a_guest_configuration_carries_its_primitives_into_the_shared_scenario() {
		// The two families used to describe a corpus in unrelated vocabularies, so a scenario expressed
		// on one side was simply unavailable on the other. This projection is what makes them one
		// description; if a knob failed to carry, that half of the scenario would silently not happen.
		let config = ChaosConfig {
			num_ops: 150,
			max_live_rows: 30,
			duplicate_update_burst: 0.5,
			update_as_remove_insert: 0.25,
			batch_size: BatchSizeDist::Geometric(0.4),
			supported_ops: SupportedOps::all(),
		};

		let scenario = config.to_scenario(400_000, 500_000);

		assert_eq!(scenario.steps, 150);
		assert_eq!(scenario.max_live, Some(30));
		assert_eq!(scenario.duplicate_update_burst, 0.5, "the duplicate burst must survive the projection");
		assert_eq!(scenario.update_as_remove_insert, 0.25, "the remove-insert rewrite must survive too");
		assert!(matches!(scenario.batch, BatchSize::Geometric { .. }), "a long-tailed batch must stay long-tailed");
		assert_eq!(scenario.tick_pct, 0, "a guest configuration has no clock dimension to advance");
		assert!(scenario.remove_pct > 0 && scenario.update_pct > 0, "SupportedOps::all must enable both");
	}

	#[test]
	fn disabling_an_operation_zeroes_its_share_rather_than_leaving_it_enabled() {
		// insert_only exists so a suite can isolate the accumulate path. If the projection left a
		// remove or update share standing, that isolation would quietly stop holding.
		let config = ChaosConfig {
			supported_ops: SupportedOps::insert_only(),
			..ChaosConfig::default()
		};

		let scenario = config.to_scenario(1_000, 2_000);

		assert_eq!(scenario.remove_pct, 0, "insert_only must not leave a remove share");
		assert_eq!(scenario.update_pct, 0, "insert_only must not leave an update share");
	}
}
