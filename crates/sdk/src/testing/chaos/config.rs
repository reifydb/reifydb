// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

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
