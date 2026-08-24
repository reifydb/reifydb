// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher},
};

const ROWS: usize = 4;

const ROW_SEEDS: [u64; ROWS] =
	[0x9E37_79B9_7F4A_7C15, 0xBF58_476D_1CE4_E5B9, 0x94D0_49BB_1331_11EB, 0x2545_F491_4F6C_DD1D];

const RESET_FACTOR: u64 = 10;

const RESET_FLOOR: u64 = 1024;

pub(super) struct Sketch {
	rows: Box<[u8]>,
	width: usize,
	increments: u64,
	resets: u64,
}

impl Sketch {
	pub(super) fn new(counters: usize) -> Self {
		let width = counters.max(1).next_power_of_two();
		Self {
			rows: vec![0u8; width * ROWS].into_boxed_slice(),
			width,
			increments: 0,
			resets: 0,
		}
	}

	pub(super) fn record<K: Hash>(&mut self, key: &K, population: usize) {
		for (row, seed) in ROW_SEEDS.iter().enumerate() {
			let index = self.cell(row, hash_with(key, *seed));
			let cell = &mut self.rows[index];
			*cell = cell.saturating_add(1);
		}
		self.increments += 1;
		if self.increments >= threshold(population) {
			self.halve();
		}
	}

	pub(super) fn estimate<K: Hash>(&self, key: &K) -> u8 {
		ROW_SEEDS
			.iter()
			.enumerate()
			.map(|(row, seed)| self.rows[self.cell(row, hash_with(key, *seed))])
			.min()
			.unwrap_or(0)
	}

	pub(super) fn resets(&self) -> u64 {
		self.resets
	}

	pub(super) fn bytes(&self) -> usize {
		self.rows.len()
	}

	pub(super) fn clear(&mut self) {
		self.rows.fill(0);
		self.increments = 0;
	}

	fn cell(&self, row: usize, hash: u64) -> usize {
		row * self.width + (hash as usize & (self.width - 1))
	}

	fn halve(&mut self) {
		for cell in self.rows.iter_mut() {
			*cell >>= 1;
		}
		self.increments = 0;
		self.resets += 1;
	}
}

fn threshold(population: usize) -> u64 {
	(RESET_FACTOR * population as u64).max(RESET_FLOOR)
}

fn hash_with<K: Hash>(key: &K, seed: u64) -> u64 {
	let mut hasher = DefaultHasher::new();
	seed.hash(&mut hasher);
	key.hash(&mut hasher);
	hasher.finish()
}
