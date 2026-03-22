// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Random number generation abstraction.
//!
//! Provides an `Rng` enum that can use either OS entropy or a deterministic seed.

use std::sync::{Arc, Mutex};

use rand::{Rng as RandRng, SeedableRng, rngs::StdRng};

/// A random number generator that can be either OS-backed or seeded/deterministic.
#[derive(Clone)]
pub enum Rng {
	/// Uses OS entropy via `getrandom` (non-deterministic).
	Os,
	/// Uses a seeded PRNG for deterministic output (e.g. testing).
	Seeded(SeededRng),
}

impl Default for Rng {
	fn default() -> Self {
		Rng::Os
	}
}

impl Rng {
	/// Create a deterministic RNG from the given seed.
	pub fn seeded(seed: u64) -> Self {
		Rng::Seeded(SeededRng::new(seed))
	}

	/// Generate 16 random bytes (suitable for UUID v4).
	pub fn bytes_16(&self) -> [u8; 16] {
		match self {
			Rng::Os => {
				let mut buf = [0u8; 16];
				getrandom::fill(&mut buf).expect("getrandom failed");
				buf
			}
			Rng::Seeded(seeded) => {
				let mut buf = [0u8; 16];
				let mut rng = seeded.inner.lock().unwrap();
				rng.fill_bytes(&mut buf);
				buf
			}
		}
	}

	/// Generate 10 random bytes (suitable for UUID v7 random portion).
	pub fn bytes_10(&self) -> [u8; 10] {
		match self {
			Rng::Os => {
				let mut buf = [0u8; 10];
				getrandom::fill(&mut buf).expect("getrandom failed");
				buf
			}
			Rng::Seeded(seeded) => {
				let mut buf = [0u8; 10];
				let mut rng = seeded.inner.lock().unwrap();
				rng.fill_bytes(&mut buf);
				buf
			}
		}
	}
}

/// A seeded, deterministic RNG backed by `StdRng` wrapped in `Arc<Mutex<..>>`.
#[derive(Clone)]
pub struct SeededRng {
	inner: Arc<Mutex<StdRng>>,
}

impl SeededRng {
	/// Create a new seeded RNG from the given seed.
	pub fn new(seed: u64) -> Self {
		Self {
			inner: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
		}
	}
}
