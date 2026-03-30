// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Random number generation abstraction.
//!
//! Provides an `Rng` enum that can use either OS entropy or a deterministic seed.

use std::sync::{Arc, Mutex};

use getrandom::fill as getrandom_fill;
use rand::{Rng as RandRng, SeedableRng, rngs::StdRng};

/// A random number generator that can be either OS-backed or seeded/deterministic.
#[derive(Clone, Default)]
pub enum Rng {
	/// Uses OS entropy via `getrandom` (non-deterministic).
	#[default]
	Os,
	/// Uses a seeded PRNG for deterministic output (e.g. testing).
	Seeded(SeededRng),
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
				getrandom_fill(&mut buf).expect("getrandom failed");
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

	/// Generate 32 random bytes (suitable for token generation).
	pub fn bytes_32(&self) -> [u8; 32] {
		match self {
			Rng::Os => {
				let mut buf = [0u8; 32];
				getrandom_fill(&mut buf).expect("getrandom failed");
				buf
			}
			Rng::Seeded(seeded) => {
				let mut buf = [0u8; 32];
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
				getrandom_fill(&mut buf).expect("getrandom failed");
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

	/// Generate 10 random bytes from the infrastructure RNG stream.
	///
	/// Uses a separate RNG stream so that infrastructure operations (like
	/// transaction ID generation) do not perturb the primary RNG state.
	/// This ensures deterministic test output regardless of how many internal
	/// transactions each test runner creates.
	pub fn infra_bytes_10(&self) -> [u8; 10] {
		match self {
			Rng::Os => {
				let mut buf = [0u8; 10];
				getrandom_fill(&mut buf).expect("getrandom failed");
				buf
			}
			Rng::Seeded(seeded) => {
				let mut buf = [0u8; 10];
				let mut rng = seeded.infra.lock().unwrap();
				rng.fill_bytes(&mut buf);
				buf
			}
		}
	}

	/// Generate 32 random bytes from the infrastructure RNG stream.
	///
	/// Uses a separate RNG stream for infrastructure operations (like
	/// session token generation) that should not affect deterministic
	/// test output.
	pub fn infra_bytes_32(&self) -> [u8; 32] {
		match self {
			Rng::Os => {
				let mut buf = [0u8; 32];
				getrandom_fill(&mut buf).expect("getrandom failed");
				buf
			}
			Rng::Seeded(seeded) => {
				let mut buf = [0u8; 32];
				let mut rng = seeded.infra.lock().unwrap();
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
	/// Separate RNG stream for infrastructure use (e.g. transaction IDs).
	infra: Arc<Mutex<StdRng>>,
}

impl SeededRng {
	/// Create a new seeded RNG from the given seed.
	pub fn new(seed: u64) -> Self {
		Self {
			inner: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
			infra: Arc::new(Mutex::new(StdRng::seed_from_u64(seed ^ 0x5A5A5A5A5A5A5A5A))),
		}
	}
}
