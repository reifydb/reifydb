// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use getrandom::fill as getrandom_fill;
use rand::{Rng as RandRng, RngExt, SeedableRng, rngs::StdRng};
use reifydb_value::clock::RandomBytes;
use xxhash_rust::xxh3::xxh3_64_with_seed;

use crate::sync::mutex::Mutex;

#[derive(Clone, Debug, Default)]
pub enum Rng {
	#[default]
	Os,

	Seeded(SeededRng),
}

impl RandomBytes for Rng {
	fn bytes_10(&self) -> [u8; 10] {
		self.bytes_10()
	}
}

impl Rng {
	pub fn seeded(seed: u64) -> Self {
		Rng::Seeded(SeededRng::new(seed))
	}

	pub fn fork(&self, key: &[u8]) -> Rng {
		match self {
			Rng::Os => Rng::Os,
			Rng::Seeded(seeded) => Rng::Seeded(SeededRng::new(xxh3_64_with_seed(key, seeded.seed))),
		}
	}

	pub fn bytes_16(&self) -> [u8; 16] {
		match self {
			Rng::Os => {
				let mut buf = [0u8; 16];
				getrandom_fill(&mut buf).expect("getrandom failed");
				buf
			}
			Rng::Seeded(seeded) => {
				let mut buf = [0u8; 16];
				let mut rng = seeded.inner.lock();
				rng.fill_bytes(&mut buf);
				buf
			}
		}
	}

	pub fn bytes_32(&self) -> [u8; 32] {
		match self {
			Rng::Os => {
				let mut buf = [0u8; 32];
				getrandom_fill(&mut buf).expect("getrandom failed");
				buf
			}
			Rng::Seeded(seeded) => {
				let mut buf = [0u8; 32];
				let mut rng = seeded.inner.lock();
				rng.fill_bytes(&mut buf);
				buf
			}
		}
	}

	pub fn bytes_10(&self) -> [u8; 10] {
		match self {
			Rng::Os => {
				let mut buf = [0u8; 10];
				getrandom_fill(&mut buf).expect("getrandom failed");
				buf
			}
			Rng::Seeded(seeded) => {
				let mut buf = [0u8; 10];
				let mut rng = seeded.inner.lock();
				rng.fill_bytes(&mut buf);
				buf
			}
		}
	}

	pub fn infra_bytes_10(&self) -> [u8; 10] {
		match self {
			Rng::Os => {
				let mut buf = [0u8; 10];
				getrandom_fill(&mut buf).expect("getrandom failed");
				buf
			}
			Rng::Seeded(seeded) => {
				let mut buf = [0u8; 10];
				let mut rng = seeded.infra.lock();
				rng.fill_bytes(&mut buf);
				buf
			}
		}
	}

	pub fn infra_bytes_32(&self) -> [u8; 32] {
		match self {
			Rng::Os => {
				let mut buf = [0u8; 32];
				getrandom_fill(&mut buf).expect("getrandom failed");
				buf
			}
			Rng::Seeded(seeded) => {
				let mut buf = [0u8; 32];
				let mut rng = seeded.infra.lock();
				rng.fill_bytes(&mut buf);
				buf
			}
		}
	}

	pub fn infra_u64_inclusive(&self, max_inclusive: u64) -> u64 {
		if max_inclusive == 0 {
			return 0;
		}
		match self {
			Rng::Os => {
				let mut buf = [0u8; 8];
				getrandom_fill(&mut buf).expect("getrandom failed");
				let raw = u64::from_le_bytes(buf);
				if max_inclusive == u64::MAX {
					raw
				} else {
					raw % (max_inclusive + 1)
				}
			}
			Rng::Seeded(seeded) => {
				let mut rng = seeded.infra.lock();
				rng.random_range(0..=max_inclusive)
			}
		}
	}
}

#[derive(Clone, Debug)]
pub struct SeededRng {
	seed: u64,

	inner: Arc<Mutex<StdRng>>,

	infra: Arc<Mutex<StdRng>>,
}

impl SeededRng {
	pub fn new(seed: u64) -> Self {
		Self {
			seed,
			inner: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
			infra: Arc::new(Mutex::new(StdRng::seed_from_u64(seed ^ 0x5A5A5A5A5A5A5A5A))),
		}
	}
}
