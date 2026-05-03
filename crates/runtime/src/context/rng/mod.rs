// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::{Arc, Mutex};

use getrandom::fill as getrandom_fill;
use rand::{Rng as RandRng, RngExt, SeedableRng, rngs::StdRng};

#[derive(Clone, Default)]
pub enum Rng {
	#[default]
	Os,

	Seeded(SeededRng),
}

impl Rng {
	pub fn seeded(seed: u64) -> Self {
		Rng::Seeded(SeededRng::new(seed))
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
				let mut rng = seeded.inner.lock().unwrap();
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
				let mut rng = seeded.inner.lock().unwrap();
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
				let mut rng = seeded.inner.lock().unwrap();
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
				let mut rng = seeded.infra.lock().unwrap();
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
				let mut rng = seeded.infra.lock().unwrap();
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
				let mut rng = seeded.infra.lock().unwrap();
				rng.random_range(0..=max_inclusive)
			}
		}
	}
}

#[derive(Clone)]
pub struct SeededRng {
	inner: Arc<Mutex<StdRng>>,

	infra: Arc<Mutex<StdRng>>,
}

impl SeededRng {
	pub fn new(seed: u64) -> Self {
		Self {
			inner: Arc::new(Mutex::new(StdRng::seed_from_u64(seed))),
			infra: Arc::new(Mutex::new(StdRng::seed_from_u64(seed ^ 0x5A5A5A5A5A5A5A5A))),
		}
	}
}
