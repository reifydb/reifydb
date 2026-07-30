// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Corpus {
	fingerprint: u64,
	steps: usize,
}

impl Corpus {
	pub fn new(fingerprint: u64, steps: usize) -> Self {
		Self {
			fingerprint,
			steps,
		}
	}

	pub fn fingerprint(&self) -> u64 {
		self.fingerprint
	}

	pub fn steps(&self) -> usize {
		self.steps
	}

	#[track_caller]
	pub fn assert_pinned(&self, expected: u64) {
		assert_eq!(
			self.fingerprint, expected,
			"this seed no longer produces the corpus it was pinned against ({} steps now). Something \
			 changed what the driver draws from the RNG, so the sequence has moved and this test is no \
			 longer covering the defect it names.\n\nDo NOT just paste {:#018x} in as the new value - \
			 that only re-pins whatever the seed happens to generate today. Re-derive a seed that still \
			 reproduces the defect (revert the fix, search seeds, confirm it fails) and record that seed \
			 with its fingerprint.",
			self.steps, self.fingerprint
		);
	}
}

pub fn mix(state: u64, value: u64) -> u64 {
	let mut h = state ^ value.wrapping_mul(0x9E37_79B9_7F4A_7C15);
	h ^= h >> 29;
	h = h.wrapping_mul(0xBF58_476D_1CE4_E5B9);
	h ^= h >> 32;
	h
}
