// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod join;
pub mod timer;

pub mod horizon {
	use reifydb_value::value::datetime::DateTime;

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	pub struct Cutoff(pub DateTime);

	impl Cutoff {
		pub fn instant(&self) -> DateTime {
			self.0
		}

		pub fn raw(&self) -> u64 {
			self.0.to_nanos()
		}
	}
}
