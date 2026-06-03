// SPDX-License-Identifier: MIT
// Copyright (c) 2026 ReifyDB

pub trait ClockNow {
	fn now_nanos(&self) -> u64;

	fn now_millis(&self) -> u64;
}

pub trait RandomBytes {
	fn bytes_10(&self) -> [u8; 10];
}

#[cfg(test)]
pub(crate) mod testing {
	use std::{cell::Cell, rc::Rc};

	use super::{ClockNow, RandomBytes};

	#[derive(Clone)]
	pub struct TestClock {
		nanos: Rc<Cell<u64>>,
	}

	impl TestClock {
		pub fn from_millis(millis: u64) -> Self {
			Self {
				nanos: Rc::new(Cell::new(millis * 1_000_000)),
			}
		}

		pub fn advance_millis(&self, millis: u64) {
			self.nanos.set(self.nanos.get() + millis * 1_000_000);
		}
	}

	impl ClockNow for TestClock {
		fn now_nanos(&self) -> u64 {
			self.nanos.get()
		}

		fn now_millis(&self) -> u64 {
			self.nanos.get() / 1_000_000
		}
	}

	pub struct TestRng;

	impl RandomBytes for TestRng {
		fn bytes_10(&self) -> [u8; 10] {
			[0; 10]
		}
	}
}
