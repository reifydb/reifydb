// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use crate::value::datetime::DateTime;

pub trait ClockNow {
	fn now(&self) -> DateTime;
}

pub trait RandomBytes {
	fn bytes_10(&self) -> [u8; 10];
}

#[cfg(test)]
pub(crate) mod testing {
	use std::{cell::Cell, rc::Rc};

	use crate::{
		clock::{ClockNow, RandomBytes},
		value::datetime::DateTime,
	};

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
		fn now(&self) -> DateTime {
			DateTime::from_nanos(self.nanos.get())
		}
	}

	pub struct TestRng;

	impl RandomBytes for TestRng {
		fn bytes_10(&self) -> [u8; 10] {
			[0; 10]
		}
	}
}
