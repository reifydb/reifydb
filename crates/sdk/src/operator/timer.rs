// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub use reifydb_abi::operator::timer::TimerKind;
use reifydb_value::value::datetime::DateTime;

pub struct Timer<'a> {
	pub at: DateTime,
	pub kind: TimerKind,
	pub key: &'a [u8],
}
