// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::state::store::TimerKind;
use reifydb_value::value::datetime::DateTime;

pub struct Timer<'a> {
	pub at: DateTime,
	pub kind: TimerKind,
	pub key: &'a [u8],
}
