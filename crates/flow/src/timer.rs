// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::state::store::TimerKind;
use reifydb_value::value::datetime::DateTime;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timer {
	pub at: DateTime,
	pub kind: TimerKind,
	pub key: EncodedKey,
}
