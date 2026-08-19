// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{interface::catalog::flow::OperatorId, state::timer::TimerKind};
use reifydb_value::value::datetime::DateTime;

#[cfg(feature = "runtime")]
pub mod extension;
#[cfg(feature = "runtime")]
pub mod registry;
#[cfg(feature = "runtime")]
pub mod wheel;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timer {
	pub due: DateTime,
	pub kind: TimerKind,
	pub key: EncodedKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TimerDue {
	pub operator_id: OperatorId,
	pub due: DateTime,
}
