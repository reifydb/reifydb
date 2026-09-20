// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::fmt::{self, Display, Formatter};

use reifydb_store_log::error::LogError;

use crate::lsn::Lsn;

pub type Result<T> = std::result::Result<T, WalError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalError {
	Log(LogError),
	Decode {
		lsn: Lsn,
		kind: u32,
		reason: String,
	},
	FloorHeld(String),
	Partitions {
		found: u32,
	},
	SyncStopped,
}

impl From<LogError> for WalError {
	fn from(error: LogError) -> Self {
		Self::Log(error)
	}
}

impl Display for WalError {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		match self {
			Self::Log(error) => write!(f, "{error}"),
			Self::Decode {
				lsn,
				kind,
				reason,
			} => write!(f, "record {} of kind {kind} failed to decode: {reason}", lsn.as_u64()),
			Self::FloorHeld(name) => write!(f, "the floor {name} is already held"),
			Self::Partitions {
				found,
			} => write!(f, "the wal needs exactly one partition scan, found {found}"),
			Self::SyncStopped => write!(f, "the wal sync loop has stopped"),
		}
	}
}
