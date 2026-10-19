// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error,
	fmt::{self, Display, Formatter},
};

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
	FloorAhead {
		floor: String,
		requested: Lsn,
		durable: Option<Lsn>,
	},
	Partitions {
		found: u32,
	},
	Purged {
		requested: Lsn,
		oldest: Lsn,
	},
	SyncStopped,
	Body(String),
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
			Self::FloorAhead {
				floor,
				requested,
				durable,
			} => write!(
				f,
				"the floor {floor} cannot move to {}, past the durable lsn {:?}",
				requested.as_u64(),
				durable.map(Lsn::as_u64)
			),
			Self::Partitions {
				found,
			} => write!(f, "the wal needs exactly one partition scan, found {found}"),
			Self::Purged {
				requested,
				oldest,
			} => write!(
				f,
				"a replay from {} is below the oldest record the log still keeps, {}",
				requested.as_u64(),
				oldest.as_u64()
			),
			Self::SyncStopped => write!(f, "the wal sync loop has stopped"),
			Self::Body(reason) => write!(f, "{reason}"),
		}
	}
}

impl Error for WalError {}
