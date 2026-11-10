// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	error::Error,
	fmt::{self, Display, Formatter},
};

use reifydb_store_log::error::LogError;
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

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
	NonPositiveInterval(Duration),
	Body(String),
	SegmentBytes {
		configured: ByteSize,
		found: ByteSize,
	},
	SegmentTooSmall {
		segment_bytes: ByteSize,
		header: usize,
	},
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
			Self::NonPositiveInterval(interval) => {
				write!(f, "the wal sync interval must be greater than zero, found {interval}")
			}
			Self::Body(reason) => write!(f, "{reason}"),
			Self::SegmentBytes {
				configured,
				found,
			} => write!(
				f,
				"the log was created with {found} segments, the wal config asks for {configured}"
			),
			Self::SegmentTooSmall {
				segment_bytes,
				header,
			} => write!(
				f,
				"a wal segment of {segment_bytes} cannot hold one record header of {header} bytes"
			),
		}
	}
}

impl Error for WalError {}
