// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod consumer;
mod storage;

pub use consumer::{CdcConsume, CdcConsumer};
use serde::{Deserialize, Serialize};
pub use storage::{CdcCount, CdcGet, CdcRange, CdcScan, CdcStorage};

use crate::{EncodedKey, Version, row::EncodedRow};

#[repr(transparent)]
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ConsumerId(pub u64);

impl ConsumerId {
	pub const FLOW_CONSUMER: ConsumerId = ConsumerId(1);
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Change {
	Insert {
		key: EncodedKey,
		after: EncodedRow,
	},
	Update {
		key: EncodedKey,
		before: EncodedRow,
		after: EncodedRow,
	},
	Delete {
		key: EncodedKey,
		before: EncodedRow,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct CdcEvent {
	pub version: Version,
	pub sequence: u16,
	pub timestamp: u64,
	pub change: Change,
}

impl CdcEvent {
	pub fn new(
		version: Version,
		sequence: u16,
		timestamp: u64,
		change: Change,
	) -> Self {
		Self {
			version,
			sequence,
			timestamp,
			change,
		}
	}
	pub fn key(&self) -> &EncodedKey {
		match &self.change {
			Change::Insert {
				key,
				..
			} => key,
			Change::Update {
				key,
				..
			} => key,
			Change::Delete {
				key,
				..
			} => key,
		}
	}
}
