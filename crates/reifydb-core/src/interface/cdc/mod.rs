// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod consumer;
mod storage;

pub use consumer::{CdcConsume, CdcConsumer};
use serde::{Deserialize, Serialize};
pub use storage::{CdcCount, CdcGet, CdcRange, CdcScan, CdcStorage};

use crate::{EncodedKey, Version, row::EncodedRow};

#[repr(transparent)]
#[derive(Debug, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct ConsumerId(pub(crate) String);

impl ConsumerId {
	pub fn new(id: impl Into<String>) -> Self {
		let id = id.into();
		assert_ne!(id, "__FLOW_CONSUMER");
		Self(id)
	}

	pub fn flow_consumer() -> Self {
		Self("__FLOW_CONSUMER".to_string())
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CdcChange {
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
	pub change: CdcChange,
}

impl CdcEvent {
	pub fn new(
		version: Version,
		sequence: u16,
		timestamp: u64,
		change: CdcChange,
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
			CdcChange::Insert {
				key,
				..
			} => key,
			CdcChange::Update {
				key,
				..
			} => key,
			CdcChange::Delete {
				key,
				..
			} => key,
		}
	}
}
