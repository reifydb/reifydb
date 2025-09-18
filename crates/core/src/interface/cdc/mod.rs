// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod storage;

use serde::{Deserialize, Serialize};
pub use storage::{CdcCount, CdcGet, CdcRange, CdcScan, CdcStorage};

use crate::{CommitVersion, EncodedKey, interface::transaction::TransactionId, row::EncodedRow};

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

impl AsRef<str> for ConsumerId {
	fn as_ref(&self) -> &str {
		&self.0
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CdcChange {
	Insert {
		key: EncodedKey,
		post: EncodedRow,
	},
	Update {
		key: EncodedKey,
		pre: EncodedRow,
		post: EncodedRow,
	},
	Delete {
		key: EncodedKey,
		pre: Option<EncodedRow>,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct CdcEvent {
	pub version: CommitVersion,
	pub sequence: u16,
	pub timestamp: u64,
	pub transaction: TransactionId,
	pub change: CdcChange,
}

impl CdcEvent {
	pub fn new(
		version: CommitVersion,
		sequence: u16,
		timestamp: u64,
		transaction: TransactionId,
		change: CdcChange,
	) -> Self {
		Self {
			version,
			sequence,
			timestamp,
			transaction,
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
