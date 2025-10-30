// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

use crate::{CommitVersion, EncodedKey, value::encoded::EncodedValues};

#[repr(transparent)]
#[derive(Debug, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct CdcConsumerId(pub(crate) String);

impl CdcConsumerId {
	pub fn new(id: impl Into<String>) -> Self {
		let id = id.into();
		assert_ne!(id, "__FLOW_CONSUMER");
		Self(id)
	}

	pub fn flow_consumer() -> Self {
		Self("__FLOW_CONSUMER".to_string())
	}
}

impl AsRef<str> for CdcConsumerId {
	fn as_ref(&self) -> &str {
		&self.0
	}
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CdcChange {
	Insert {
		key: EncodedKey,
		post: EncodedValues,
	},
	Update {
		key: EncodedKey,
		pre: EncodedValues,
		post: EncodedValues,
	},
	Delete {
		key: EncodedKey,
		pre: Option<EncodedValues>,
	},
}

/// Structure for storing CDC data with shared metadata
#[derive(Debug, Clone, PartialEq)]
pub struct Cdc {
	pub version: CommitVersion,
	pub timestamp: u64,
	pub changes: Vec<CdcSequencedChange>,
}

impl Cdc {
	pub fn new(version: CommitVersion, timestamp: u64, changes: Vec<CdcSequencedChange>) -> Self {
		Self {
			version,
			timestamp,
			changes,
		}
	}
}

/// Structure for individual changes within a transaction
#[derive(Debug, Clone, PartialEq)]
pub struct CdcSequencedChange {
	pub sequence: u16,
	pub change: CdcChange,
}

impl CdcSequencedChange {
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
