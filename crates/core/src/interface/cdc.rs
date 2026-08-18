// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_value::value::datetime::DateTime;
use serde::{Deserialize, Serialize};

use crate::common::CommitVersion;

#[repr(transparent)]
#[derive(Debug, Clone, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct CdcConsumerId(pub(crate) String);

impl CdcConsumerId {
	const FLOW: &'static str = "__FLOW_COORDINATOR";
	const SUBSCRIPTION: &'static str = "__SUBSCRIPTION_CONSUMER";

	pub fn new(id: impl Into<String>) -> Self {
		let id = id.into();
		assert_ne!(id, Self::FLOW);
		assert_ne!(id, Self::SUBSCRIPTION);
		Self(id)
	}

	pub fn flow_consumer() -> Self {
		Self(Self::FLOW.to_string())
	}

	pub fn subscription_consumer() -> Self {
		Self(Self::SUBSCRIPTION.to_string())
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerClass {
	Pinning,
	Ephemeral,
}

impl ConsumerClass {
	pub fn encode(self) -> u8 {
		match self {
			Self::Pinning => 0,
			Self::Ephemeral => 1,
		}
	}

	pub fn decode(byte: u8) -> Option<Self> {
		match byte {
			0 => Some(Self::Pinning),
			1 => Some(Self::Ephemeral),
			_ => None,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointState {
	Valid,
	Invalidated,
}

impl CheckpointState {
	pub fn encode(self) -> u8 {
		match self {
			Self::Valid => 0,
			Self::Invalidated => 1,
		}
	}

	pub fn decode(byte: u8) -> Option<Self> {
		match byte {
			0 => Some(Self::Valid),
			1 => Some(Self::Invalidated),
			_ => None,
		}
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
		post: EncodedBytes,
	},
	Update {
		key: EncodedKey,
		pre: EncodedBytes,
		post: EncodedBytes,
	},
	Delete {
		key: EncodedKey,
		pre: Option<EncodedBytes>,
		visible: bool,
	},
}

impl CdcChange {
	pub fn key(&self) -> &EncodedKey {
		match self {
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

	pub fn value_bytes(&self) -> usize {
		match self {
			CdcChange::Insert {
				post,
				..
			} => post.len(),
			CdcChange::Update {
				pre,
				post,
				..
			} => pre.len() + post.len(),
			CdcChange::Delete {
				pre,
				..
			} => pre.as_ref().map(|p| p.len()).unwrap_or(0),
		}
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cdc {
	pub version: CommitVersion,
	pub timestamp: DateTime,

	pub changes: Vec<CdcChange>,
}

impl Cdc {
	pub fn new(version: CommitVersion, timestamp: DateTime, changes: Vec<CdcChange>) -> Self {
		Self {
			version,
			timestamp,
			changes,
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerState {
	pub consumer_id: CdcConsumerId,
	pub checkpoint: CommitVersion,
}

#[derive(Debug, Clone)]
pub struct CdcBatch {
	pub items: Vec<Cdc>,

	pub has_more: bool,
}

impl CdcBatch {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}
