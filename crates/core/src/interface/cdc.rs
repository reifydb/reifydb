// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::value::datetime::DateTime;
use serde::{Deserialize, Serialize};

use crate::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::change::Change,
};

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

/// Internal system/metadata change (flow registrations, catalog ops, etc.)
/// Kept in encoded form for key-level inspection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SystemChange {
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

impl SystemChange {
	/// Get the key for this change.
	pub fn key(&self) -> &EncodedKey {
		match self {
			SystemChange::Insert {
				key,
				..
			} => key,
			SystemChange::Update {
				key,
				..
			} => key,
			SystemChange::Delete {
				key,
				..
			} => key,
		}
	}

	/// Calculate the approximate value bytes for this change (pre + post values).
	pub fn value_bytes(&self) -> usize {
		match self {
			SystemChange::Insert {
				post,
				..
			} => post.len(),
			SystemChange::Update {
				pre,
				post,
				..
			} => pre.len() + post.len(),
			SystemChange::Delete {
				pre,
				..
			} => pre.as_ref().map(|p| p.len()).unwrap_or(0),
		}
	}
}

/// Structure for storing CDC data with shared metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cdc {
	pub version: CommitVersion,
	pub timestamp: DateTime,
	/// Row-data changes in columnar format
	pub changes: Vec<Change>,
	/// Internal system changes
	pub system_changes: Vec<SystemChange>,
}

impl Cdc {
	pub fn new(
		version: CommitVersion,
		timestamp: DateTime,
		changes: Vec<Change>,
		system_changes: Vec<SystemChange>,
	) -> Self {
		Self {
			version,
			timestamp,
			changes,
			system_changes,
		}
	}
}

/// Represents the state of a CDC consumer
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerState {
	pub consumer_id: CdcConsumerId,
	pub checkpoint: CommitVersion,
}

/// A batch of CDC entries with continuation info.
#[derive(Debug, Clone)]
pub struct CdcBatch {
	/// The CDC entries in this batch.
	pub items: Vec<Cdc>,
	/// Whether there are more items after this batch.
	pub has_more: bool,
}

impl CdcBatch {
	/// Creates an empty batch with no more results.
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}

	/// Returns true if this batch contains no items.
	pub fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
}
