// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::internal_error;
use serde::{Deserialize, Serialize};

use super::{CdcConsumerKeyRange, EncodableKey, MultiVersionQueryTransaction as QueryTransaction};
use crate::{CommitVersion, EncodedKey, key::CdcConsumerKey, value::encoded::EncodedValues};

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

/// Represents the state of a CDC consumer
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerState {
	pub consumer_id: CdcConsumerId,
	pub checkpoint: CommitVersion,
}

/// Retrieves the state of all CDC consumers
pub async fn get_all_consumer_states<T: QueryTransaction>(txn: &mut T) -> reifydb_type::Result<Vec<ConsumerState>> {
	let mut states = Vec::new();

	let batch = txn.range(CdcConsumerKeyRange::full_scan()).await?;
	for multi in batch.items {
		let key = CdcConsumerKey::decode(&multi.key)
			.ok_or_else(|| internal_error!("Unable to decode CdConsumerKey"))?;

		if multi.values.len() >= 8 {
			let mut buffer = [0u8; 8];
			buffer.copy_from_slice(&multi.values[0..8]);
			let checkpoint = CommitVersion(u64::from_be_bytes(buffer));

			states.push(ConsumerState {
				consumer_id: key.consumer,
				checkpoint,
			});
		}
	}

	Ok(states)
}
