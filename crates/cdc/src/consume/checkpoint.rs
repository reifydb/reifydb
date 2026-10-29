// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{CheckpointState, ConsumerClass},
	key::cdc::ToConsumerKey,
};
use reifydb_transaction::transaction::command::CommandTransaction;
use reifydb_value::{Result, util::cowvec::CowVec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointRow {
	pub version: CommitVersion,
	pub class: ConsumerClass,
	pub state: CheckpointState,
}

impl CheckpointRow {
	pub fn decode(row: &[u8]) -> Option<Self> {
		if row.len() < 10 {
			return None;
		}
		let mut buffer = [0u8; 8];
		buffer.copy_from_slice(&row[0..8]);
		Some(Self {
			version: CommitVersion(u64::from_be_bytes(buffer)),
			class: ConsumerClass::decode(row[8])?,
			state: CheckpointState::decode(row[9])?,
		})
	}

	fn encode(&self) -> EncodedBytes {
		let mut bytes = Vec::with_capacity(10);
		bytes.extend_from_slice(&self.version.0.to_be_bytes());
		bytes.push(self.class.encode());
		bytes.push(self.state.encode());
		EncodedBytes(CowVec::new(bytes))
	}
}

pub struct CdcCheckpoint {}

impl CdcCheckpoint {
	pub fn persist<K: ToConsumerKey>(
		txn: &mut CommandTransaction,
		consumer: &K,
		version: CommitVersion,
		class: ConsumerClass,
	) -> Result<()> {
		let key = consumer.to_consumer_key();
		let row = CheckpointRow {
			version,
			class,
			state: CheckpointState::Valid,
		};
		txn.set(vec![(key.clone().into(), row.encode())])
	}
}
