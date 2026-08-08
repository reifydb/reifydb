// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	common::CommitVersion,
	interface::cdc::{CheckpointState, ConsumerClass},
	key::cdc_consumer::ToConsumerKey,
};
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
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
	pub fn fetch<K: ToConsumerKey>(txn: &mut Transaction<'_>, consumer: &K) -> Result<CommitVersion> {
		Ok(Self::fetch_opt(txn, consumer)?.unwrap_or(CommitVersion(1)))
	}

	pub fn fetch_opt<K: ToConsumerKey>(txn: &mut Transaction<'_>, consumer: &K) -> Result<Option<CommitVersion>> {
		Ok(Self::fetch_row(txn, consumer)?.map(|row| row.version))
	}

	pub fn fetch_row<K: ToConsumerKey>(txn: &mut Transaction<'_>, consumer: &K) -> Result<Option<CheckpointRow>> {
		let key = consumer.to_consumer_key();
		Ok(txn.get(&key)?.and_then(|multi| CheckpointRow::decode(&multi.bytes)))
	}

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
		txn.set(&key, row.encode())
	}

	pub fn invalidate<K: ToConsumerKey>(txn: &mut CommandTransaction, consumer: &K) -> Result<()> {
		let key = consumer.to_consumer_key();
		let Some(multi) = txn.get(&key)? else {
			return Ok(());
		};
		let Some(mut bytes) = CheckpointRow::decode(&multi.bytes) else {
			return Ok(());
		};
		assert_ne!(
			bytes.class,
			ConsumerClass::Pinning,
			"a Pinning consumer checkpoint can never be invalidated: retention must never overtake it"
		);
		bytes.state = CheckpointState::Invalidated;
		txn.set(&key, bytes.encode())
	}

	pub fn delete<K: ToConsumerKey>(txn: &mut CommandTransaction, consumer: &K) -> Result<()> {
		let key = consumer.to_consumer_key();
		txn.remove(&key)
	}
}
