// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, encoded::row::EncodedRow, key::cdc_consumer::ToConsumerKey};
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::{Result, util::cowvec::CowVec};

pub struct CdcCheckpoint {}

impl CdcCheckpoint {
	pub fn fetch<K: ToConsumerKey>(txn: &mut Transaction<'_>, consumer: &K) -> Result<CommitVersion> {
		let key = consumer.to_consumer_key();

		txn.get(&key)?
			.and_then(|multi| {
				if multi.row.len() >= 8 {
					let mut buffer = [0u8; 8];
					buffer.copy_from_slice(&multi.row[0..8]);
					Some(CommitVersion(u64::from_be_bytes(buffer)))
				} else {
					None
				}
			})
			.map(Ok)
			.unwrap_or(Ok(CommitVersion(1)))
	}

	pub fn fetch_opt<K: ToConsumerKey>(txn: &mut Transaction<'_>, consumer: &K) -> Result<Option<CommitVersion>> {
		let key = consumer.to_consumer_key();

		Ok(txn.get(&key)?.and_then(|multi| {
			if multi.row.len() >= 8 {
				let mut buffer = [0u8; 8];
				buffer.copy_from_slice(&multi.row[0..8]);
				Some(CommitVersion(u64::from_be_bytes(buffer)))
			} else {
				None
			}
		}))
	}

	pub fn persist<K: ToConsumerKey>(
		txn: &mut CommandTransaction,
		consumer: &K,
		version: CommitVersion,
	) -> Result<()> {
		let key = consumer.to_consumer_key();
		let version_bytes = version.0.to_be_bytes().to_vec();
		txn.set(&key, EncodedRow(CowVec::new(version_bytes)))
	}

	pub fn delete<K: ToConsumerKey>(txn: &mut CommandTransaction, consumer: &K) -> Result<()> {
		let key = consumer.to_consumer_key();
		txn.remove(&key)
	}
}
