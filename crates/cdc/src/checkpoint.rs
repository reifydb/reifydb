// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{CommitVersion, CowVec, interface::ToConsumerKey, value::encoded::EncodedValues};
use reifydb_transaction::{IntoStandardTransaction, StandardCommandTransaction};

pub struct CdcCheckpoint {}

impl CdcCheckpoint {
	pub fn fetch<K: ToConsumerKey>(
		txn: &mut impl IntoStandardTransaction,
		consumer: &K,
	) -> reifydb_core::Result<CommitVersion> {
		let key = consumer.to_consumer_key();

		txn.get(&key)?
			.and_then(|multi| {
				if multi.values.len() >= 8 {
					let mut buffer = [0u8; 8];
					buffer.copy_from_slice(&multi.values[0..8]);
					Some(CommitVersion(u64::from_be_bytes(buffer)))
				} else {
					None
				}
			})
			.map(Ok)
			.unwrap_or(Ok(CommitVersion(1)))
	}

	pub fn persist<K: ToConsumerKey>(
		txn: &mut StandardCommandTransaction,
		consumer: &K,
		version: CommitVersion,
	) -> reifydb_core::Result<()> {
		let key = consumer.to_consumer_key();
		let version_bytes = version.0.to_be_bytes().to_vec();
		txn.set(&key, EncodedValues(CowVec::new(version_bytes)))
	}
}
