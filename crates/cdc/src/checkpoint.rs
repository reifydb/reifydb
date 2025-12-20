// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CommitVersion, CowVec,
	interface::{CommandTransaction, QueryTransaction, ToConsumerKey},
	value::encoded::EncodedValues,
};

pub struct CdcCheckpoint {}

impl CdcCheckpoint {
	pub fn fetch<K: ToConsumerKey>(
		txn: &mut impl QueryTransaction,
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
		txn: &mut impl CommandTransaction,
		consumer: &K,
		version: CommitVersion,
	) -> reifydb_core::Result<()> {
		let key = consumer.to_consumer_key();
		let version_bytes = version.0.to_be_bytes().to_vec();
		txn.set(&key, EncodedValues(CowVec::new(version_bytes)))
	}
}
