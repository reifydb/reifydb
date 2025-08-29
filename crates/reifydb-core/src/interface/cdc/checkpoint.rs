// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	CowVec, Version,
	interface::{CommandTransaction, QueryTransaction, ToConsumerKey},
	row::EncodedRow,
};

pub struct CdcCheckpoint {}

impl CdcCheckpoint {
	pub fn fetch<K: ToConsumerKey>(
		txn: &mut impl QueryTransaction,
		consumer: &K,
	) -> crate::Result<Version> {
		let key = consumer.to_consumer_key();
		txn.get(&key)?
			.and_then(|record| {
				if record.row.len() >= 8 {
					let mut buffer = [0u8; 8];
					buffer.copy_from_slice(
						&record.row[0..8],
					);
					Some(u64::from_be_bytes(buffer))
				} else {
					None
				}
			})
			.map(Ok)
			.unwrap_or(Ok(1))
	}

	pub fn persist<K: ToConsumerKey>(
		txn: &mut impl CommandTransaction,
		consumer: &K,
		version: Version,
	) -> crate::Result<()> {
		let key = consumer.to_consumer_key();
		let version_bytes = version.to_be_bytes().to_vec();
		txn.set(&key, EncodedRow(CowVec::new(version_bytes)))
	}
}
