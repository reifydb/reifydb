// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::schema::RowSchema,
	key::{
		EncodableKey,
		system_version::{SystemVersion, SystemVersionKey},
	},
};
use reifydb_transaction::single::SingleTransaction;
use reifydb_type::value::r#type::Type;

const CURRENT_STORAGE_VERSION: u8 = 0x01;

/// Ensures the storage version key exists and matches the expected version.
/// On first boot, creates the version entry.
pub(crate) fn ensure_storage_version(single: &SingleTransaction) -> crate::Result<()> {
	let schema = RowSchema::testing(&[Type::Uint1]);
	let key = SystemVersionKey {
		version: SystemVersion::Storage,
	}
	.encode();

	let mut tx = single.begin_command([&key])?;

	match tx.get(&key)? {
		None => {
			let mut row = schema.allocate();
			schema.set_u8(&mut row, 0, CURRENT_STORAGE_VERSION);
			tx.set(&key, row)?;
		}
		Some(single) => {
			let version = schema.get_u8(&single.row, 0);
			assert_eq!(CURRENT_STORAGE_VERSION, version, "Storage version mismatch");
		}
	};

	tx.commit()?;

	Ok(())
}
