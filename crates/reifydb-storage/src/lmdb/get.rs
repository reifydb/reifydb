// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, EncodedKey, Result, Version,
	interface::{Unversioned, UnversionedGet, Versioned, VersionedGet},
	row::EncodedRow,
};

use crate::lmdb::Lmdb;

impl VersionedGet for Lmdb {
	fn get(
		&self,
		key: &EncodedKey,
		version: Version,
	) -> Result<Option<Versioned>> {
		let txn = self.env.read_txn().unwrap(); // FIXME
		Ok(self.db.get(&txn, key).unwrap().map(|bytes| Versioned {
			key: key.clone(),
			row: EncodedRow(CowVec::new(bytes.to_vec())),
			version,
		}))
	}
}

impl UnversionedGet for Lmdb {
	fn get(&self, key: &EncodedKey) -> Result<Option<Unversioned>> {
		let txn = self.env.read_txn().unwrap(); // FIXME
		Ok(self.db.get(&txn, key).unwrap().map(|bytes| Unversioned {
			key: key.clone(),
			row: EncodedRow(CowVec::new(bytes.to_vec())),
		}))
	}
}
