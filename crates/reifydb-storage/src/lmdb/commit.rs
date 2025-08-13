// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	CowVec, Result, Version,
	delta::Delta,
	interface::{UnversionedCommit, VersionedCommit},
};

use crate::lmdb::Lmdb;

impl VersionedCommit for Lmdb {
	fn commit(
		&self,
		delta: CowVec<Delta>,
		_version: Version,
	) -> Result<()> {
		let mut tx = self.env.write_txn().unwrap();
		for delta in delta {
			match delta {
				Delta::Set {
					key,
					row,
				} => {
					self.db.put(&mut tx, &key[..], &row)
						.unwrap();
				}
				Delta::Remove {
					key,
				} => {
					self.db.delete(&mut tx, &key[..])
						.unwrap();
				}
			}
		}
		tx.commit().unwrap();
		Ok(())
	}
}

impl UnversionedCommit for Lmdb {
	fn commit(&mut self, delta: CowVec<Delta>) -> Result<()> {
		let mut tx = self.env.write_txn().unwrap();
		for delta in delta {
			match delta {
				Delta::Set {
					key,
					row,
				} => {
					self.db.put(&mut tx, &key[..], &row)
						.unwrap();
				}
				Delta::Remove {
					key,
				} => {
					self.db.delete(&mut tx, &key[..])
						.unwrap();
				}
			}
		}
		tx.commit().unwrap();
		Ok(())
	}
}
