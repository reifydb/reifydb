// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	EncodedKey, Result, Version,
	interface::{UnversionedContains, VersionedContains},
};

use crate::lmdb::Lmdb;

impl VersionedContains for Lmdb {
	fn contains(
		&self,
		_key: &EncodedKey,
		_version: Version,
	) -> Result<bool> {
		todo!()
	}
}

impl UnversionedContains for Lmdb {
	fn contains(&self, _key: &EncodedKey) -> Result<bool> {
		todo!()
	}
}
