// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::EncodedKey;
use crate::sqlite::Sqlite;
use crate::Unversioned;
use crate::unversioned::UnversionedGet;

impl UnversionedGet for Sqlite{
	fn get(&self, key: &EncodedKey) -> Option<Unversioned> {
		todo!()
	}
}