// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_persistence::Key;
use crate::{Get, StoredValue, Version};
use crate::lmdb::Lmdb;

impl Get for Lmdb{
	fn get(&self, key: &Key, version: Version) -> Option<StoredValue> {
		todo!()
	}
}