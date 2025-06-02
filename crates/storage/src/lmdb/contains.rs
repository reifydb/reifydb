// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_persistence::Key;
use crate::{Contains, Version};
use crate::lmdb::Lmdb;

impl Contains for Lmdb{
	fn contains(&self, key: &Key, version: Version) -> bool {
		todo!()
	}
}