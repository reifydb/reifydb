// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::{Get, Key, StoredValue, Version};

impl Get for Lmdb {
    fn get(&self, key: &Key, version: Version) -> Option<StoredValue> {
        todo!()
    }
}
