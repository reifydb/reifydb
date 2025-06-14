// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::VersionedContains;
use crate::lmdb::Lmdb;
use reifydb_core::{EncodedKey, Version};

impl VersionedContains for Lmdb {
    fn contains(&self, key: &EncodedKey, version: Version) -> bool {
        todo!()
    }
}
