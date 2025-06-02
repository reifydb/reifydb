// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::{Contains, Key, Version};

impl Contains for Lmdb {
    fn contains(&self, key: &Key, version: Version) -> bool {
        todo!()
    }
}
