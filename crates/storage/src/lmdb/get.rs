// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::lmdb::Lmdb;
use crate::{Get, Stored};
use reifydb_core::{Key, Version};

impl Get for Lmdb {
    fn get(&self, key: &Key, version: Version) -> Option<Stored> {
        todo!()
    }
}
