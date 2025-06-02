// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Key, Version};
use reifydb_core::AsyncCowVec;

pub type Value = AsyncCowVec<u8>;

pub struct StoredValue {
    pub key: Key,
    pub value: Value,
    pub version: Version,
}
