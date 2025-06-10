// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use reifydb_core::delta::Bytes;
use reifydb_core::{Key, Version};

pub struct Stored {
    pub key: Key,
    pub bytes: Bytes,
    pub version: Version,
}
