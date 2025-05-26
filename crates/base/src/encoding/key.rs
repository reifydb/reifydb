// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::encoding;
// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0
use crate::encoding::keycode;
use serde::{Deserialize, Serialize};

/// Adds automatic Keycode encode/decode methods to key enums. These are used
/// as keys in the key/value store.
pub trait Key<'de>: Serialize + Deserialize<'de> {
    /// Decodes a key from a byte slice using Keycode.
    fn decode(bytes: &'de [u8]) -> encoding::Result<Self> {
        keycode::deserialize(bytes)
    }

    /// Encodes a key to a byte vector using Keycode.
    ///
    /// In the common case, the encoded key is borrowed for a store engine
    /// call and then thrown away. We could avoid a bunch of allocations by
    /// taking a reusable byte vector to encode into and return a reference to
    /// it, but we keep it simple.
    fn encode(&self) -> Vec<u8> {
        keycode::serialize(self)
    }
}
