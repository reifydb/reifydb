// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/erikgrinaker/toydb (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::{BTreeSet, HashSet};
use std::hash::Hash;
use std::io::{Read, Write};

/// Adds automatic Bincode encode-decode methods to value types. These are used
/// for values in key-value storage engines, and also for e.g. network protocol
/// messages and other values.
pub trait Value: Serialize + DeserializeOwned {
    /// Decodes a value from a byte slice using Bincode.
    fn decode(bytes: &[u8]) -> crate::encoding::Result<Self> {
        crate::encoding::bincode::deserialize(bytes)
    }

    /// Decodes a value from a reader using Bincode.
    fn decode_from<R: Read>(reader: R) -> crate::encoding::Result<Self> {
        crate::encoding::bincode::deserialize_from(reader)
    }

    /// Decodes a value from a reader using Bincode, or returns None if the
    /// reader is closed.
    fn maybe_decode_from<R: Read>(reader: R) -> crate::encoding::Result<Option<Self>> {
        crate::encoding::bincode::maybe_deserialize_from(reader)
    }

    /// Encodes a value to a byte vector using Bincode.
    fn encode(&self) -> Vec<u8> {
        crate::encoding::bincode::serialize(self)
    }

    /// Encodes a value into a writer using Bincode.
    fn encode_into<W: Write>(&self, writer: W) -> crate::encoding::Result<()> {
        crate::encoding::bincode::serialize_into(writer, self)
    }
}

/// Blanket implementations for various types wrapping a value type.
impl<V: Value> Value for Option<V> {}
impl<V: Value> Value for crate::encoding::Result<V> {}
impl<V: Value> Value for Vec<V> {}
impl<V1: Value, V2: Value> Value for (V1, V2) {}
impl<V: Value + Eq + Hash> Value for HashSet<V> {}
impl<V: Value + Eq + Ord + Hash> Value for BTreeSet<V> {}
