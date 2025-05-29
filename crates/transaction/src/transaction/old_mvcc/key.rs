// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::transaction::old_mvcc::Version;
use reifydb_core::encoding;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::{Display, Formatter};

/// MVCC keys, using the Keycode encoding which preserves the ordering and
/// grouping of keys.
///
/// Cow byte slices allow encoding borrowed values and decoding owned values.
#[derive(Debug, Deserialize, Serialize)]
pub enum Key<'a> {
    /// The next available version.
    NextVersion,
    /// Active (uncommitted) transactions by version.
    TxActive(Version),
    /// A snapshot of the active set at each version. Only written for
    /// versions where the active set is non-empty (excluding itself).
    TxActiveSnapshot(Version),
    /// Keeps track of all keys written to by an active transaction (identified
    /// by its version), in case it needs to roll back.
    TxWrite(
        Version,
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    /// A versioned key-value pair.
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
        Version,
    ),
    /// Unversioned non-transactional key-value pairs, mostly used for metadata.
    /// These exist separately from versioned keys, i.e. the unversioned key
    /// "foo" is entirely independent of the versioned key "foo@7".
    Unversioned(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
}

impl Display for Key<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Key::NextVersion => f.write_str("NextVersion"),
            Key::TxActive(_) => f.write_str("TxActive"),
            Key::TxActiveSnapshot(_) => f.write_str("TxActiveSnapshot"),
            Key::TxWrite(_, _) => f.write_str("TxWrite"),
            Key::Version(_, _) => f.write_str("Version"),
            Key::Unversioned(_) => f.write_str("Unversioned"),
        }
    }
}

impl<'a> encoding::Key<'a> for Key<'a> {}

/// MVCC key prefixes, for prefix scans. These must match the keys above,
/// including the enum variant index.
#[derive(Debug, Deserialize, Serialize)]
pub enum KeyPrefix<'a> {
    NextVersion,
    TxActive,
    TxActiveSnapshot,
    TxWrite(Version),
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    Unversioned,
}

impl<'a> encoding::Key<'a> for KeyPrefix<'a> {}

#[cfg(test)]
mod tests {
    use crate::transaction::old_mvcc::{Key, KeyPrefix, Version};
    use reifydb_core::encoding::Key as _;

    #[test]
    fn key_prefix_next_version() {
        let prefix = KeyPrefix::NextVersion.encode();
        let key = Key::NextVersion.encode();
        assert_eq!(prefix, key[..prefix.len()]);
    }

    #[test]
    fn key_prefix_txn_active() {
        let prefix = KeyPrefix::TxActive.encode();
        let key = Key::TxActive(Version(1)).encode();
        assert_eq!(prefix, key[..prefix.len()]);
    }

    #[test]
    fn key_prefix_txn_active_snapshot() {
        let prefix = KeyPrefix::TxActiveSnapshot.encode();
        let key = Key::TxActiveSnapshot(Version(1)).encode();
        assert_eq!(prefix, key[..prefix.len()]);
    }

    #[test]
    fn key_prefix_txn_write() {
        let prefix = KeyPrefix::TxWrite(Version(1)).encode();
        let key = Key::TxWrite(Version(1), b"foo".as_slice().into()).encode();
        assert_eq!(prefix, key[..prefix.len()]);
    }

    #[test]
    fn key_prefix_version() {
        let prefix = KeyPrefix::Version(b"foo".as_slice().into()).encode();
        let key = Key::Version(b"foo".as_slice().into(), Version(1)).encode();
        assert_eq!(prefix, key[..prefix.len()]);
    }

    #[test]
    fn key_prefix_unversioned() {
        let prefix = KeyPrefix::Unversioned.encode();
        let key = Key::Unversioned(b"foo".as_slice().into()).encode();
        assert_eq!(prefix, key[..prefix.len()]);
    }
}
