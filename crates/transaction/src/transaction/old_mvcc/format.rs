// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::transaction::old_mvcc;
pub use reifydb_core::encoding::format::{Formatter, Raw};
use reifydb_core::encoding::{Key, bincode};
use std::collections::BTreeSet;
use std::marker::PhantomData;

/// Formats MVCC keys/values. Dispatches to F to format the inner key/value.
pub struct MVCC<F: Formatter>(PhantomData<F>);

impl<F: Formatter> Formatter for MVCC<F> {
    fn key(key: &[u8]) -> String {
        let Ok(key) = old_mvcc::Key::decode(key) else {
            return Raw::key(key); // invalid key
        };
        match key {
            old_mvcc::Key::TxWrite(version, innerkey) => {
                format!("mvcc:TxWrite({version}, {})", F::key(&innerkey))
            }
            old_mvcc::Key::Version(innerkey, version) => {
                format!("mvcc:Version({}, {version})", F::key(&innerkey))
            }
            old_mvcc::Key::Unversioned(innerkey) => {
                format!("mvcc:Unversioned({})", F::key(&innerkey))
            }
            old_mvcc::Key::NextVersion | old_mvcc::Key::TxActive(_) | old_mvcc::Key::TxActiveSnapshot(_) => {
                format!("mvcc:{key:?}")
            }
        }
    }

    fn value(key: &[u8], value: &[u8]) -> String {
        let Ok(key) = old_mvcc::Key::decode(key) else {
            return Raw::bytes(value); // invalid key
        };
        match key {
            old_mvcc::Key::NextVersion => {
                let Ok(version) = bincode::deserialize::<old_mvcc::Version>(value) else {
                    return Raw::bytes(value);
                };
                version.to_string()
            }
            old_mvcc::Key::TxActiveSnapshot(_) => {
                let Ok(active) = bincode::deserialize::<BTreeSet<u64>>(value) else {
                    return Raw::bytes(value);
                };
                format!(
                    "{{{}}}",
                    active.iter().map(ToString::to_string).collect::<Vec<_>>().join(",")
                )
            }
            old_mvcc::Key::TxActive(_) | old_mvcc::Key::TxWrite(_, _) => Raw::bytes(value),
            old_mvcc::Key::Version(userkey, _) => match bincode::deserialize(value) {
                Ok(Some(value)) => F::value(&userkey, value),
                Ok(None) => "None".to_string(),
                Err(_) => Raw::bytes(value),
            },
            old_mvcc::Key::Unversioned(userkey) => F::value(&userkey, value),
        }
    }
}
