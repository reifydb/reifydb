// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::Version;
use core::cmp::{self, Reverse};
use reifydb_persistence::{Action, Key, Value};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TransactionAction {
    pub action: Action,
    pub version: Version,
}

impl PartialOrd for TransactionAction {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TransactionAction {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.action
            .key()
            .cmp(other.action.key())
            .then_with(|| Reverse(self.version).cmp(&Reverse(other.version)))
    }
}

impl Clone for TransactionAction {
    fn clone(&self) -> Self {
        Self { version: self.version, action: self.action.clone() }
    }
}

impl TransactionAction {
    pub fn action(&self) -> &Action {
        &self.action
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn into_components(self) -> (u64, Action) {
        (self.version, self.action)
    }

    pub fn key(&self) -> &Key {
        &self.action.key()
    }

    pub fn value(&self) -> Option<&Value> {
        self.action.value()
    }

    pub fn split(self) -> (Key, TransactionValue) {
        let TransactionAction { action: data, version } = self;

        let (key, value) = match data {
            Action::Set { key, value } => (key, Some(value)),
            Action::Remove { key } => (key, None),
        };
        (key, TransactionValue { value, version })
    }

    pub fn unsplit(key: Key, value: TransactionValue) -> Self {
        let TransactionValue { value, version } = value;
        TransactionAction {
            action: match value {
                Some(value) => Action::Set { key, value },
                None => Action::Remove { key },
            },
            version,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TransactionValue {
    pub version: Version,
    pub value: Option<Value>,
}

impl Clone for TransactionValue {
    fn clone(&self) -> Self {
        Self { version: self.version, value: self.value.clone() }
    }
}
