// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Key, Value};
use std::cmp;

/// Operation on a key-value pair.
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Delta {
    Set { key: Key, value: Value },
    Remove { key: Key },
}

impl PartialOrd for Delta {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Delta {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.key().cmp(other.key())
    }
}

impl Delta {
    /// Returns the key
    pub fn key(&self) -> &Key {
        match self {
            Self::Set { key, .. } => key,
            Self::Remove { key } => key,
        }
    }

    /// Returns the value, if None, it means the entry is marked as remove.
    pub fn value(&self) -> Option<&Value> {
        match self {
            Self::Set { value, .. } => Some(value),
            Self::Remove { .. } => None,
        }
    }
}

impl Clone for Delta {
    fn clone(&self) -> Self {
        match self {
            Self::Set { key, value } => Self::Set { key: key.clone(), value: value.clone() },
            Self::Remove { key } => Self::Remove { key: key.clone() },
        }
    }
}
