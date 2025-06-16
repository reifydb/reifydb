// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::key::EncodedKey;
use crate::row::EncodedRow;
use std::cmp;

#[derive(Debug, PartialEq, Eq)]
pub enum Delta {
    Set { key: EncodedKey, row: EncodedRow },
    Remove { key: EncodedKey },
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
    pub fn key(&self) -> &EncodedKey {
        match self {
            Self::Set { key, .. } => key,
            Self::Remove { key } => key,
        }
    }

    /// Returns the row, if None, it means the entry is marked as remove.
    pub fn row(&self) -> Option<&EncodedRow> {
        match self {
            Self::Set { row, .. } => Some(row),
            Self::Remove { .. } => None,
        }
    }
}

impl Clone for Delta {
    fn clone(&self) -> Self {
        match self {
            Self::Set { key, row: value } => Self::Set { key: key.clone(), row: value.clone() },
            Self::Remove { key } => Self::Remove { key: key.clone() },
        }
    }
}
