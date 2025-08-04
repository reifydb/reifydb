// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::row::{EncodedKey, EncodedRow};
use std::cmp;

#[derive(Debug, PartialEq, Eq)]
pub enum Delta {
    Insert { key: EncodedKey, row: EncodedRow },
    Update { key: EncodedKey, row: EncodedRow },
    Upsert { key: EncodedKey, row: EncodedRow },
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
            Self::Insert { key, .. } | Self::Update { key, .. } | Self::Upsert { key, .. } => key,
            Self::Remove { key } => key,
        }
    }

    /// Returns the row, if None, it means the entry is marked as remove.
    pub fn row(&self) -> Option<&EncodedRow> {
        match self {
            Self::Insert { row, .. } | Self::Update { row, .. } | Self::Upsert { row, .. } => {
                Some(row)
            }
            Self::Remove { .. } => None,
        }
    }
}

impl Clone for Delta {
    fn clone(&self) -> Self {
        match self {
            Self::Insert { key, row: value } => {
                Self::Insert { key: key.clone(), row: value.clone() }
            }
            Self::Update { key, row: value } => {
                Self::Update { key: key.clone(), row: value.clone() }
            }
            Self::Upsert { key, row: value } => {
                Self::Upsert { key: key.clone(), row: value.clone() }
            }
            Self::Remove { key } => Self::Remove { key: key.clone() },
        }
    }
}
