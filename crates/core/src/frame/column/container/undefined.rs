// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct UndefinedContainer {
    len: usize,
}

impl UndefinedContainer {
    pub fn new(len: usize) -> Self {
        Self { len }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { len: 0 }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn push_undefined(&mut self) {
        self.len += 1;
    }

    pub fn extend(&mut self, other: &Self) -> crate::Result<()> {
        self.len += other.len;
        Ok(())
    }

    pub fn extend_from_undefined(&mut self, len: usize) {
        self.len += len;
    }

    pub fn slice(&self, start: usize, end: usize) -> Self {
        Self {
            len: (end - start).min(self.len.saturating_sub(start)),
        }
    }

    pub fn filter(&mut self, mask: &crate::BitVec) {
        let mut new_len = 0;
        for (i, keep) in mask.iter().enumerate() {
            if keep && i < self.len {
                new_len += 1;
            }
        }
        self.len = new_len;
    }

    pub fn reorder(&mut self, indices: &[usize]) {
        self.len = indices.len();
    }
}

impl Default for UndefinedContainer {
    fn default() -> Self {
        Self { len: 0 }
    }
}