// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::Key;
use core::ops::Bound;
use std::collections::BTreeSet;

#[derive(Clone, Debug)]
enum Read {
    Single(Key),
    Range { start: Bound<Key>, end: Bound<Key> },
    All,
}

/// A [`Conflict`] conflict manager implementation that based on the [`BTreeSet`](std::collections::BTreeSet).
#[derive(Debug)]
pub struct BTreeConflict {
    reads: Vec<Read>,
    conflict_keys: BTreeSet<Key>,
}

impl Clone for BTreeConflict {
    fn clone(&self) -> Self {
        Self { reads: self.reads.clone(), conflict_keys: self.conflict_keys.clone() }
    }
}

impl Default for BTreeConflict {
    fn default() -> Self {
        BTreeConflict::new()
    }
}

impl Conflict for BTreeConflict {
    fn new() -> Self {
        Self { reads: Vec::new(), conflict_keys: BTreeSet::new() }
    }

    fn mark_read(&mut self, key: &Key) {
        self.reads.push(Read::Single(key.clone()));
    }

    fn mark_conflict(&mut self, key: &Key) {
        self.conflict_keys.insert(key.clone());
    }

    fn has_conflict(&self, other: &Self) -> bool {
        if self.reads.is_empty() {
            return false;
        }

        for ro in self.reads.iter() {
            match ro {
                Read::Single(k) => {
                    if other.conflict_keys.contains(k) {
                        return true;
                    }
                }
                Read::Range { start, end } => match (start.to_owned(), end.to_owned()) {
                    (Bound::Included(start), Bound::Included(end)) => {
                        if other
                            .conflict_keys
                            .range((Bound::Included(start), Bound::Included(end)))
                            .next()
                            .is_some()
                        {
                            return true;
                        }
                    }
                    (Bound::Included(start), Bound::Excluded(end)) => {
                        if other
                            .conflict_keys
                            .range((Bound::Included(start), Bound::Excluded(end)))
                            .next()
                            .is_some()
                        {
                            return true;
                        }
                    }
                    (Bound::Included(start), Bound::Unbounded) => {
                        if other
                            .conflict_keys
                            .range((Bound::Included(start), Bound::Unbounded))
                            .next()
                            .is_some()
                        {
                            return true;
                        }
                    }
                    (Bound::Excluded(start), Bound::Included(end)) => {
                        if other
                            .conflict_keys
                            .range((Bound::Excluded(start), Bound::Included(end)))
                            .next()
                            .is_some()
                        {
                            return true;
                        }
                    }
                    (Bound::Excluded(start), Bound::Excluded(end)) => {
                        if other
                            .conflict_keys
                            .range((Bound::Excluded(start), Bound::Excluded(end)))
                            .next()
                            .is_some()
                        {
                            return true;
                        }
                    }
                    (Bound::Excluded(start), Bound::Unbounded) => {
                        if other
                            .conflict_keys
                            .range((Bound::Excluded(start), Bound::Unbounded))
                            .next()
                            .is_some()
                        {
                            return true;
                        }
                    }
                    (Bound::Unbounded, Bound::Included(end)) => {
                        let range = ..=end;
                        for write in other.conflict_keys.iter() {
                            if range.contains(&write) {
                                return true;
                            }
                        }
                    }
                    (Bound::Unbounded, Bound::Excluded(end)) => {
                        let range = ..end;
                        for write in other.conflict_keys.iter() {
                            if range.contains(&write) {
                                return true;
                            }
                        }
                    }
                    (Bound::Unbounded, Bound::Unbounded) => unreachable!(),
                },
                Read::All => {
                    if !other.conflict_keys.is_empty() {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn rollback(&mut self) {
        self.reads.clear();
        self.conflict_keys.clear();
    }
}

impl ConflictRange for BTreeConflict {
    fn mark_range(&mut self, range: impl RangeBounds<Key>) {
        let start = match range.start_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(k) => Bound::Included(k.clone()),
            Bound::Excluded(k) => Bound::Excluded(k.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };

        if start == Bound::Unbounded && end == Bound::Unbounded {
            self.reads.push(Read::All);
            return;
        }

        self.reads.push(Read::Range { start, end });
    }
}

#[cfg(test)]
mod test {
    use super::{BTreeConflict, Conflict};

    #[test]
    fn test_btree_cm() {
        let mut cm = BTreeConflict::new();
        cm.mark_read(&b"1".to_vec());
        cm.mark_read(&b"2".to_vec());
        cm.mark_conflict(&b"2".to_vec());
        cm.mark_conflict(&b"3".to_vec());
        let cm2 = cm.clone();
        assert!(cm.has_conflict(&cm2));
    }
}
