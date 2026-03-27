// Copyright (c) 2025 ReifyDB
// SPDX-License-Identifier: Apache-2.0

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
	message::Command,
	node::{NodeId, Term},
};

/// A log index (entry position). Starts at 1. 0 indicates no index.
pub type Index = u64;

/// A log entry containing a state machine command.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
	/// The entry index.
	pub index: Index,
	/// The term in which the entry was added.
	pub term: Term,
	/// The state machine command.
	pub command: Command,
}

/// An in-memory Raft log. Stores entries and persistent metadata (term/vote,
/// commit index) without any durable storage.
pub struct Log {
	/// Log entries, keyed by index.
	entries: BTreeMap<Index, Entry>,
	/// The current term.
	term: Term,
	/// Our leader vote in the current term, if any.
	vote: Option<NodeId>,
	/// The index of the last stored entry.
	last_index: Index,
	/// The term of the last stored entry.
	last_term: Term,
	/// The index of the last committed entry.
	commit_index: Index,
	/// The term of the last committed entry.
	commit_term: Term,
}

impl Log {
	/// Creates a new empty in-memory log.
	pub fn new() -> Self {
		Self {
			entries: BTreeMap::new(),
			term: 0,
			vote: None,
			last_index: 0,
			last_term: 0,
			commit_index: 0,
			commit_term: 0,
		}
	}

	/// Returns the commit index and term.
	pub fn get_commit_index(&self) -> (Index, Term) {
		(self.commit_index, self.commit_term)
	}

	/// Returns the last log index and term.
	pub fn get_last_index(&self) -> (Index, Term) {
		(self.last_index, self.last_term)
	}

	/// Returns the current term and vote.
	pub fn get_term_vote(&self) -> (Term, Option<NodeId>) {
		(self.term, self.vote)
	}

	/// Stores the current term and cast vote. Enforces that the term does not
	/// regress, and that we only vote for one node in a term.
	pub fn set_term_vote(&mut self, term: Term, vote: Option<NodeId>) {
		assert!(term > 0, "can't set term 0");
		assert!(term >= self.term, "term regression {} → {}", self.term, term);
		assert!(term > self.term || self.vote.is_none() || vote == self.vote, "can't change vote");

		self.term = term;
		self.vote = vote;
	}

	/// Appends a command to the log at the current term, returning its index.
	pub fn append(&mut self, command: Command) -> Index {
		assert!(self.term > 0, "can't append entry in term 0");
		let entry = Entry {
			index: self.last_index + 1,
			term: self.term,
			command,
		};
		self.last_index = entry.index;
		self.last_term = entry.term;
		self.entries.insert(entry.index, entry);
		self.last_index
	}

	/// Commits entries up to and including the given index.
	pub fn commit(&mut self, index: Index) -> Index {
		let term = match self.get(index) {
			Some(entry) if entry.index < self.commit_index => {
				panic!("commit index regression {} → {}", self.commit_index, entry.index);
			}
			Some(entry) if entry.index == self.commit_index => return index,
			Some(entry) => entry.term,
			None => panic!("commit index {index} does not exist"),
		};
		self.commit_index = index;
		self.commit_term = term;
		index
	}

	/// Fetches an entry at an index, or None if it does not exist.
	pub fn get(&self, index: Index) -> Option<&Entry> {
		self.entries.get(&index)
	}

	/// Checks if the log contains an entry with the given index and term.
	pub fn has(&self, index: Index, term: Term) -> bool {
		if index == 0 || index > self.last_index {
			return false;
		}
		if (index, term) == (self.last_index, self.last_term) {
			return true;
		}
		self.entries.get(&index).map(|e| e.term == term).unwrap_or(false)
	}

	/// Returns entries in the given index range (inclusive start, inclusive end).
	pub fn scan(&self, from: Index, to: Index) -> Vec<Entry> {
		self.entries.range(from..=to).map(|(_, e)| e.clone()).collect()
	}

	/// Returns entries ready to apply: from applied_index+1 to commit_index.
	pub fn scan_apply(&self, applied_index: Index) -> Vec<Entry> {
		if applied_index >= self.commit_index {
			return Vec::new();
		}
		self.scan(applied_index + 1, self.commit_index)
	}

	/// Splices entries into the log. New indexes will be appended. Overlapping
	/// indexes with the same term are skipped. Overlapping indexes with
	/// different terms truncate the log and splice the new entries.
	pub fn splice(&mut self, entries: Vec<Entry>) -> Index {
		let (Some(first), Some(last)) = (entries.first(), entries.last()) else {
			return self.last_index;
		};

		assert!(first.index > 0 && first.term > 0, "spliced entry has index or term 0");
		assert!(entries.windows(2).all(|w| w[0].index + 1 == w[1].index), "spliced entries are not contiguous");
		assert!(entries.windows(2).all(|w| w[0].term <= w[1].term), "spliced entries have term regression",);
		assert!(last.term <= self.term, "splice term {} beyond current {}", last.term, self.term);

		// Check connection to existing log.
		if first.index > 1 {
			let base = self.get(first.index - 1);
			assert!(base.is_some(), "first index {} must touch existing log", first.index);
			if let Some(base) = base {
				assert!(
					first.term >= base.term,
					"splice term regression {} → {}",
					base.term,
					first.term
				);
			}
		}

		// Skip entries already in the log with matching terms.
		let mut entries = entries.as_slice();
		for entry in self.scan(first.index, last.index) {
			if entries.is_empty() {
				break;
			}
			assert_eq!(entry.index, entries[0].index, "index mismatch");
			if entry.term != entries[0].term {
				break;
			}
			entries = &entries[1..];
		}

		let Some(first) = entries.first() else {
			return self.last_index;
		};

		assert!(first.index > self.commit_index, "spliced entries below commit index");

		// Write new entries and truncate any old tail.
		for entry in entries {
			self.entries.insert(entry.index, entry.clone());
		}
		let truncate_from = last.index + 1;
		let to_remove: Vec<Index> = self.entries.range(truncate_from..).map(|(&k, _)| k).collect();
		for k in to_remove {
			self.entries.remove(&k);
		}

		self.last_index = last.index;
		self.last_term = last.term;
		self.last_index
	}
}
