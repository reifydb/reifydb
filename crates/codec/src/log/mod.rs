// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub mod index;
pub mod meta;
pub mod reader;
pub mod record;
pub mod vote;

use std::fmt::{self, Display, Formatter};

use reifydb_value::reifydb_assertions;
use serde::{Deserialize, Serialize};

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogVersion(u64);

impl LogVersion {
	pub const ZERO: Self = Self(0);

	pub const fn new(version: u64) -> Self {
		Self(version)
	}

	pub const fn as_u64(self) -> u64 {
		self.0
	}
}

impl Display for LogVersion {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl From<LogVersion> for u64 {
	fn from(version: LogVersion) -> Self {
		version.0
	}
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LogIndex(u64);

impl LogIndex {
	pub const ZERO: Self = Self(0);

	pub const fn new(index: u64) -> Self {
		Self(index)
	}

	pub const fn as_u64(self) -> u64 {
		self.0
	}
}

impl Display for LogIndex {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl From<LogIndex> for u64 {
	fn from(index: LogIndex) -> Self {
		index.0
	}
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Term(u64);

impl Term {
	pub const ZERO: Self = Self(0);

	pub const fn new(term: u64) -> Self {
		Self(term)
	}

	pub const fn as_u64(self) -> u64 {
		self.0
	}
}

impl Display for Term {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl From<Term> for u64 {
	fn from(term: Term) -> Self {
		term.0
	}
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeId(u64);

impl NodeId {
	pub const fn new(id: u64) -> Self {
		reifydb_assertions! {
			assert!(
				id != u64::MAX,
				"u64::MAX is the on disk encoding of no vote, so a node carrying it is indistinguishable \
				 from a node that has never voted and can vote twice in one term"
			);
		}
		Self(id)
	}

	pub const fn as_u64(self) -> u64 {
		self.0
	}
}

impl Display for NodeId {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl From<NodeId> for u64 {
	fn from(id: NodeId) -> Self {
		id.0
	}
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct RecordKind(u32);

impl RecordKind {
	pub const fn new(kind: u32) -> Self {
		Self(kind)
	}

	pub const fn as_u32(self) -> u32 {
		self.0
	}
}

impl Display for RecordKind {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl From<RecordKind> for u32 {
	fn from(kind: RecordKind) -> Self {
		kind.0
	}
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Position(u64);

impl Position {
	pub const ZERO: Self = Self(0);

	pub const fn new(position: u64) -> Self {
		Self(position)
	}

	pub const fn as_u64(self) -> u64 {
		self.0
	}

	pub const fn advance(self, bytes: u64) -> Self {
		Self(self.0 + bytes)
	}

	pub const fn distance_from(self, earlier: Self) -> u64 {
		self.0.saturating_sub(earlier.0)
	}
}

impl Display for Position {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl From<Position> for u64 {
	fn from(position: Position) -> Self {
		position.0
	}
}

#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct VoteSeq(u64);

impl VoteSeq {
	pub const FIRST: Self = Self(1);

	pub const fn new(seq: u64) -> Self {
		Self(seq)
	}

	pub const fn as_u64(self) -> u64 {
		self.0
	}

	pub const fn next(self) -> Self {
		Self(self.0 + 1)
	}
}

impl Display for VoteSeq {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl From<VoteSeq> for u64 {
	fn from(seq: VoteSeq) -> Self {
		seq.0
	}
}
