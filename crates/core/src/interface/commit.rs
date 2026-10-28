// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet},
	mem::size_of,
	sync::{Arc, OnceLock},
};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};
use reifydb_wal::lsn::Lsn;

use crate::{
	common::{ChangeVersion, CommitVersion},
	delta::{Delta, RemoveAnnounce, SetPre},
	interface::{catalog::object::ObjectId, change::Change},
	key::cdc::should_exclude_from_cdc,
};

pub fn flow_relevant(delta: &Delta) -> bool {
	!should_exclude_from_cdc(delta.key().kind())
		&& !matches!(
			delta,
			Delta::Remove {
				announce: RemoveAnnounce::Silent,
				..
			}
		)
}

#[derive(Debug)]
pub struct CommitEntry {
	pub version: ChangeVersion,
	pub lsn: Lsn,
	pub at: DateTime,
	pub deltas: Vec<Delta>,
	changed_objects: OnceLock<BTreeSet<ObjectId>>,
	lift: OnceLock<CommitLift>,
}

#[derive(Debug, Default)]
pub struct CommitLift {
	pub objects: BTreeMap<ObjectId, CommitObjectLift>,
}

#[derive(Debug, Default)]
pub struct CommitObjectLift {
	pub rows: Vec<(usize, RowNumber)>,
	pub change: OnceLock<Option<Change>>,
}

impl CommitEntry {
	pub fn build(version: ChangeVersion, lsn: Lsn, at: DateTime, deltas: Vec<Delta>) -> Self {
		Self {
			version,
			lsn,
			at,
			deltas: deltas.into_iter().filter(flow_relevant).collect(),
			changed_objects: OnceLock::new(),
			lift: OnceLock::new(),
		}
	}

	pub fn changed_objects_with(&self, compute: impl FnOnce(&Self) -> BTreeSet<ObjectId>) -> &BTreeSet<ObjectId> {
		self.changed_objects.get_or_init(|| compute(self))
	}

	pub fn lift_with(&self, build: impl FnOnce(&Self) -> CommitLift) -> &CommitLift {
		self.lift.get_or_init(|| build(self))
	}

	pub fn bytes(&self) -> u64 {
		let deltas: usize = self
			.deltas
			.iter()
			.map(|delta| {
				size_of::<Delta>()
					+ delta.bytes().map_or(0, |bytes| bytes.len())
					+ delta_pre(delta).map_or(0, |pre| pre.len())
			})
			.sum();
		(size_of::<Self>() + deltas) as u64
	}
}

fn delta_pre(delta: &Delta) -> Option<&EncodedBytes> {
	match delta {
		Delta::Set {
			pre: SetPre::Present(pre),
			..
		} => Some(pre),
		Delta::Set {
			..
		} => None,
		Delta::Remove {
			announce,
			..
		} => announce.pre(),
	}
}

#[derive(Debug, Clone)]
pub struct Loaded {
	pub entries: Vec<Arc<CommitEntry>>,
	pub advance_to: CommitVersion,
	pub more: bool,
}

pub trait CommitLog: Send + Sync + 'static {
	fn read(&self, after: (CommitVersion, Lsn), up_to: CommitVersion, budget: ByteSize) -> Result<Loaded>;
}

pub trait CommitFloor: Send + Sync + 'static {
	fn position(&self) -> Option<Lsn>;

	fn advance(&self, lsn: Lsn);
}

#[derive(Clone)]
pub struct FlowWal {
	pub log: Arc<dyn CommitLog>,
	pub floor: Arc<dyn CommitFloor>,
}
