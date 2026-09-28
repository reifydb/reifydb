// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ops::{Bound, ControlFlow},
	sync::Arc as StdArc,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::common::CommitVersion;
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};

use crate::{
	storage::{Read, Remove, RowStats, Rows, Write},
	store::EvictedVersion,
};

pub trait CommitHooks: Send + Sync {
	fn on(&self, _point: HookPoint<'_>) {}
}

pub struct NoHooks;

impl CommitHooks for NoHooks {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPoint<'a> {
	BeforeClose,
	AfterClose,
	Visit {
		key: &'a EncodedKey,
	},
	BeforeVersions {
		key: &'a [u8],
	},
	BeforeRemove,
}

pub struct TestingRows<R> {
	rows: R,
	hooks: StdArc<dyn CommitHooks>,
}

impl<R> TestingRows<R> {
	pub fn over(rows: R, hooks: StdArc<dyn CommitHooks>) -> Self {
		Self {
			rows,
			hooks,
		}
	}
}

impl<R: Rows> Rows for TestingRows<R> {
	fn empty(&self) -> Self {
		Self {
			rows: self.rows.empty(),
			hooks: StdArc::clone(&self.hooks),
		}
	}
}

impl<R: Read> Read for TestingRows<R> {
	fn get(&self, key: &[u8], version: CommitVersion) -> Option<(CommitVersion, Option<CowVec<u8>>)> {
		self.rows.get(key, version)
	}

	fn scan(
		&self,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		reverse: bool,
		mut visit: impl FnMut(&EncodedKey, &[(CommitVersion, &Option<CowVec<u8>>)]) -> ControlFlow<()>,
	) {
		self.rows.scan(start, end, reverse, |key, versions| {
			self.hooks.on(HookPoint::Visit {
				key,
			});
			visit(key, versions)
		});
	}

	fn scan_closed(
		&self,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		cutoff: CommitVersion,
		mut visit: impl FnMut(&EncodedKey, &[(CommitVersion, &Option<CowVec<u8>>)]) -> ControlFlow<()>,
	) {
		self.rows.scan_closed(start, end, cutoff, |key, versions| {
			self.hooks.on(HookPoint::Visit {
				key,
			});
			visit(key, versions)
		});
	}

	fn versions(&self, key: &[u8]) -> Vec<CommitVersion> {
		self.hooks.on(HookPoint::BeforeVersions {
			key,
		});
		self.rows.versions(key)
	}

	fn stats(&self) -> RowStats {
		self.rows.stats()
	}
}

impl<R: Write> Write for TestingRows<R> {
	fn insert(&self, version: CommitVersion, rows: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> ByteSize {
		self.rows.insert(version, rows)
	}

	fn close(&self) {
		self.hooks.on(HookPoint::BeforeClose);
		self.rows.close();
		self.hooks.on(HookPoint::AfterClose);
	}
}

impl<R: Remove> Remove for TestingRows<R> {
	fn remove(&self, pairs: Vec<(EncodedKey, CommitVersion)>) -> (Vec<EvictedVersion>, Vec<EncodedKey>) {
		self.hooks.on(HookPoint::BeforeRemove);
		self.rows.remove(pairs)
	}
}
