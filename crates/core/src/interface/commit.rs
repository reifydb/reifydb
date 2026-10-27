// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_value::value::datetime::DateTime;
use reifydb_wal::lsn::Lsn;

use crate::{
	common::ChangeVersion,
	delta::{Delta, RemoveAnnounce},
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
}

impl CommitEntry {
	pub fn build(version: ChangeVersion, lsn: Lsn, at: DateTime, deltas: Vec<Delta>) -> Self {
		Self {
			version,
			lsn,
			at,
			deltas: deltas.into_iter().filter(flow_relevant).collect(),
		}
	}
}
