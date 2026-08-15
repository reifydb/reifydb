// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
use rusqlite::params;
use tracing::instrument;

use crate::sqlite::{
	SqliteOperatorStorage,
	sql::{CHECKPOINT_FLOOR_SQL, CHECKPOINT_GET_SQL, CHECKPOINT_LIST_SQL},
};

impl SqliteOperatorStorage {
	#[instrument(name = "store::operator::checkpoint_get", level = "trace", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_get(&self, flow: FlowId) -> Option<CommitVersion> {
		let guard = self.read_conn();
		let conn = guard.as_ref()?;
		let mut stmt =
			conn.prepare_cached(CHECKPOINT_GET_SQL).expect("flow checkpoint read could not be prepared");
		let mut rows = stmt.query(params![flow.0 as i64]).expect("flow checkpoint read failed");
		let row = rows.next().expect("flow checkpoint read failed")?;
		Some(CommitVersion(row.get::<_, i64>(0).expect("flow checkpoints carry a version") as u64))
	}

	#[instrument(name = "store::operator::checkpoint_floor", level = "trace", skip(self))]
	pub fn checkpoint_floor(&self) -> Option<CommitVersion> {
		let guard = self.read_conn();
		let conn = guard.as_ref()?;
		let mut stmt =
			conn.prepare_cached(CHECKPOINT_FLOOR_SQL).expect("flow checkpoint floor could not be prepared");
		let mut rows = stmt.query([]).expect("flow checkpoint floor failed");
		let row = rows.next().expect("flow checkpoint floor failed")?;
		let version: Option<i64> = row.get(0).expect("the floor query returns one nullable column");
		version.map(|version| CommitVersion(version as u64))
	}

	#[instrument(name = "store::operator::checkpoint_list", level = "trace", skip(self))]
	pub fn checkpoint_list(&self) -> Vec<FlowId> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt =
			conn.prepare_cached(CHECKPOINT_LIST_SQL).expect("flow checkpoint list could not be prepared");
		let mut rows = stmt.query([]).expect("flow checkpoint list failed");
		let mut out = Vec::new();
		while let Some(row) = rows.next().expect("flow checkpoint list failed") {
			out.push(FlowId(row.get::<_, i64>(0).expect("flow checkpoints carry a flow") as u64));
		}
		out
	}
}
