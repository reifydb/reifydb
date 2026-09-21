// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowId};
use rusqlite::params;
use tracing::instrument;

use crate::{
	error::{OperatorError, Result},
	persistent::sqlite::{
		SqlitePersistent,
		sql::{CHECKPOINT_FLOOR_SQL, CHECKPOINT_GET_SQL, CHECKPOINT_LIST_SQL},
	},
};

impl SqlitePersistent {
	#[instrument(name = "store::operator::persistent::sqlite::checkpoint_get", level = "trace", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_get(&self, flow: FlowId) -> Result<Option<CommitVersion>> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Err(OperatorError::Closed);
		};
		let mut stmt = conn.prepare_cached(CHECKPOINT_GET_SQL)?;
		let mut rows = stmt.query(params![flow.0 as i64])?;
		let Some(row) = rows.next()? else {
			return Ok(None);
		};
		Ok(Some(CommitVersion(row.get::<_, i64>(0)? as u64)))
	}

	#[instrument(name = "store::operator::persistent::sqlite::checkpoint_floor", level = "trace", skip(self))]
	pub fn checkpoint_floor(&self) -> Result<Option<CommitVersion>> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Err(OperatorError::Closed);
		};
		let mut stmt = conn.prepare_cached(CHECKPOINT_FLOOR_SQL)?;
		let mut rows = stmt.query([])?;
		let Some(row) = rows.next()? else {
			return Ok(None);
		};
		let version: Option<i64> = row.get(0)?;
		Ok(version.map(|version| CommitVersion(version as u64)))
	}

	#[instrument(name = "store::operator::persistent::sqlite::checkpoint_list", level = "trace", skip(self))]
	pub fn checkpoint_list(&self) -> Result<Vec<FlowId>> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Err(OperatorError::Closed);
		};
		let mut stmt = conn.prepare_cached(CHECKPOINT_LIST_SQL)?;
		let mut rows = stmt.query([])?;
		let mut out = Vec::new();
		while let Some(row) = rows.next()? {
			out.push(FlowId(row.get::<_, i64>(0)? as u64));
		}
		Ok(out)
	}
}
