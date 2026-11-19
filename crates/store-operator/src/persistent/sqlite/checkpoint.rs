// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::FlowId, operator::FlowCheckpoint},
};
use reifydb_wal::lsn::Lsn;
use rusqlite::params;
use tracing::instrument;

use crate::{
	error::StoreOperatorError,
	persistent::sqlite::{
		SqlitePersistent,
		sql::{
			CHECKPOINT_GET_SQL, CHECKPOINT_LIST_SQL, LOWEST_CHECKPOINT_LSN_SQL, LOWEST_CHECKPOINT_SQL,
			SAVED_LSN_GET_SQL,
		},
	},
};

impl SqlitePersistent {
	#[instrument(name = "store::operator::persistent::sqlite::checkpoint_get", level = "trace", skip(self), fields(flow = flow.0))]
	pub fn checkpoint_get(&self, flow: FlowId) -> Result<Option<FlowCheckpoint>, StoreOperatorError> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Err(StoreOperatorError::Closed);
		};
		let mut stmt = conn.prepare_cached(CHECKPOINT_GET_SQL)?;
		let mut rows = stmt.query(params![flow.0 as i64])?;
		let Some(row) = rows.next()? else {
			return Ok(None);
		};
		Ok(Some(FlowCheckpoint {
			version: CommitVersion(row.get::<_, i64>(0)? as u64),
			lsn: Lsn::new(row.get::<_, i64>(1)? as u64),
		}))
	}

	#[instrument(name = "store::operator::persistent::sqlite::lowest_checkpoint", level = "trace", skip(self))]
	pub fn lowest_checkpoint(&self) -> Result<Option<CommitVersion>, StoreOperatorError> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Err(StoreOperatorError::Closed);
		};
		let mut stmt = conn.prepare_cached(LOWEST_CHECKPOINT_SQL)?;
		let mut rows = stmt.query([])?;
		let Some(row) = rows.next()? else {
			return Ok(None);
		};
		let version: Option<i64> = row.get(0)?;
		Ok(version.map(|version| CommitVersion(version as u64)))
	}

	#[instrument(name = "store::operator::persistent::sqlite::lowest_checkpoint_lsn", level = "trace", skip(self))]
	pub fn lowest_checkpoint_lsn(&self) -> Result<Option<Lsn>, StoreOperatorError> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Err(StoreOperatorError::Closed);
		};
		let mut stmt = conn.prepare_cached(LOWEST_CHECKPOINT_LSN_SQL)?;
		let mut rows = stmt.query([])?;
		let Some(row) = rows.next()? else {
			return Ok(None);
		};
		let lsn: Option<i64> = row.get(0)?;
		Ok(lsn.map(|lsn| Lsn::new(lsn as u64)))
	}

	#[instrument(name = "store::operator::persistent::sqlite::checkpoint_list", level = "trace", skip(self))]
	pub fn checkpoint_list(&self) -> Result<Vec<FlowId>, StoreOperatorError> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Err(StoreOperatorError::Closed);
		};
		let mut stmt = conn.prepare_cached(CHECKPOINT_LIST_SQL)?;
		let mut rows = stmt.query([])?;
		let mut out = Vec::new();
		while let Some(row) = rows.next()? {
			out.push(FlowId(row.get::<_, i64>(0)? as u64));
		}
		Ok(out)
	}

	#[instrument(name = "store::operator::persistent::sqlite::saved_lsn", level = "trace", skip(self))]
	pub fn saved_lsn(&self) -> Result<Option<Lsn>, StoreOperatorError> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Err(StoreOperatorError::Closed);
		};
		let mut stmt = conn.prepare_cached(SAVED_LSN_GET_SQL)?;
		let mut rows = stmt.query([])?;
		let Some(row) = rows.next()? else {
			return Ok(None);
		};
		let bytes = row.get::<_, Vec<u8>>(0)?;
		let bytes: [u8; 8] = bytes.as_slice().try_into().map_err(|_| StoreOperatorError::Backend {
			message: format!("operator.db saved lsn holds {} bytes, expected 8", bytes.len()),
		})?;
		Ok(Some(Lsn::new(u64::from_le_bytes(bytes))))
	}
}
