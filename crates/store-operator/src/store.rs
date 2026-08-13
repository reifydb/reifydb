// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::{interface::catalog::flow::OperatorId, key::operator_state::GroupId};
use reifydb_runtime::{shutdown::Shutdown, sync::mutex::Mutex};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{datetime::DateTime, row_number::RowNumber},
};
use rusqlite::{Connection, Rows, ToSql, params};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct OperatorBatch {
	pub items: Vec<(EncodedKey, EncodedOperatorRow)>,
	pub has_more: bool,
}

impl OperatorBatch {
	pub fn empty() -> Self {
		Self {
			items: Vec::new(),
			has_more: false,
		}
	}
}

const ANCHORS_BY_EXPIRY_SQL: &str = r#"SELECT "side", "row_number", "expiry" FROM "operator_seal_anchor"
	   WHERE "operator" = ?1 AND "group" = ?2
	   ORDER BY "expiry" ASC LIMIT ?3"#;

const ANCHORS_DUE_SQL: &str = r#"SELECT "side", "row_number", "expiry" FROM "operator_seal_anchor"
	   WHERE "operator" = ?1 AND "group" = ?2 AND "expiry" <= ?3
	   ORDER BY "expiry" ASC LIMIT ?4"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSealAnchor {
	pub side: u8,
	pub row_number: RowNumber,
	pub expiry: DateTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorStateCensus {
	pub operator: OperatorId,
	pub prefix: Vec<u8>,
	pub keys: u64,
	pub key_bytes: u64,
	pub value_bytes: u64,
}

#[derive(Clone)]
pub struct OperatorStore {
	inner: Arc<StoreInner>,
}

struct StoreInner {
	conn: Mutex<Option<Connection>>,
}

impl Default for OperatorStore {
	fn default() -> Self {
		Self::memory()
	}
}

impl OperatorStore {
	pub fn memory() -> Self {
		Self::with_connection(
			Connection::open_in_memory().expect("operator state database could not be opened"),
		)
	}

	#[cfg(not(target_arch = "wasm32"))]
	pub fn sqlite(config: SqliteConfig) -> Self {
		let path = resolve_db_path(config.path.clone(), "operator.db");
		let conn = connect(&path, convert_flags(&config.flags))
			.expect("operator state database could not be opened");
		pragma::apply(&conn, &config).expect("operator state pragmas could not be applied");
		Self::with_connection(conn)
	}

	fn with_connection(conn: Connection) -> Self {
		ensure_schema(&conn);
		Self {
			inner: Arc::new(StoreInner {
				conn: Mutex::new(Some(conn)),
			}),
		}
	}

	pub fn set(&self, operator: OperatorId, key: EncodedKey, row: EncodedOperatorRow) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.execute(
			r#"INSERT INTO "operator_state" ("operator", "key", "bytes") VALUES (?1, ?2, ?3)
			   ON CONFLICT ("operator", "key") DO UPDATE SET "bytes" = excluded."bytes""#,
			params![operator.0 as i64, key.as_slice(), &row.bytes()[..]],
		)
		.expect("operator state write failed");
	}

	pub fn remove(&self, operator: OperatorId, key: &EncodedKey) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.execute(
			r#"DELETE FROM "operator_state" WHERE "operator" = ?1 AND "key" = ?2"#,
			params![operator.0 as i64, key.as_slice()],
		)
		.expect("operator state delete failed");
	}

	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedOperatorRow> {
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref()?;
		let mut stmt = conn
			.prepare_cached(r#"SELECT "bytes" FROM "operator_state" WHERE "operator" = ?1 AND "key" = ?2"#)
			.expect("operator state read could not be prepared");
		let mut rows =
			stmt.query(params![operator.0 as i64, key.as_slice()]).expect("operator state read failed");
		let row = rows.next().expect("operator state read failed")?;
		let bytes: Vec<u8> = row.get(0).expect("operator state rows carry a blob payload");
		Some(decode_row(bytes))
	}

	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return false;
		};
		let mut stmt = conn
			.prepare_cached(
				r#"SELECT 1 FROM "operator_state" WHERE "operator" = ?1 AND "key" = ?2 LIMIT 1"#,
			)
			.expect("operator state probe could not be prepared");
		let mut rows =
			stmt.query(params![operator.0 as i64, key.as_slice()]).expect("operator state probe failed");
		rows.next().expect("operator state probe failed").is_some()
	}

	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		let mut sql = String::from(r#"SELECT "key", "bytes" FROM "operator_state" WHERE "operator" = ?1"#);
		let mut blobs: Vec<Vec<u8>> = Vec::new();
		push_bound(&mut sql, &mut blobs, range.start.as_ref(), true);
		push_bound(&mut sql, &mut blobs, range.end.as_ref(), false);
		sql.push_str(&format!(r#" ORDER BY "key" ASC LIMIT ?{}"#, blobs.len() + 2));

		let limit = batch_size.max(1);
		let operator_param = operator.0 as i64;
		let limit_param = limit.min(i64::MAX as u64 - 1) as i64 + 1;
		let mut bound_params: Vec<&dyn ToSql> = Vec::with_capacity(blobs.len() + 2);
		bound_params.push(&operator_param);
		for blob in &blobs {
			bound_params.push(blob);
		}
		bound_params.push(&limit_param);

		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return OperatorBatch::empty();
		};
		let mut stmt = conn.prepare_cached(&sql).expect("operator state scan could not be prepared");
		let mut rows = stmt.query(bound_params.as_slice()).expect("operator state scan failed");

		let mut items = Vec::new();
		while let Some(row) = rows.next().expect("operator state scan failed") {
			let key: Vec<u8> = row.get(0).expect("operator state rows carry a blob key");
			let bytes: Vec<u8> = row.get(1).expect("operator state rows carry a blob payload");
			items.push((EncodedKey::new(key), decode_row(bytes)));
		}

		let has_more = items.len() as u64 > limit;
		items.truncate(limit as usize);
		OperatorBatch {
			items,
			has_more,
		}
	}

	pub fn bytes(&self, operator: OperatorId) -> u64 {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return 0;
		};
		conn.query_row(
			r#"SELECT COALESCE(SUM(LENGTH("key") + LENGTH("bytes")), 0) FROM "operator_state"
			   WHERE "operator" = ?1"#,
			params![operator.0 as i64],
			|row| row.get::<_, i64>(0),
		)
		.expect("operator state size query failed") as u64
	}

	pub fn total_bytes(&self) -> u64 {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return 0;
		};
		conn.query_row(
			r#"SELECT COALESCE(SUM(LENGTH("key") + LENGTH("bytes")), 0) FROM "operator_state""#,
			[],
			|row| row.get::<_, i64>(0),
		)
		.expect("operator state size query failed") as u64
	}

	pub fn census(&self, prefix_len: u32) -> Vec<OperatorStateCensus> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt = conn
			.prepare_cached(
				r#"SELECT "operator", substr("key", 1, ?1) AS "prefix", COUNT(*),
				          SUM(LENGTH("key")), SUM(LENGTH("bytes"))
				   FROM "operator_state"
				   GROUP BY "operator", "prefix"
				   ORDER BY "operator", "prefix""#,
			)
			.expect("operator state census could not be prepared");
		let mut rows = stmt.query(params![prefix_len as i64]).expect("operator state census failed");

		let mut out = Vec::new();
		while let Some(row) = rows.next().expect("operator state census failed") {
			out.push(OperatorStateCensus {
				operator: OperatorId(
					row.get::<_, i64>(0).expect("census rows carry an operator") as u64
				),
				prefix: row.get(1).expect("census rows carry a blob prefix"),
				keys: row.get::<_, i64>(2).expect("census rows carry a key count") as u64,
				key_bytes: row.get::<_, i64>(3).expect("census rows carry a key byte sum") as u64,
				value_bytes: row.get::<_, i64>(4).expect("census rows carry a value byte sum") as u64,
			});
		}
		out
	}

	pub fn anchor_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref()?;
		let mut stmt = conn
			.prepare_cached(
				r#"SELECT "expiry" FROM "operator_seal_anchor"
				   WHERE "operator" = ?1 AND "group" = ?2 AND "side" = ?3 AND "row_number" = ?4"#,
			)
			.expect("seal anchor read could not be prepared");
		let mut rows = stmt
			.query(params![operator.0 as i64, group.0 as i64, side as i64, row_number.0 as i64])
			.expect("seal anchor read failed");
		let row = rows.next().expect("seal anchor read failed")?;
		Some(decode_expiry(row.get(0).expect("seal anchors carry an expiry")))
	}

	pub fn anchors_by_expiry(&self, operator: OperatorId, group: GroupId, limit: u64) -> Vec<OperatorSealAnchor> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt =
			conn.prepare_cached(ANCHORS_BY_EXPIRY_SQL).expect("seal anchor scan could not be prepared");
		let rows = stmt
			.query(params![operator.0 as i64, group.0 as i64, limit as i64])
			.expect("seal anchor scan failed");
		collect_anchors(rows)
	}

	pub fn anchors_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<OperatorSealAnchor> {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt =
			conn.prepare_cached(ANCHORS_DUE_SQL).expect("seal anchor due scan could not be prepared");
		let rows = stmt
			.query(params![operator.0 as i64, group.0 as i64, at.to_millis() as i64, limit as i64])
			.expect("seal anchor due scan failed");
		collect_anchors(rows)
	}

	pub fn anchor_set(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
		expiry: DateTime,
	) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(
			r#"INSERT INTO "operator_seal_anchor" ("operator", "group", "side", "row_number", "expiry")
			   VALUES (?1, ?2, ?3, ?4, ?5)
			   ON CONFLICT ("operator", "group", "side", "row_number")
			   DO UPDATE SET "expiry" = excluded."expiry""#,
		)
		.expect("seal anchor write could not be prepared")
		.execute(params![
			operator.0 as i64,
			group.0 as i64,
			side as i64,
			row_number.0 as i64,
			expiry.to_millis() as i64
		])
		.expect("seal anchor write failed");
	}

	pub fn anchor_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(
			r#"DELETE FROM "operator_seal_anchor"
			   WHERE "operator" = ?1 AND "group" = ?2 AND "side" = ?3 AND "row_number" = ?4"#,
		)
		.expect("seal anchor delete could not be prepared")
		.execute(params![operator.0 as i64, group.0 as i64, side as i64, row_number.0 as i64])
		.expect("seal anchor delete failed");
	}

	pub fn anchors_remove_group(&self, operator: OperatorId, group: GroupId) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(r#"DELETE FROM "operator_seal_anchor" WHERE "operator" = ?1 AND "group" = ?2"#)
			.expect("seal anchor group delete could not be prepared")
			.execute(params![operator.0 as i64, group.0 as i64])
			.expect("seal anchor group delete failed");
	}

	pub fn anchors_drop_operator(&self, operator: OperatorId) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(r#"DELETE FROM "operator_seal_anchor" WHERE "operator" = ?1"#)
			.expect("seal anchor operator delete could not be prepared")
			.execute(params![operator.0 as i64])
			.expect("seal anchor operator delete failed");
	}

	pub fn drop_operator_state(&self, operator: OperatorId) {
		{
			let guard = self.inner.conn.lock();
			if let Some(conn) = guard.as_ref() {
				conn.execute(
					r#"DELETE FROM "operator_state" WHERE "operator" = ?1"#,
					params![operator.0 as i64],
				)
				.expect("operator state drop failed");
			}
		}
	}
}

impl Shutdown for OperatorStore {
	fn shutdown(&self) {
		if let Some(conn) = self.inner.conn.lock().take() {
			let _ = conn.close();
		}
	}
}

fn push_bound(sql: &mut String, blobs: &mut Vec<Vec<u8>>, bound: Bound<&EncodedKey>, lower: bool) {
	let (key, operator) = match bound {
		Bound::Included(key) => (
			key,
			if lower {
				">="
			} else {
				"<="
			},
		),
		Bound::Excluded(key) => (
			key,
			if lower {
				">"
			} else {
				"<"
			},
		),
		Bound::Unbounded => return,
	};
	blobs.push(key.as_slice().to_vec());
	sql.push_str(&format!(r#" AND "key" {} ?{}"#, operator, blobs.len() + 1));
}

fn decode_expiry(millis: i64) -> DateTime {
	DateTime::from_millis(u64::try_from(millis).expect("seal anchor expiries are written as unsigned millis"))
}

fn collect_anchors(mut rows: Rows<'_>) -> Vec<OperatorSealAnchor> {
	let mut out = Vec::new();
	while let Some(row) = rows.next().expect("seal anchor scan failed") {
		out.push(OperatorSealAnchor {
			side: u8::try_from(row.get::<_, i64>(0).expect("seal anchors carry a side"))
				.expect("seal anchor sides are written as bytes"),
			row_number: RowNumber(row.get::<_, i64>(1).expect("seal anchors carry a row number") as u64),
			expiry: decode_expiry(row.get(2).expect("seal anchors carry an expiry")),
		});
	}
	out
}

fn decode_row(bytes: Vec<u8>) -> EncodedOperatorRow {
	EncodedOperatorRow::try_from(EncodedBytes(CowVec::new(bytes)))
		.expect("operator state is written only through set, which types it")
}

fn ensure_schema(conn: &Connection) {
	conn.execute_batch(
		r#"CREATE TABLE IF NOT EXISTS "operator_state" (
			"operator" INTEGER NOT NULL,
			"key" BLOB NOT NULL,
			"bytes" BLOB NOT NULL,
			PRIMARY KEY ("operator", "key")
		) WITHOUT ROWID;

		CREATE TABLE IF NOT EXISTS "operator_seal_anchor" (
			"operator" INTEGER NOT NULL,
			"group" INTEGER NOT NULL,
			"side" INTEGER NOT NULL,
			"row_number" INTEGER NOT NULL,
			"expiry" INTEGER NOT NULL,
			PRIMARY KEY ("operator", "group", "side", "row_number")
		) WITHOUT ROWID;

		CREATE INDEX IF NOT EXISTS "operator_seal_anchor_due"
			ON "operator_seal_anchor" ("operator", "group", "expiry");"#,
	)
	.expect("operator state schema could not be created");
}
