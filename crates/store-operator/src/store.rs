// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_runtime::{shutdown::Shutdown, sync::mutex::Mutex};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_value::util::cowvec::CowVec;
use rusqlite::{Connection, ToSql, params};

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

#[derive(Clone)]
pub struct OperatorStore {
	inner: Arc<StoreInner>,
}

struct StoreInner {
	conn: Mutex<Option<Connection>>,
}

impl Default for OperatorStore {
	fn default() -> Self {
		Self::testing_memory()
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

	pub fn testing_memory() -> Self {
		Self::memory()
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

	pub fn drop_arena(&self, operator: OperatorId) {
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
		) WITHOUT ROWID;"#,
	)
	.expect("operator state schema could not be created");
}
