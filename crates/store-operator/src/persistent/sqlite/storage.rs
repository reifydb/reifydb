// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	ops::Bound,
	sync::{
		Arc,
		atomic::{AtomicU64, AtomicUsize, Ordering},
	},
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, OperatorStateKey},
	metrics::{collect::MetricsCollector, sample::MetricsSample, scan::record_page},
};
use reifydb_runtime::{
	shutdown::Shutdown,
	sync::mutex::{Mutex, MutexGuard},
};
#[cfg(not(target_arch = "wasm32"))]
use reifydb_sqlite::{
	SqliteConfig, SqliteTempPathGuard,
	connection::{connect, convert_flags, resolve_db_path},
	memory::sweep_connection_cache,
	pragma,
};
use reifydb_value::{
	byte_size::ByteSize,
	count::Count,
	util::cowvec::CowVec,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};
use rusqlite::{Connection, Rows, ToSql, params};
use tracing::instrument;

use crate::types::{
	ANCHOR_KEY_BYTES, ANCHOR_VALUE_BYTES, OperatorBatch, OperatorSealAnchor, OperatorSealAnchorCensus,
	OperatorStateCensus, OperatorWrite,
};

#[cfg(test)]
mod tests;

const ANCHORS_BY_EXPIRY_SQL: &str = r#"SELECT "side", "row_number", "expiry" FROM "operator_seal_anchor"
	   WHERE "operator" = ?1 AND "group" = ?2
	   ORDER BY "expiry" ASC LIMIT ?3"#;

const ANCHORS_DUE_SQL: &str = r#"SELECT "side", "row_number", "expiry" FROM "operator_seal_anchor"
	   WHERE "operator" = ?1 AND "group" = ?2 AND "expiry" <= ?3
	   ORDER BY "expiry" ASC LIMIT ?4"#;

const ANCHOR_SET_SQL: &str = r#"INSERT INTO "operator_seal_anchor" ("operator", "group", "side", "row_number", "expiry")
	   VALUES (?1, ?2, ?3, ?4, ?5)
	   ON CONFLICT ("operator", "group", "side", "row_number")
	   DO UPDATE SET "expiry" = excluded."expiry""#;

const ANCHOR_REMOVE_SQL: &str = r#"DELETE FROM "operator_seal_anchor"
	   WHERE "operator" = ?1 AND "group" = ?2 AND "side" = ?3 AND "row_number" = ?4"#;

const STATE_SET_SQL: &str = r#"INSERT INTO "operator_state" ("operator", "key", "bytes") VALUES (?1, ?2, ?3)
	   ON CONFLICT ("operator", "key") DO UPDATE SET "bytes" = excluded."bytes""#;

const STATE_REMOVE_SQL: &str = r#"DELETE FROM "operator_state" WHERE "operator" = ?1 AND "key" = ?2"#;

const BUSY_TIMEOUT: Duration = Duration::from_milliseconds_const(200);

const SQLITE_SCOPE: &str = "sqlite::operator";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OperatorPageCacheMetrics {
	pub used: ByteSize,
	pub hits: Count,
	pub misses: Count,
	pub connections_sampled: Count,
	pub connections_total: Count,
}

#[derive(Clone)]
pub struct SqliteOperatorStorage {
	inner: Arc<StoreInner>,
}

struct StoreInner {
	conn: Mutex<Option<Connection>>,
	readers: ReadPool,
	cache_hits: AtomicU64,
	cache_misses: AtomicU64,
}

struct ReadPool {
	conns: Vec<Mutex<Option<Connection>>>,
	next: AtomicUsize,
}

impl ReadPool {
	fn acquire(&self) -> MutexGuard<'_, Option<Connection>> {
		let n = self.conns.len();
		let start = self.next.fetch_add(1, Ordering::Relaxed) % n;
		for i in 0..n {
			if let Some(guard) = self.conns[(start + i) % n].try_lock() {
				return guard;
			}
		}
		self.conns[start].lock()
	}

	fn shutdown(&self) {
		for slot in &self.conns {
			drop(slot.lock().take());
		}
	}
}

impl SqliteOperatorStorage {
	pub fn in_memory() -> (Self, SqliteTempPathGuard) {
		let (config, guard) = SqliteConfig::in_memory();
		(Self::new(config), guard)
	}

	#[cfg(not(target_arch = "wasm32"))]
	#[instrument(name = "store::operator::new", level = "debug", skip(config), fields(
		db_path = ?config.path,
		page_size = config.page_size.as_ref().map(|size| size.as_bytes()),
		read_pool_size = config.read_pool_size,
		journal_mode = config.journal_mode.as_ref().map(|mode| mode.as_str())
	))]
	pub fn new(config: SqliteConfig) -> Self {
		let path = resolve_db_path(config.path.clone(), "operator.db");
		let flags = convert_flags(&config.flags);

		let conn = connect(&path, flags).expect("operator state database could not be opened");
		pragma::apply(&conn, &config).expect("operator state pragmas could not be applied");
		conn.busy_timeout(BUSY_TIMEOUT.to_std()).expect("operator state busy timeout could not be set");

		let pool_size = config.read_pool_size.max(1) as usize;
		let mut readers = Vec::with_capacity(pool_size);
		for _ in 0..pool_size {
			let reader = connect(&path, flags).expect("operator state read connection could not be opened");
			pragma::apply_read_only(&reader, &config)
				.expect("operator state read pragmas could not be applied");
			reader.busy_timeout(BUSY_TIMEOUT.to_std())
				.expect("operator state read busy timeout could not be set");
			readers.push(reader);
		}

		Self::with_connections(conn, readers)
	}

	fn with_connections(conn: Connection, readers: Vec<Connection>) -> Self {
		ensure_schema(&conn);
		Self {
			inner: Arc::new(StoreInner {
				conn: Mutex::new(Some(conn)),
				readers: ReadPool {
					conns: readers.into_iter().map(|reader| Mutex::new(Some(reader))).collect(),
					next: AtomicUsize::new(0),
				},
				cache_hits: AtomicU64::new(0),
				cache_misses: AtomicU64::new(0),
			}),
		}
	}

	fn read_conn(&self) -> MutexGuard<'_, Option<Connection>> {
		if self.inner.readers.conns.is_empty() {
			return self.inner.conn.lock();
		}
		self.inner.readers.acquire()
	}

	pub fn page_cache_metrics(&self) -> OperatorPageCacheMetrics {
		let mut used = 0u64;
		let mut sampled = 0u64;
		let mut sweep = |conn: &Connection| {
			let swept = sweep_connection_cache(conn);
			self.inner.cache_hits.fetch_add(swept.hits.as_u64(), Ordering::Relaxed);
			self.inner.cache_misses.fetch_add(swept.misses.as_u64(), Ordering::Relaxed);
			used += swept.used.as_bytes();
			sampled += 1;
		};
		if let Some(guard) = self.inner.conn.try_lock()
			&& let Some(conn) = guard.as_ref()
		{
			sweep(conn);
		}
		for slot in &self.inner.readers.conns {
			if let Some(guard) = slot.try_lock()
				&& let Some(conn) = guard.as_ref()
			{
				sweep(conn);
			}
		}
		OperatorPageCacheMetrics {
			used: ByteSize::from_bytes(used),
			hits: Count::new(self.inner.cache_hits.load(Ordering::Relaxed)),
			misses: Count::new(self.inner.cache_misses.load(Ordering::Relaxed)),
			connections_sampled: Count::new(sampled),
			connections_total: Count::new(1 + self.inner.readers.conns.len() as u64),
		}
	}

	pub fn metrics_collectors(&self) -> Vec<Arc<dyn MetricsCollector>> {
		vec![Arc::new(OperatorPageCacheCollector {
			store: self.clone(),
		})]
	}

	#[instrument(name = "store::operator::set", level = "debug", skip(self, key, row), fields(operator = operator.0, key_len = key.len()))]
	pub fn set(&self, operator: OperatorId, key: EncodedKey, row: EncodedOperatorRow) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.execute(STATE_SET_SQL, params![operator.0 as i64, key.as_slice(), &row.bytes()[..]])
			.expect("operator state write failed");
	}

	#[instrument(name = "store::operator::remove", level = "debug", skip(self, key), fields(operator = operator.0, key_len = key.len()))]
	pub fn remove(&self, operator: OperatorId, key: &EncodedKey) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.execute(STATE_REMOVE_SQL, params![operator.0 as i64, key.as_slice()])
			.expect("operator state delete failed");
	}

	#[instrument(name = "store::operator::apply_batch", level = "debug", skip(self, writes), fields(write_count = writes.len()))]
	pub fn apply_batch(&self, writes: &[OperatorWrite]) {
		if writes.is_empty() {
			return;
		}
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		let transaction = conn.unchecked_transaction().expect("operator state batch could not begin");
		for write in writes {
			match write {
				OperatorWrite::Set {
					operator,
					key,
					row,
				} => transaction
					.prepare_cached(STATE_SET_SQL)
					.expect("operator state write could not be prepared")
					.execute(params![operator.0 as i64, key.as_slice(), &row.bytes()[..]])
					.expect("operator state write failed"),
				OperatorWrite::Remove {
					operator,
					key,
				} => transaction
					.prepare_cached(STATE_REMOVE_SQL)
					.expect("operator state delete could not be prepared")
					.execute(params![operator.0 as i64, key.as_slice()])
					.expect("operator state delete failed"),
				OperatorWrite::AnchorSet {
					operator,
					group,
					side,
					row_number,
					expiry,
				} => transaction
					.prepare_cached(ANCHOR_SET_SQL)
					.expect("seal anchor write could not be prepared")
					.execute(params![
						operator.0 as i64,
						group.0 as i64,
						*side as i64,
						row_number.0 as i64,
						expiry.to_millis() as i64
					])
					.expect("seal anchor write failed"),
				OperatorWrite::AnchorRemove {
					operator,
					group,
					side,
					row_number,
				} => transaction
					.prepare_cached(ANCHOR_REMOVE_SQL)
					.expect("seal anchor delete could not be prepared")
					.execute(params![
						operator.0 as i64,
						group.0 as i64,
						*side as i64,
						row_number.0 as i64
					])
					.expect("seal anchor delete failed"),
			};
		}
		transaction.commit().expect("operator state batch could not commit");
	}

	#[instrument(name = "store::operator::get", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.len()))]
	pub fn get(&self, operator: OperatorId, key: &EncodedKey) -> Option<EncodedOperatorRow> {
		let guard = self.read_conn();
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

	#[instrument(name = "store::operator::contains", level = "trace", skip(self, key), fields(operator = operator.0, key_len = key.len()), ret)]
	pub fn contains(&self, operator: OperatorId, key: &EncodedKey) -> bool {
		let guard = self.read_conn();
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

	#[instrument(name = "store::operator::range_batch", level = "trace", skip(self, range), fields(operator = operator.0, batch_size = batch_size))]
	pub fn range_batch(&self, operator: OperatorId, range: EncodedKeyRange, batch_size: u64) -> OperatorBatch {
		let sql = range_sql(range.start.as_ref(), range.end.as_ref());
		let mut blobs: Vec<&[u8]> = Vec::with_capacity(2);
		if let Bound::Included(key) | Bound::Excluded(key) = range.start.as_ref() {
			blobs.push(key.as_slice());
		}
		if let Bound::Included(key) | Bound::Excluded(key) = range.end.as_ref() {
			blobs.push(key.as_slice());
		}

		let limit = batch_size.max(1);
		let operator_param = operator.0 as i64;
		let limit_param = limit.min(i64::MAX as u64 - 1) as i64 + 1;
		let mut bound_params: Vec<&dyn ToSql> = Vec::with_capacity(blobs.len() + 2);
		bound_params.push(&operator_param);
		for blob in &blobs {
			bound_params.push(blob);
		}
		bound_params.push(&limit_param);

		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return OperatorBatch::empty();
		};
		let mut stmt = conn.prepare_cached(sql).expect("operator state scan could not be prepared");
		let mut rows = stmt.query(bound_params.as_slice()).expect("operator state scan failed");

		let mut items = Vec::new();
		while let Some(row) = rows.next().expect("operator state scan failed") {
			let key: Vec<u8> = row.get(0).expect("operator state rows carry a blob key");
			let bytes: Vec<u8> = row.get(1).expect("operator state rows carry a blob payload");
			items.push((EncodedKey::new(key), decode_row(bytes)));
		}
		record_page(items.len() as u64, 0);

		let has_more = items.len() as u64 > limit;
		items.truncate(limit as usize);
		OperatorBatch {
			items,
			has_more,
		}
	}

	#[instrument(name = "store::operator::bytes", level = "trace", skip(self), fields(operator = operator.0), ret)]
	pub fn bytes(&self, operator: OperatorId) -> u64 {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return 0;
		};
		let state = conn
			.query_row(
				r#"SELECT COALESCE(SUM(LENGTH("key") + LENGTH("bytes")), 0) FROM "operator_state"
				   WHERE "operator" = ?1"#,
				params![operator.0 as i64],
				|row| row.get::<_, i64>(0),
			)
			.expect("operator state size query failed") as u64;
		let anchors = conn
			.query_row(
				r#"SELECT COUNT(*) FROM "operator_seal_anchor" WHERE "operator" = ?1"#,
				params![operator.0 as i64],
				|row| row.get::<_, i64>(0),
			)
			.expect("seal anchor size query failed") as u64;
		state + anchors * (ANCHOR_KEY_BYTES + ANCHOR_VALUE_BYTES)
	}

	#[instrument(name = "store::operator::total_bytes", level = "trace", skip(self), ret)]
	pub fn total_bytes(&self) -> u64 {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return 0;
		};
		let state = conn
			.query_row(
				r#"SELECT COALESCE(SUM(LENGTH("key") + LENGTH("bytes")), 0) FROM "operator_state""#,
				[],
				|row| row.get::<_, i64>(0),
			)
			.expect("operator state size query failed") as u64;
		let anchors = conn
			.query_row(r#"SELECT COUNT(*) FROM "operator_seal_anchor""#, [], |row| row.get::<_, i64>(0))
			.expect("seal anchor size query failed") as u64;
		state + anchors * (ANCHOR_KEY_BYTES + ANCHOR_VALUE_BYTES)
	}

	#[instrument(name = "store::operator::census", level = "debug", skip(self))]
	pub fn census(&self) -> Vec<OperatorStateCensus> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt = conn
			.prepare_cached(
				r#"SELECT "operator", "keyspace", "keys", "key_bytes", "value_bytes"
				   FROM "operator_state_census"
				   WHERE "keys" > 0
				   ORDER BY "operator", "keyspace""#,
			)
			.expect("operator state census could not be prepared");
		let mut rows = stmt.query([]).expect("operator state census failed");

		let mut out = Vec::new();
		while let Some(row) = rows.next().expect("operator state census failed") {
			let stored: Vec<u8> = row.get(1).expect("census rows carry a keyspace byte");
			out.push(OperatorStateCensus {
				operator: OperatorId(
					row.get::<_, i64>(0).expect("census rows carry an operator") as u64
				),
				keyspace: OperatorStateKey::decode_keyspace(
					*stored.first().expect("state keys carry a keyspace byte"),
				),
				keys: row.get::<_, i64>(2).expect("census rows carry a key count") as u64,
				key_bytes: row.get::<_, i64>(3).expect("census rows carry a key byte sum") as u64,
				value_bytes: row.get::<_, i64>(4).expect("census rows carry a value byte sum") as u64,
			});
		}
		out
	}

	#[instrument(name = "store::operator::anchor_get", level = "trace", skip(self), fields(operator = operator.0, group = group.0, side = side))]
	pub fn anchor_get(
		&self,
		operator: OperatorId,
		group: GroupId,
		side: u8,
		row_number: RowNumber,
	) -> Option<DateTime> {
		let guard = self.read_conn();
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

	#[instrument(name = "store::operator::anchors_by_expiry", level = "trace", skip(self), fields(operator = operator.0, group = group.0, limit = limit))]
	pub fn anchors_by_expiry(&self, operator: OperatorId, group: GroupId, limit: u64) -> Vec<OperatorSealAnchor> {
		let guard = self.read_conn();
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

	#[instrument(name = "store::operator::anchors_due", level = "trace", skip(self, at), fields(operator = operator.0, group = group.0, limit = limit))]
	pub fn anchors_due(
		&self,
		operator: OperatorId,
		group: GroupId,
		at: DateTime,
		limit: u64,
	) -> Vec<OperatorSealAnchor> {
		let guard = self.read_conn();
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

	#[instrument(name = "store::operator::anchor_census", level = "debug", skip(self))]
	pub fn anchor_census(&self) -> Vec<OperatorSealAnchorCensus> {
		let guard = self.read_conn();
		let Some(conn) = guard.as_ref() else {
			return Vec::new();
		};
		let mut stmt = conn
			.prepare_cached(
				r#"SELECT "operator", COUNT(*) FROM "operator_seal_anchor"
				   GROUP BY "operator" ORDER BY "operator""#,
			)
			.expect("seal anchor census could not be prepared");
		let mut rows = stmt.query([]).expect("seal anchor census failed");

		let mut out = Vec::new();
		while let Some(row) = rows.next().expect("seal anchor census failed") {
			out.push(OperatorSealAnchorCensus {
				operator: OperatorId(
					row.get::<_, i64>(0).expect("census rows carry an operator") as u64
				),
				keys: row.get::<_, i64>(1).expect("census rows carry a key count") as u64,
			});
		}
		out
	}

	#[instrument(name = "store::operator::anchor_set", level = "debug", skip(self, expiry), fields(operator = operator.0, group = group.0, side = side))]
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
		conn.prepare_cached(ANCHOR_SET_SQL)
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

	#[instrument(name = "store::operator::anchor_remove", level = "debug", skip(self), fields(operator = operator.0, group = group.0, side = side))]
	pub fn anchor_remove(&self, operator: OperatorId, group: GroupId, side: u8, row_number: RowNumber) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		conn.prepare_cached(ANCHOR_REMOVE_SQL)
			.expect("seal anchor delete could not be prepared")
			.execute(params![operator.0 as i64, group.0 as i64, side as i64, row_number.0 as i64])
			.expect("seal anchor delete failed");
	}

	#[instrument(name = "store::operator::anchors_remove_group", level = "debug", skip(self), fields(operator = operator.0, group = group.0))]
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

	#[instrument(name = "store::operator::anchors_drop_operator", level = "debug", skip(self), fields(operator = operator.0))]
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

	#[instrument(name = "store::operator::drop_operator_state", level = "debug", skip(self), fields(operator = operator.0))]
	pub fn drop_operator_state(&self, operator: OperatorId) {
		let guard = self.inner.conn.lock();
		let Some(conn) = guard.as_ref() else {
			return;
		};
		let transaction = conn.unchecked_transaction().expect("operator state drop could not begin");
		transaction
			.execute(r#"DELETE FROM "operator_state" WHERE "operator" = ?1"#, params![operator.0 as i64])
			.expect("operator state drop failed");
		transaction
			.execute(
				r#"DELETE FROM "operator_seal_anchor" WHERE "operator" = ?1"#,
				params![operator.0 as i64],
			)
			.expect("seal anchor drop failed");
		transaction.commit().expect("operator state drop could not commit");
	}
}

impl Shutdown for SqliteOperatorStorage {
	fn shutdown(&self) {
		self.inner.readers.shutdown();
		if let Some(conn) = self.inner.conn.lock().take() {
			let _ = conn.close();
		}
	}
}

struct OperatorPageCacheCollector {
	store: SqliteOperatorStorage,
}

impl MetricsCollector for OperatorPageCacheCollector {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let metrics = self.store.page_cache_metrics();
		out.push(MetricsSample::bytes(SQLITE_SCOPE, "page_cache_used_bytes", metrics.used));
		out.push(MetricsSample::counter(SQLITE_SCOPE, "page_cache_hit_count", metrics.hits.as_u64()));
		out.push(MetricsSample::counter(SQLITE_SCOPE, "page_cache_miss_count", metrics.misses.as_u64()));
		out.push(MetricsSample::count(
			SQLITE_SCOPE,
			"page_cache_sampled_connections",
			metrics.connections_sampled.as_u64(),
		));
		out.push(MetricsSample::count(SQLITE_SCOPE, "connections_total", metrics.connections_total.as_u64()));
	}
}

macro_rules! range_sql_variant {
	($($clause:expr),*) => {
		concat!(
			r#"SELECT "key", "bytes" FROM "operator_state" WHERE "operator" = ?1"#,
			$($clause,)*
		)
	};
}

fn range_sql(start: Bound<&EncodedKey>, end: Bound<&EncodedKey>) -> &'static str {
	match (start, end) {
		(Bound::Unbounded, Bound::Unbounded) => range_sql_variant!(r#" ORDER BY "key" ASC LIMIT ?2"#),
		(Bound::Unbounded, Bound::Included(_)) => {
			range_sql_variant!(r#" AND "key" <= ?2"#, r#" ORDER BY "key" ASC LIMIT ?3"#)
		}
		(Bound::Unbounded, Bound::Excluded(_)) => {
			range_sql_variant!(r#" AND "key" < ?2"#, r#" ORDER BY "key" ASC LIMIT ?3"#)
		}
		(Bound::Included(_), Bound::Unbounded) => {
			range_sql_variant!(r#" AND "key" >= ?2"#, r#" ORDER BY "key" ASC LIMIT ?3"#)
		}
		(Bound::Excluded(_), Bound::Unbounded) => {
			range_sql_variant!(r#" AND "key" > ?2"#, r#" ORDER BY "key" ASC LIMIT ?3"#)
		}
		(Bound::Included(_), Bound::Included(_)) => {
			range_sql_variant!(
				r#" AND "key" >= ?2"#,
				r#" AND "key" <= ?3"#,
				r#" ORDER BY "key" ASC LIMIT ?4"#
			)
		}
		(Bound::Included(_), Bound::Excluded(_)) => {
			range_sql_variant!(
				r#" AND "key" >= ?2"#,
				r#" AND "key" < ?3"#,
				r#" ORDER BY "key" ASC LIMIT ?4"#
			)
		}
		(Bound::Excluded(_), Bound::Included(_)) => {
			range_sql_variant!(
				r#" AND "key" > ?2"#,
				r#" AND "key" <= ?3"#,
				r#" ORDER BY "key" ASC LIMIT ?4"#
			)
		}
		(Bound::Excluded(_), Bound::Excluded(_)) => {
			range_sql_variant!(
				r#" AND "key" > ?2"#,
				r#" AND "key" < ?3"#,
				r#" ORDER BY "key" ASC LIMIT ?4"#
			)
		}
	}
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
	let keyspace = OperatorStateKey::KEYSPACE_INNER_OFFSET + 1;
	conn.execute_batch(&format!(r#"CREATE TABLE IF NOT EXISTS "operator_state" (
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
			ON "operator_seal_anchor" ("operator", "group", "expiry");

		CREATE TABLE IF NOT EXISTS "operator_state_census" (
			"operator" INTEGER NOT NULL,
			"keyspace" BLOB NOT NULL,
			"keys" INTEGER NOT NULL,
			"key_bytes" INTEGER NOT NULL,
			"value_bytes" INTEGER NOT NULL,
			PRIMARY KEY ("operator", "keyspace")
		) WITHOUT ROWID;

		CREATE TRIGGER IF NOT EXISTS "operator_state_census_insert"
		AFTER INSERT ON "operator_state" BEGIN
			INSERT INTO "operator_state_census"
				("operator", "keyspace", "keys", "key_bytes", "value_bytes")
			VALUES (NEW."operator", substr(NEW."key", {keyspace}, 1), 1,
				LENGTH(NEW."key"), LENGTH(NEW."bytes"))
			ON CONFLICT ("operator", "keyspace") DO UPDATE SET
				"keys" = "keys" + 1,
				"key_bytes" = "key_bytes" + LENGTH(NEW."key"),
				"value_bytes" = "value_bytes" + LENGTH(NEW."bytes");
		END;

		CREATE TRIGGER IF NOT EXISTS "operator_state_census_update"
		AFTER UPDATE ON "operator_state" BEGIN
			UPDATE "operator_state_census"
			SET "value_bytes" = "value_bytes" - LENGTH(OLD."bytes") + LENGTH(NEW."bytes")
			WHERE "operator" = NEW."operator"
			  AND "keyspace" = substr(NEW."key", {keyspace}, 1);
		END;

		CREATE TRIGGER IF NOT EXISTS "operator_state_census_delete"
		AFTER DELETE ON "operator_state" BEGIN
			UPDATE "operator_state_census"
			SET "keys" = "keys" - 1,
			    "key_bytes" = "key_bytes" - LENGTH(OLD."key"),
			    "value_bytes" = "value_bytes" - LENGTH(OLD."bytes")
			WHERE "operator" = OLD."operator"
			  AND "keyspace" = substr(OLD."key", {keyspace}, 1);
		END;"#))
		.expect("operator state schema could not be created");

	seed_census(conn);
}

fn seed_census(conn: &Connection) {
	let seeded: i64 = conn
		.query_row(r#"SELECT COUNT(*) FROM "operator_state_census""#, [], |row| row.get(0))
		.expect("operator state census count failed");
	if seeded > 0 {
		return;
	}
	let keyspace = OperatorStateKey::KEYSPACE_INNER_OFFSET + 1;
	conn.execute(
		&format!(r#"INSERT INTO "operator_state_census"
				("operator", "keyspace", "keys", "key_bytes", "value_bytes")
			   SELECT "operator", substr("key", {keyspace}, 1), COUNT(*),
			          SUM(LENGTH("key")), SUM(LENGTH("bytes"))
			   FROM "operator_state"
			   GROUP BY "operator", substr("key", {keyspace}, 1)"#),
		[],
	)
	.expect("operator state census could not be seeded");
}
