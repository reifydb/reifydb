// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{bytes::EncodedBytes, operator::EncodedOperatorRow},
};
use reifydb_core::{common::CommitVersion, interface::catalog::flow::OperatorId, internal_error};
use reifydb_runtime::sync::mutex::Mutex;
use reifydb_sqlite::{
	SqliteConfig,
	connection::{connect, convert_flags, resolve_db_path},
	pragma,
};
use reifydb_value::{Result, util::cowvec::CowVec};
use rusqlite::{Connection, OptionalExtension, Result as SqliteResult, Transaction as SqliteTransaction, params};
use xxhash_rust::xxh3::Xxh3;

pub const DEFAULT_SNAPSHOT_CHUNK_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotManifest {
	pub operator: OperatorId,
	pub generation: u64,
	pub upper: CommitVersion,
	pub flow_cursor: CommitVersion,
	pub content_hash: u64,
	pub dictionary_max: Vec<(u64, u128)>,
	pub chunk_count: u64,
}

#[derive(Debug, Clone)]
pub struct LoadedSnapshot {
	pub manifest: SnapshotManifest,
	pub entries: Vec<(EncodedKey, EncodedOperatorRow)>,
}

#[derive(Debug, Clone, Copy)]
pub struct SnapshotWrite<'a> {
	pub operator: OperatorId,
	pub upper: CommitVersion,
	pub flow_cursor: CommitVersion,
	pub dictionary_max: &'a [(u64, u128)],
	pub chunk_bytes: usize,
}

#[derive(Clone)]
pub struct SnapshotStore {
	inner: Arc<SnapshotStoreInner>,
}

struct SnapshotStoreInner {
	conn: Mutex<Option<Connection>>,
}

impl SnapshotStore {
	pub fn sqlite(config: SqliteConfig) -> Self {
		let db_path = resolve_db_path(config.path.clone(), "operator.db");
		let flags = convert_flags(&config.flags);
		let conn = connect(&db_path, flags).expect("Failed to connect to operator snapshot database");
		pragma::apply(&conn, &config).expect("Failed to configure operator snapshot SQLite pragmas");
		ensure_schema(&conn);
		Self {
			inner: Arc::new(SnapshotStoreInner {
				conn: Mutex::new(Some(conn)),
			}),
		}
	}

	pub fn write(
		&self,
		write: SnapshotWrite<'_>,
		entries: &mut dyn Iterator<Item = Result<(EncodedKey, EncodedOperatorRow)>>,
	) -> Result<u64> {
		let chunk_bytes = write.chunk_bytes.max(1);
		let mut guard = self.inner.conn.lock();
		let conn = guard.as_mut().ok_or_else(|| internal_error!("operator snapshot connection is closed"))?;
		let txn = conn.transaction().map_err(|e| internal_error!("snapshot begin failed: {}", e))?;

		let operator = write.operator.0 as i64;
		let generation = txn
			.query_row(
				r#"SELECT COALESCE(MAX(generation), 0) FROM "snapshot_manifest" WHERE operator = ?1"#,
				params![operator],
				|row| row.get::<_, i64>(0),
			)
			.map_err(|e| internal_error!("snapshot generation lookup failed: {}", e))? as u64
			+ 1;

		let mut hasher = Xxh3::new();
		let mut buffer: Vec<u8> = Vec::new();
		let mut seq: u64 = 0;
		for entry in entries {
			let (key, row) = entry?;
			encode_entry(&mut buffer, &key, &row);
			if buffer.len() >= chunk_bytes {
				insert_chunk(&txn, operator, generation, seq, &buffer, &mut hasher)?;
				buffer.clear();
				seq += 1;
			}
		}
		if !buffer.is_empty() {
			insert_chunk(&txn, operator, generation, seq, &buffer, &mut hasher)?;
			seq += 1;
		}

		txn.execute(
			r#"INSERT INTO "snapshot_manifest"
			   (operator, generation, upper, flow_cursor, content_hash, dictionary_max, chunk_count, created_at)
			   VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, CAST(strftime('%s', 'now') AS INTEGER))"#,
			params![
				operator,
				generation as i64,
				write.upper.0 as i64,
				write.flow_cursor.0 as i64,
				hasher.digest().to_be_bytes().as_slice(),
				encode_dictionary_max(write.dictionary_max),
				seq as i64,
			],
		)
		.map_err(|e| internal_error!("snapshot manifest insert failed: {}", e))?;

		let retained_floor = generation.saturating_sub(1) as i64;
		txn.execute(
			r#"DELETE FROM "snapshot_chunk" WHERE operator = ?1 AND generation < ?2"#,
			params![operator, retained_floor],
		)
		.map_err(|e| internal_error!("snapshot chunk prune failed: {}", e))?;
		txn.execute(
			r#"DELETE FROM "snapshot_manifest" WHERE operator = ?1 AND generation < ?2"#,
			params![operator, retained_floor],
		)
		.map_err(|e| internal_error!("snapshot manifest prune failed: {}", e))?;

		txn.commit().map_err(|e| internal_error!("snapshot commit failed: {}", e))?;
		Ok(generation)
	}

	pub fn generations(&self, operator: OperatorId) -> Result<Vec<u64>> {
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref().ok_or_else(|| internal_error!("operator snapshot connection is closed"))?;
		let mut stmt = conn
			.prepare_cached(
				r#"SELECT generation FROM "snapshot_manifest" WHERE operator = ?1 ORDER BY generation DESC"#,
			)
			.map_err(|e| internal_error!("snapshot generation query failed: {}", e))?;
		let generations = stmt
			.query_map(params![operator.0 as i64], |row| row.get::<_, i64>(0))
			.and_then(|rows| rows.collect::<SqliteResult<Vec<i64>>>())
			.map_err(|e| internal_error!("snapshot generation scan failed: {}", e))?;
		Ok(generations.into_iter().map(|generation| generation as u64).collect())
	}

	pub fn generation_cursors(&self, operator: OperatorId) -> Result<Vec<(u64, CommitVersion)>> {
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref().ok_or_else(|| internal_error!("operator snapshot connection is closed"))?;
		let mut stmt = conn
			.prepare_cached(
				r#"SELECT generation, flow_cursor FROM "snapshot_manifest"
				   WHERE operator = ?1 ORDER BY generation DESC"#,
			)
			.map_err(|e| internal_error!("snapshot cursor query failed: {}", e))?;
		let rows = stmt
			.query_map(params![operator.0 as i64], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
			.and_then(|rows| rows.collect::<SqliteResult<Vec<(i64, i64)>>>())
			.map_err(|e| internal_error!("snapshot cursor scan failed: {}", e))?;
		Ok(rows.into_iter()
			.map(|(generation, cursor)| (generation as u64, CommitVersion(cursor as u64)))
			.collect())
	}

	pub fn operators(&self) -> Result<Vec<OperatorId>> {
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref().ok_or_else(|| internal_error!("operator snapshot connection is closed"))?;
		let mut stmt = conn
			.prepare_cached(r#"SELECT DISTINCT operator FROM "snapshot_manifest" ORDER BY operator ASC"#)
			.map_err(|e| internal_error!("snapshot operator query failed: {}", e))?;
		let operators = stmt
			.query_map([], |row| row.get::<_, i64>(0))
			.and_then(|rows| rows.collect::<SqliteResult<Vec<i64>>>())
			.map_err(|e| internal_error!("snapshot operator scan failed: {}", e))?;
		Ok(operators.into_iter().map(|operator| OperatorId(operator as u64)).collect())
	}

	pub fn load(&self, operator: OperatorId, generation: u64) -> Result<LoadedSnapshot> {
		let guard = self.inner.conn.lock();
		let conn = guard.as_ref().ok_or_else(|| internal_error!("operator snapshot connection is closed"))?;

		let manifest = conn
			.query_row(
				r#"SELECT upper, flow_cursor, content_hash, dictionary_max, chunk_count FROM "snapshot_manifest"
				   WHERE operator = ?1 AND generation = ?2"#,
				params![operator.0 as i64, generation as i64],
				|row| {
					Ok((
						row.get::<_, i64>(0)?,
						row.get::<_, i64>(1)?,
						row.get::<_, Vec<u8>>(2)?,
						row.get::<_, Vec<u8>>(3)?,
						row.get::<_, i64>(4)?,
					))
				},
			)
			.optional()
			.map_err(|e| internal_error!("snapshot manifest read failed: {}", e))?;
		let Some((upper, flow_cursor, content_hash, dictionary_max, chunk_count)) = manifest else {
			return Err(internal_error!(
				"snapshot manifest missing for operator {} generation {}",
				operator.0,
				generation
			));
		};
		let content_hash: [u8; 8] = content_hash
			.try_into()
			.map_err(|_| internal_error!("snapshot manifest carries a malformed content hash"))?;
		let content_hash = u64::from_be_bytes(content_hash);
		let dictionary_max = decode_dictionary_max(&dictionary_max)
			.ok_or_else(|| internal_error!("snapshot manifest carries a malformed dictionary record"))?;

		let mut stmt = conn
			.prepare_cached(
				r#"SELECT seq, bytes FROM "snapshot_chunk"
				   WHERE operator = ?1 AND generation = ?2 ORDER BY seq ASC"#,
			)
			.map_err(|e| internal_error!("snapshot chunk query failed: {}", e))?;
		let chunks = stmt
			.query_map(params![operator.0 as i64, generation as i64], |row| {
				Ok((row.get::<_, i64>(0)?, row.get::<_, Vec<u8>>(1)?))
			})
			.and_then(|rows| rows.collect::<SqliteResult<Vec<(i64, Vec<u8>)>>>())
			.map_err(|e| internal_error!("snapshot chunk scan failed: {}", e))?;

		if chunks.len() as u64 != chunk_count as u64 {
			return Err(internal_error!(
				"snapshot for operator {} generation {} has {} chunks, manifest expects {}",
				operator.0,
				generation,
				chunks.len(),
				chunk_count
			));
		}

		let mut hasher = Xxh3::new();
		let mut entries: Vec<(EncodedKey, EncodedOperatorRow)> = Vec::new();
		for (expected_seq, (seq, bytes)) in chunks.iter().enumerate() {
			if *seq as u64 != expected_seq as u64 {
				return Err(internal_error!(
					"snapshot for operator {} generation {} has a chunk gap at seq {}",
					operator.0,
					generation,
					expected_seq
				));
			}
			hasher.update(bytes);
			decode_entries(bytes, &mut entries).ok_or_else(|| {
				internal_error!(
					"snapshot for operator {} generation {} has a malformed chunk at seq {}",
					operator.0,
					generation,
					expected_seq
				)
			})?;
		}

		if hasher.digest() != content_hash {
			return Err(internal_error!(
				"snapshot for operator {} generation {} fails its content hash check",
				operator.0,
				generation
			));
		}

		Ok(LoadedSnapshot {
			manifest: SnapshotManifest {
				operator,
				generation,
				upper: CommitVersion(upper as u64),
				flow_cursor: CommitVersion(flow_cursor as u64),
				content_hash,
				dictionary_max,
				chunk_count: chunk_count as u64,
			},
			entries,
		})
	}

	pub fn discard(&self, operator: OperatorId, generation: u64) -> Result<()> {
		let mut guard = self.inner.conn.lock();
		let conn = guard.as_mut().ok_or_else(|| internal_error!("operator snapshot connection is closed"))?;
		let txn = conn.transaction().map_err(|e| internal_error!("snapshot discard begin failed: {}", e))?;
		txn.execute(
			r#"DELETE FROM "snapshot_chunk" WHERE operator = ?1 AND generation = ?2"#,
			params![operator.0 as i64, generation as i64],
		)
		.map_err(|e| internal_error!("snapshot chunk discard failed: {}", e))?;
		txn.execute(
			r#"DELETE FROM "snapshot_manifest" WHERE operator = ?1 AND generation = ?2"#,
			params![operator.0 as i64, generation as i64],
		)
		.map_err(|e| internal_error!("snapshot manifest discard failed: {}", e))?;
		txn.commit().map_err(|e| internal_error!("snapshot discard commit failed: {}", e))
	}
}

fn ensure_schema(conn: &Connection) {
	conn.execute(
		r#"CREATE TABLE IF NOT EXISTS "snapshot_manifest" (
			operator INTEGER NOT NULL,
			generation INTEGER NOT NULL,
			upper INTEGER NOT NULL,
			flow_cursor INTEGER NOT NULL,
			content_hash BLOB NOT NULL,
			dictionary_max BLOB NOT NULL,
			chunk_count INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (operator, generation)
		) WITHOUT ROWID"#,
		[],
	)
	.expect("Failed to create snapshot manifest table");
	conn.execute(
		r#"CREATE TABLE IF NOT EXISTS "snapshot_chunk" (
			operator INTEGER NOT NULL,
			generation INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			bytes BLOB NOT NULL,
			PRIMARY KEY (operator, generation, seq)
		) WITHOUT ROWID"#,
		[],
	)
	.expect("Failed to create snapshot chunk table");
}

fn insert_chunk(
	txn: &SqliteTransaction<'_>,
	operator: i64,
	generation: u64,
	seq: u64,
	bytes: &[u8],
	hasher: &mut Xxh3,
) -> Result<()> {
	hasher.update(bytes);
	txn.execute(
		r#"INSERT INTO "snapshot_chunk" (operator, generation, seq, bytes) VALUES (?1, ?2, ?3, ?4)"#,
		params![operator, generation as i64, seq as i64, bytes],
	)
	.map_err(|e| internal_error!("snapshot chunk insert failed: {}", e))?;
	Ok(())
}

fn encode_entry(buffer: &mut Vec<u8>, key: &EncodedKey, row: &EncodedOperatorRow) {
	buffer.extend_from_slice(&(key.as_slice().len() as u32).to_le_bytes());
	buffer.extend_from_slice(key.as_slice());
	buffer.extend_from_slice(&(row.bytes().as_slice().len() as u32).to_le_bytes());
	buffer.extend_from_slice(row.bytes().as_slice());
}

fn decode_entries(bytes: &[u8], entries: &mut Vec<(EncodedKey, EncodedOperatorRow)>) -> Option<()> {
	let mut offset = 0usize;
	while offset < bytes.len() {
		let key = decode_field(bytes, &mut offset)?;
		let row = decode_field(bytes, &mut offset)?;
		let row = EncodedOperatorRow::try_from(EncodedBytes(CowVec::new(row.to_vec()))).ok()?;
		entries.push((EncodedKey::new(key), row));
	}
	Some(())
}

fn decode_field<'a>(bytes: &'a [u8], offset: &mut usize) -> Option<&'a [u8]> {
	let len_end = offset.checked_add(4)?;
	let len = u32::from_le_bytes(bytes.get(*offset..len_end)?.try_into().ok()?) as usize;
	let field_end = len_end.checked_add(len)?;
	let field = bytes.get(len_end..field_end)?;
	*offset = field_end;
	Some(field)
}

fn encode_dictionary_max(entries: &[(u64, u128)]) -> Vec<u8> {
	let mut bytes = Vec::with_capacity(4 + entries.len() * 24);
	bytes.extend_from_slice(&(entries.len() as u32).to_le_bytes());
	for (dictionary, max) in entries {
		bytes.extend_from_slice(&dictionary.to_le_bytes());
		bytes.extend_from_slice(&max.to_le_bytes());
	}
	bytes
}

fn decode_dictionary_max(bytes: &[u8]) -> Option<Vec<(u64, u128)>> {
	let count = u32::from_le_bytes(bytes.get(0..4)?.try_into().ok()?) as usize;
	let mut entries = Vec::with_capacity(count);
	let mut offset = 4usize;
	for _ in 0..count {
		let dictionary = u64::from_le_bytes(bytes.get(offset..offset + 8)?.try_into().ok()?);
		let max = u128::from_le_bytes(bytes.get(offset + 8..offset + 24)?.try_into().ok()?);
		entries.push((dictionary, max));
		offset += 24;
	}
	(offset == bytes.len()).then_some(entries)
}
