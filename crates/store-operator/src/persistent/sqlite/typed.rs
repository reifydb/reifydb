// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{keyspace::KeyspaceSpec, state::GroupId, traits::Keyspace},
		typed::{
			direction::Direction,
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue},
			range::KeyRange,
		},
	},
};
use reifydb_sqlite::batch::values_placeholders;
use rusqlite::{Connection, Error as SqliteError, Row, Transaction, params_from_iter, types::Value};
use tracing::instrument;

pub fn table_of(name: &str, operator: OperatorId) -> String {
	format!("operator_{}_{}", name.to_ascii_lowercase(), operator.0)
}

pub fn create_table(spec: &KeyspaceSpec, operator: OperatorId) -> String {
	let mut sql = format!("CREATE TABLE \"{}\" (\n", table_of(spec.name, operator));
	for column in spec.columns {
		sql.push_str(&format!("\t\"{}\" {} NOT NULL,\n", column.name, sql_type(column.ty)));
	}
	if spec.columns.is_empty() {
		sql.push_str("\t\"unit\" INTEGER NOT NULL DEFAULT 0,\n");
	}
	let key = primary_key(spec.columns);
	sql.push_str(&format!("\t\"bytes\" BLOB NOT NULL,\n\tPRIMARY KEY ({key})\n) WITHOUT ROWID;"));
	sql
}

fn primary_key(columns: &[KeyColumn]) -> String {
	match columns.is_empty() {
		true => "\"unit\"".to_string(),
		false => columns.iter().map(|column| format!("\"{}\"", column.name)).collect::<Vec<_>>().join(", "),
	}
}

fn sql_type(ty: KeyColumnType) -> &'static str {
	match ty {
		KeyColumnType::U8 => "INTEGER",
		KeyColumnType::U64 | KeyColumnType::Blob16 | KeyColumnType::Blob24 => "BLOB",
	}
}

fn flip(direction: Direction, byte: u8) -> u8 {
	match direction {
		Direction::Asc => byte,
		Direction::Desc => !byte,
	}
}

fn flipped(direction: Direction, bytes: &[u8]) -> Vec<u8> {
	bytes.iter().map(|byte| flip(direction, *byte)).collect()
}

pub fn to_sql(value: KeyValue, direction: Direction) -> Value {
	match value {
		KeyValue::U8(v) => Value::Integer(i64::from(flip(direction, v))),
		KeyValue::U64(v) => Value::Blob(flipped(direction, &v.to_be_bytes())),
		KeyValue::Blob16(v) => Value::Blob(flipped(direction, &v)),
		KeyValue::Blob24(v) => Value::Blob(flipped(direction, &v)),
	}
}

pub fn from_sql(row: &Row<'_>, at: usize, column: &KeyColumn) -> Option<KeyValue> {
	match column.ty {
		KeyColumnType::U8 => row
			.get::<_, i64>(at)
			.ok()
			.and_then(|v| u8::try_from(v).ok())
			.map(|v| KeyValue::U8(flip(column.direction, v))),
		KeyColumnType::U64 => {
			let blob: Vec<u8> = row.get(at).ok()?;
			<[u8; 8]>::try_from(flipped(column.direction, &blob).as_slice())
				.ok()
				.map(|bytes| KeyValue::U64(u64::from_be_bytes(bytes)))
		}
		KeyColumnType::Blob16 => {
			let blob: Vec<u8> = row.get(at).ok()?;
			<[u8; 16]>::try_from(flipped(column.direction, &blob).as_slice()).ok().map(KeyValue::Blob16)
		}
		KeyColumnType::Blob24 => {
			let blob: Vec<u8> = row.get(at).ok()?;
			<[u8; 24]>::try_from(flipped(column.direction, &blob).as_slice()).ok().map(KeyValue::Blob24)
		}
	}
}

pub trait SqlKey: Keyspace {
	fn table(operator: OperatorId) -> String;

	fn bind_key(key: &Self::GroupedKey) -> Vec<Value>;

	fn read_key(row: &Row<'_>, at: usize) -> Option<Self::GroupedKey>;

	fn columns() -> &'static [KeyColumn];

	fn column_list() -> String;

	fn placeholders(from: usize) -> String;

	fn key_predicate(from: usize) -> String;

	fn key_columns() -> String;

	fn key_values(from: usize) -> String;

	fn ordering(order: &str) -> String;

	fn key_in(rows: usize) -> String;
}

impl<K: Keyspace> SqlKey for K {
	fn table(operator: OperatorId) -> String {
		table_of(K::NAME, operator)
	}

	fn bind_key(key: &Self::GroupedKey) -> Vec<Value> {
		key.key_values()
			.into_iter()
			.zip(Self::columns())
			.map(|(value, column)| to_sql(value, column.direction))
			.collect()
	}

	fn read_key(row: &Row<'_>, at: usize) -> Option<Self::GroupedKey> {
		let mut values = Vec::with_capacity(Self::columns().len());
		for (offset, column) in Self::columns().iter().enumerate() {
			values.push(from_sql(row, at + offset, column)?);
		}
		<Self::GroupedKey as KeyLayout>::from_key_values(&values)
	}

	fn columns() -> &'static [KeyColumn] {
		<Self::GroupedKey as KeyLayout>::COLUMNS
	}

	fn column_list() -> String {
		Self::columns().iter().map(|column| format!("\"{}\"", column.name)).collect::<Vec<_>>().join(", ")
	}

	fn placeholders(from: usize) -> String {
		(0..Self::columns().len()).map(|at| format!("?{}", from + at)).collect::<Vec<_>>().join(", ")
	}

	fn key_predicate(from: usize) -> String {
		Self::columns()
			.iter()
			.enumerate()
			.map(|(at, column)| format!(" AND \"{}\" = ?{}", column.name, from + at))
			.collect::<Vec<_>>()
			.join("")
	}

	fn key_columns() -> String {
		match Self::columns().is_empty() {
			true => String::new(),
			false => format!("{}, ", Self::column_list()),
		}
	}

	fn key_values(from: usize) -> String {
		match Self::columns().is_empty() {
			true => String::new(),
			false => format!("{}, ", Self::placeholders(from)),
		}
	}

	fn ordering(order: &str) -> String {
		match Self::columns().is_empty() {
			true => String::new(),
			false => format!(
				" ORDER BY {}",
				Self::columns()
					.iter()
					.map(|column| format!("\"{}\" {}", column.name, order))
					.collect::<Vec<_>>()
					.join(", ")
			),
		}
	}

	fn key_in(rows: usize) -> String {
		match Self::columns().is_empty() {
			true => String::new(),
			false => format!(
				" WHERE ({}) IN (VALUES {})",
				Self::column_list(),
				values_placeholders(rows, Self::columns().len())
			),
		}
	}
}

pub const WRITE_CHUNK: usize = 100;

fn set_sql<K: Keyspace>(operator: OperatorId, rows: usize) -> String {
	let cols = K::columns().len() + 1;
	format!(
		"INSERT INTO \"{}\" ({}\"bytes\") VALUES {}\n\
		 ON CONFLICT ({}) DO UPDATE SET \"bytes\" = excluded.\"bytes\"",
		K::table(operator),
		K::key_columns(),
		values_placeholders(rows, cols),
		primary_key(K::columns())
	)
}

fn remove_sql<K: Keyspace>(operator: OperatorId, rows: usize) -> String {
	format!("DELETE FROM \"{}\"{}", K::table(operator), K::key_in(rows))
}

#[instrument(name = "store::operator::persistent::sqlite::set_chunked", level = "debug", skip_all, fields(row_count = rows.len()))]
pub fn set_chunked<K: Keyspace>(txn: &Transaction, operator: OperatorId, rows: &[(K::GroupedKey, Vec<u8>)]) {
	if rows.is_empty() {
		return;
	}
	let bind = |row: &(K::GroupedKey, Vec<u8>), params: &mut Vec<Value>| {
		params.extend(K::bind_key(&row.0));
		params.push(Value::Blob(row.1.clone()));
	};
	let mut chunks = rows.chunks_exact(WRITE_CHUNK);
	let chunk_sql = set_sql::<K>(operator, WRITE_CHUNK);
	for full in chunks.by_ref() {
		let mut params = Vec::with_capacity(WRITE_CHUNK * (K::columns().len() + 1));
		for row in full {
			bind(row, &mut params);
		}
		txn.prepare_cached(&chunk_sql)
			.expect("chunked operator state write could not be prepared")
			.execute(params_from_iter(params))
			.expect("chunked operator state write failed");
	}
	let rest = chunks.remainder();
	if rest.is_empty() {
		return;
	}
	let rest_sql = set_sql::<K>(operator, rest.len());
	let mut params = Vec::with_capacity(rest.len() * (K::columns().len() + 1));
	for row in rest {
		bind(row, &mut params);
	}
	txn.prepare_cached(&rest_sql)
		.expect("operator state write could not be prepared")
		.execute(params_from_iter(params))
		.expect("operator state write failed");
}

#[instrument(name = "store::operator::persistent::sqlite::remove_chunked", level = "debug", skip_all, fields(key_count = keys.len()))]
pub fn remove_chunked<K: Keyspace>(txn: &Transaction, operator: OperatorId, keys: &[K::GroupedKey]) {
	if keys.is_empty() {
		return;
	}
	let bind = |key: &K::GroupedKey, params: &mut Vec<Value>| {
		params.extend(K::bind_key(key));
	};
	let mut chunks = keys.chunks_exact(WRITE_CHUNK);
	let chunk_sql = remove_sql::<K>(operator, WRITE_CHUNK);
	for full in chunks.by_ref() {
		let mut params = Vec::with_capacity(WRITE_CHUNK * K::columns().len());
		for row in full {
			bind(row, &mut params);
		}
		txn.prepare_cached(&chunk_sql)
			.expect("chunked operator state delete could not be prepared")
			.execute(params_from_iter(params))
			.expect("chunked operator state delete failed");
	}
	let rest = chunks.remainder();
	if rest.is_empty() {
		return;
	}
	let rest_sql = remove_sql::<K>(operator, rest.len());
	let mut params = Vec::with_capacity(rest.len() * K::columns().len());
	for row in rest {
		bind(row, &mut params);
	}
	txn.prepare_cached(&rest_sql)
		.expect("operator state delete could not be prepared")
		.execute(params_from_iter(params))
		.expect("operator state delete failed");
}

pub fn get<K: Keyspace>(conn: &Connection, operator: OperatorId, key: &K::GroupedKey) -> Option<Vec<u8>> {
	let sql = format!("SELECT \"bytes\" FROM \"{}\" WHERE 1{}", K::table(operator), K::key_predicate(1));
	let params = K::bind_key(key);
	let mut stmt = conn.prepare_cached(&sql).expect("operator state get could not be prepared");
	match stmt.query_row(params_from_iter(params), |row| row.get::<_, Vec<u8>>(0)) {
		Ok(bytes) => Some(bytes),
		Err(SqliteError::QueryReturnedNoRows) => None,
		Err(err) => panic!("operator state read failed: {err}"),
	}
}

pub const READ_CHUNK: usize = 100;

fn get_batch_sql<K: Keyspace>(operator: OperatorId, rows: usize) -> String {
	format!("SELECT {}\"bytes\" FROM \"{}\"{}", K::key_columns(), K::table(operator), K::key_in(rows))
}

pub fn get_batch<K: Keyspace>(
	conn: &Connection,
	operator: OperatorId,
	keys: &[K::GroupedKey],
) -> Vec<(K::GroupedKey, Vec<u8>)> {
	let mut out = Vec::with_capacity(keys.len());
	for chunk in keys.chunks(READ_CHUNK) {
		let sql = get_batch_sql::<K>(operator, chunk.len());
		let mut params = Vec::with_capacity(chunk.len() * K::columns().len());
		for key in chunk {
			params.extend(K::bind_key(key));
		}
		let mut stmt = conn.prepare_cached(&sql).expect("operator state batch get could not be prepared");
		let mut rows = stmt.query(params_from_iter(params)).expect("operator state batch get failed");
		while let Some(row) = rows.next().expect("operator state batch get row failed") {
			let key = K::read_key(row, 0)
				.expect("an operator state row does not decode as its own key layout");
			let bytes: Vec<u8> = row.get(K::columns().len()).expect("operator state row has no payload");
			out.push((key, bytes));
		}
	}
	out
}

pub fn scan<K: Keyspace>(conn: &Connection, operator: OperatorId) -> Vec<(K::GroupedKey, Vec<u8>)> {
	let sql = format!("SELECT {}\"bytes\" FROM \"{}\"{}", K::key_columns(), K::table(operator), K::ordering("ASC"));
	let mut stmt = conn.prepare_cached(&sql).expect("operator state scan could not be prepared");
	let mut rows = stmt.query([]).expect("operator state scan failed");
	let mut out = Vec::new();
	while let Some(row) = rows.next().expect("operator state scan row failed") {
		let key = K::read_key(row, 0).expect("an operator state row does not decode as its own key layout");
		let bytes: Vec<u8> = row.get(K::columns().len()).expect("operator state row has no payload");
		out.push((key, bytes));
	}
	out
}

fn bound_clause<K: Keyspace>(
	bound: Bound<&K::GroupedKey>,
	op_included: &str,
	op_excluded: &str,
	from: usize,
) -> String {
	if K::columns().is_empty() {
		return String::new();
	}
	match bound {
		Bound::Unbounded => String::new(),
		Bound::Included(_) => {
			format!(" AND ({}) {} ({})", K::column_list(), op_included, K::placeholders(from))
		}
		Bound::Excluded(_) => {
			format!(" AND ({}) {} ({})", K::column_list(), op_excluded, K::placeholders(from))
		}
	}
}

fn bound_key<K: Keyspace>(bound: Bound<&K::GroupedKey>) -> Option<&K::GroupedKey> {
	match bound {
		Bound::Unbounded => None,
		Bound::Included(key) | Bound::Excluded(key) => Some(key),
	}
}

fn slots(count: usize, from: usize) -> String {
	(0..count).map(|at| format!("?{}", from + at)).collect::<Vec<_>>().join(", ")
}

fn group_column<K: Keyspace>() -> &'static KeyColumn {
	K::columns().first().expect("a group scoped keyspace must lead its key layout with the group column")
}

fn group_clause<K: Keyspace>(groups: usize, from: usize) -> String {
	format!(" AND \"{}\" IN ({})", group_column::<K>().name, slots(groups, from))
}

fn bind_groups<K: Keyspace>(groups: &[GroupId]) -> Vec<Value> {
	let direction = group_column::<K>().direction;
	groups.iter().map(|group| to_sql(KeyValue::Blob24(*group.as_bytes()), direction)).collect()
}

fn suffix_width<K: Keyspace>() -> usize {
	K::columns().len().saturating_sub(1)
}

fn suffix_list<K: Keyspace>() -> String {
	K::columns()[1..].iter().map(|column| format!("\"{}\"", column.name)).collect::<Vec<_>>().join(", ")
}

fn suffix_clause<K: Keyspace>(
	bound: Bound<&K::GroupedKey>,
	op_included: &str,
	op_excluded: &str,
	from: usize,
) -> String {
	let width = suffix_width::<K>();
	if width == 0 {
		return String::new();
	}
	match bound {
		Bound::Unbounded => String::new(),
		Bound::Included(_) => {
			format!(" AND ({}) {} ({})", suffix_list::<K>(), op_included, slots(width, from))
		}
		Bound::Excluded(_) => {
			format!(" AND ({}) {} ({})", suffix_list::<K>(), op_excluded, slots(width, from))
		}
	}
}

fn bind_suffix<K: Keyspace>(key: &K::GroupedKey) -> Vec<Value> {
	let mut values = K::bind_key(key);
	values.split_off(1)
}

fn bounded<K: Keyspace>(
	conn: &Connection,
	operator: OperatorId,
	range: &KeyRange<K::GroupedKey>,
	limit: u64,
	order: &str,
) -> Vec<(K::GroupedKey, Vec<u8>)> {
	let width = K::columns().len();
	let mut at = 1;
	let start = bound_clause::<K>(range.start.as_ref(), ">=", ">", at);
	if bound_key::<K>(range.start.as_ref()).is_some() {
		at += width;
	}
	let end = bound_clause::<K>(range.end.as_ref(), "<=", "<", at);
	let sql = format!(
		"SELECT {}\"bytes\" FROM \"{}\" WHERE 1{}{}{} LIMIT {}",
		K::key_columns(),
		K::table(operator),
		start,
		end,
		K::ordering(order),
		limit as i64
	);
	let mut params = Vec::new();
	if !K::columns().is_empty() {
		for bound in [range.start.as_ref(), range.end.as_ref()] {
			if let Some(key) = bound_key::<K>(bound) {
				params.extend(K::bind_key(key));
			}
		}
	}
	let mut stmt = conn.prepare_cached(&sql).expect("operator state range could not be prepared");
	let mut rows = stmt.query(params_from_iter(params)).expect("operator state range failed");
	let mut out = Vec::new();
	while let Some(row) = rows.next().expect("operator state range row failed") {
		let key = K::read_key(row, 0).expect("an operator state row does not decode as its own key layout");
		let bytes: Vec<u8> = row.get(width).expect("operator state row has no payload");
		out.push((key, bytes));
	}
	out
}

pub fn keys_after<K: Keyspace>(
	conn: &Connection,
	operator: OperatorId,
	after: Option<&K::GroupedKey>,
	limit: u64,
) -> Vec<K::GroupedKey> {
	let start = bound_clause::<K>(after.map_or(Bound::Unbounded, Bound::Excluded), ">=", ">", 1);
	let columns = match K::columns().is_empty() {
		true => "1".to_string(),
		false => K::column_list(),
	};
	let sql = format!(
		"SELECT {} FROM \"{}\" WHERE 1{}{} LIMIT {}",
		columns,
		K::table(operator),
		start,
		K::ordering("ASC"),
		limit as i64
	);
	let mut params = Vec::new();
	if let Some(key) = after
		&& !K::columns().is_empty()
	{
		params.extend(K::bind_key(key));
	}
	let mut stmt = conn.prepare_cached(&sql).expect("operator state key scan could not be prepared");
	let mut rows = stmt.query(params_from_iter(params)).expect("operator state key scan failed");
	let mut out = Vec::new();
	while let Some(row) = rows.next().expect("operator state key scan row failed") {
		out.push(K::read_key(row, 0).expect("an operator state row does not decode as its own key layout"));
	}
	out
}

pub fn range<K: Keyspace>(
	conn: &Connection,
	operator: OperatorId,
	range: &KeyRange<K::GroupedKey>,
	limit: u64,
) -> Vec<(K::GroupedKey, Vec<u8>)> {
	bounded::<K>(conn, operator, range, limit, "ASC")
}

pub fn last<K: Keyspace>(
	conn: &Connection,
	operator: OperatorId,
	range: &KeyRange<K::GroupedKey>,
	limit: u64,
) -> Vec<(K::GroupedKey, Vec<u8>)> {
	bounded::<K>(conn, operator, range, limit, "DESC")
}

pub fn range_in<K: Keyspace>(
	conn: &Connection,
	operator: OperatorId,
	groups: &[GroupId],
	range: &KeyRange<K::GroupedKey>,
	limit: u64,
	order: &str,
) -> Vec<(K::GroupedKey, Vec<u8>)> {
	let width = K::columns().len();
	let suffix = suffix_width::<K>();
	let mut at = 1;
	let groups_clause = group_clause::<K>(groups.len(), at);
	at += groups.len();
	let start = suffix_clause::<K>(range.start.as_ref(), ">=", ">", at);
	if bound_key::<K>(range.start.as_ref()).is_some() {
		at += suffix;
	}
	let end = suffix_clause::<K>(range.end.as_ref(), "<=", "<", at);
	let sql = format!(
		"SELECT {}\"bytes\" FROM \"{}\" WHERE 1{}{}{}{} LIMIT {}",
		K::key_columns(),
		K::table(operator),
		groups_clause,
		start,
		end,
		K::ordering(order),
		limit as i64
	);
	let mut params = Vec::new();
	params.extend(bind_groups::<K>(groups));
	if suffix > 0 {
		for bound in [range.start.as_ref(), range.end.as_ref()] {
			if let Some(key) = bound_key::<K>(bound) {
				params.extend(bind_suffix::<K>(key));
			}
		}
	}
	let mut stmt = conn.prepare_cached(&sql).expect("operator state group range could not be prepared");
	let mut rows = stmt.query(params_from_iter(params)).expect("operator state group range failed");
	let mut out = Vec::new();
	while let Some(row) = rows.next().expect("operator state group range row failed") {
		let key = K::read_key(row, 0).expect("an operator state row does not decode as its own key layout");
		let bytes: Vec<u8> = row.get(width).expect("operator state row has no payload");
		out.push((key, bytes));
	}
	out
}

pub fn census<K: Keyspace>(conn: &Connection, operator: OperatorId) -> (u64, u64) {
	let sql = format!("SELECT COUNT(*), COALESCE(SUM(LENGTH(\"bytes\")), 0) FROM \"{}\"", K::table(operator));
	let mut stmt = conn.prepare_cached(&sql).expect("operator state census could not be prepared");
	let (keys, bytes): (i64, i64) =
		stmt.query_row([], |row| Ok((row.get(0)?, row.get(1)?))).expect("operator state census failed");
	(keys as u64, bytes as u64)
}

pub fn clear<K: Keyspace>(txn: &Transaction, operator: OperatorId) {
	txn.prepare_cached(&format!("DELETE FROM \"{}\"", K::table(operator)))
		.expect("operator state drop could not be prepared")
		.execute([])
		.expect("operator state drop failed");
}

pub fn occupied<K: Keyspace>(conn: &Connection, operator: OperatorId) -> bool {
	conn.prepare_cached(&format!("SELECT EXISTS (SELECT 1 FROM \"{}\")", K::table(operator)))
		.expect("operator state occupancy could not be prepared")
		.query_row([], |row| row.get(0))
		.expect("operator state occupancy check failed")
}

#[cfg(test)]
mod tests {
	use std::{collections::HashSet, ops::Bound, ptr::null_mut};

	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::{
			operator::{
				keyspace::{
					KEYSPACES,
					join::{JoinLeft, JoinLeftKey, JoinRight, JoinRightKey},
					ringbuffer::{RingbufferTtlArm, RingbufferTtlArmKey},
					timer::TimerWheel,
				},
				state::{GroupId, KeyspaceId},
				traits::Keyspace,
			},
			typed::{
				direction::{Asc, Desc},
				range::KeyRange,
			},
		},
	};
	use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};
	use rusqlite::{
		Connection,
		ffi::{SQLITE_STMTSTATUS_REPREPARE, sqlite3_next_stmt, sqlite3_stmt_status},
	};

	use super::{
		SqlKey, create_table, keys_after as conn_keys_after, last as conn_last, range as conn_range, range_in,
		table_of,
	};
	use crate::persistent::sqlite::{
		SqlitePersistent,
		fixture::{census, encode, get, keys_after, last, open, range, remove_one, scan, set_one, with_conn},
		registry::TableRegistry,
	};

	fn group(id: u128) -> GroupId {
		GroupId::hashed(Hash128(id))
	}

	fn left(group_id: u128, row: u64) -> JoinLeftKey {
		JoinLeftKey {
			group: Desc(group(group_id)),
			row: Asc(RowNumber(row)),
		}
	}

	fn right(group_id: u128, row: u64) -> JoinRightKey {
		JoinRightKey {
			group: Desc(group(group_id)),
			row: Asc(RowNumber(row)),
		}
	}

	#[test]
	fn every_keyspace_gets_a_table_and_no_two_share_a_name() {
		// the table name is derived from NAME, so two keyspaces that lowercase alike would silently share
		// one table and read each other's rows back as their own shape
		let conn = Connection::open_in_memory().unwrap();
		let mut seen = HashSet::new();
		for spec in KEYSPACES {
			let table = table_of(spec.name, OperatorId(1));
			assert!(seen.insert(table.clone()), "{} reuses the table name {table}", spec.name);
			conn.execute_batch(&create_table(spec, OperatorId(1))).unwrap();
		}
		// a table name the registry cannot parse back would stop the next open or hide that table's rows
		let mut every: Vec<KeyspaceId> = KEYSPACES.iter().map(|spec| spec.id).collect();
		every.sort_unstable();
		assert_eq!(TableRegistry::load(&conn).mask(OperatorId(1)).held(), every);
	}

	#[test]
	fn the_primary_key_carries_every_column_in_order_and_never_a_direction_keyword() {
		// direction lives in the stored bytes now, so every column must compare ASC; a DESC left in the
		// clause would invert an already inverted column and a bounded row value compare could not be
		// written against this table at all
		let ddl = create_table(KEYSPACES.iter().find(|spec| spec.name == "JOIN_LEFT").unwrap(), OperatorId(1));
		assert!(ddl.contains(r#"PRIMARY KEY ("group", "row")"#), "{ddl}");
		assert!(!ddl.contains("DESC"), "{ddl}");
		assert!(!ddl.contains(" ASC"), "{ddl}");
		assert!(ddl.contains(r#""group" BLOB NOT NULL"#), "{ddl}");
		assert!(ddl.contains(r#""row" BLOB NOT NULL"#), "{ddl}");
		assert!(ddl.contains("WITHOUT ROWID"), "{ddl}");
	}

	#[test]
	fn a_descending_column_is_stored_as_the_complement_of_its_ascending_bytes() {
		// this is the whole mechanism: memcmp on the stored bytes must be the key's Ord, so a Desc column
		// stores ~bytes and a read that forgot to complement back would hand out a different group id
		let store = open();
		let root = JoinLeftKey {
			group: Desc(GroupId::ROOT),
			row: Asc(RowNumber(0)),
		};
		set_one::<JoinLeft>(&store, OperatorId(1), &root, b"x");
		let (group, row): (Vec<u8>, Vec<u8>) = with_conn(&store, |conn| {
			conn.query_row(r#"SELECT "group", "row" FROM "operator_join_left_1""#, [], |r| {
				Ok((r.get(0).unwrap(), r.get(1).unwrap()))
			})
			.unwrap()
		});
		assert_eq!(group, vec![0xFF; GroupId::WIDTH], "Desc<GroupId> of the minimum must store as all ones");
		assert_eq!(row, vec![0x00; 8], "Asc<RowNumber> of zero must store unchanged");
		assert_eq!(scan::<JoinLeft>(&store, OperatorId(1))[0].0, root);
	}

	#[test]
	#[should_panic(expected = "operator state read failed")]
	fn a_state_row_that_cannot_be_read_stops_instead_of_reading_as_a_missing_key() {
		// a failed read and an absent key are not the same answer: None tells the operator it has no
		// state for this key, so it rebuilds from nothing while the real row is still sitting there.
		let store = open();
		let key = left(7, 1);
		set_one::<JoinLeft>(&store, OperatorId(1), &key, b"payload");
		with_conn(&store, |conn| {
			conn.execute(r#"UPDATE "operator_join_left_1" SET "bytes" = 12345"#, []).unwrap()
		});

		get::<JoinLeft>(&store, OperatorId(1), &key);
	}

	#[test]
	fn a_typed_key_survives_a_write_and_a_read_back() {
		let store = open();
		let key = left(u128::MAX - 7, 41);
		set_one::<JoinLeft>(&store, OperatorId(3), &key, b"payload");
		assert_eq!(get::<JoinLeft>(&store, OperatorId(3), &key).as_deref(), Some(b"payload".as_slice()));
	}

	#[test]
	fn a_scan_returns_the_rows_in_the_order_the_key_type_orders_them() {
		// this is what the directions are for: the tier merges a sqlite page with its in memory runs by
		// assuming both are in Ord order, so a table that sorts differently corrupts the merge silently
		let store = open();
		let mut keys = vec![left(5, 2), left(1, 9), left(5, 1), left(9, 3), left(1, 0)];
		for key in &keys {
			set_one::<JoinLeft>(&store, OperatorId(1), key, b"x");
		}
		let scanned: Vec<JoinLeftKey> =
			scan::<JoinLeft>(&store, OperatorId(1)).into_iter().map(|(k, _)| k).collect();
		keys.sort();
		assert_eq!(scanned, keys);
	}

	#[test]
	fn one_operator_never_reads_another_operators_rows() {
		// the operator lives only in the table name; without it one flow would read another flow's state
		let store = open();
		let key = left(4, 4);
		set_one::<JoinLeft>(&store, OperatorId(1), &key, b"mine");
		set_one::<JoinLeft>(&store, OperatorId(2), &key, b"yours");
		assert_eq!(get::<JoinLeft>(&store, OperatorId(1), &key).as_deref(), Some(b"mine".as_slice()));
		assert_eq!(scan::<JoinLeft>(&store, OperatorId(2)).len(), 1);
	}

	#[test]
	fn a_second_write_to_one_key_replaces_it_rather_than_duplicating_it() {
		// the primary key is the whole key, so a missing upsert would either fail the insert or leave two
		// rows the scan then serves as two distinct keys
		let store = open();
		let key = left(2, 2);
		set_one::<JoinLeft>(&store, OperatorId(1), &key, b"first");
		set_one::<JoinLeft>(&store, OperatorId(1), &key, b"second");
		assert_eq!(scan::<JoinLeft>(&store, OperatorId(1)).len(), 1);
		assert_eq!(get::<JoinLeft>(&store, OperatorId(1), &key).as_deref(), Some(b"second".as_slice()));
	}

	#[test]
	fn a_removed_key_is_gone_and_its_neighbours_are_not() {
		let store = open();
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1), b"a");
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 2), b"b");
		remove_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1));
		assert_eq!(get::<JoinLeft>(&store, OperatorId(1), &left(1, 1)), None);
		assert_eq!(get::<JoinLeft>(&store, OperatorId(1), &left(1, 2)).as_deref(), Some(b"b".as_slice()));
	}

	fn seeded() -> SqlitePersistent {
		let store = open();
		for group in [1u128, 2] {
			for row in 0u64..4 {
				set_one::<JoinLeft>(
					&store,
					OperatorId(1),
					&left(group, row),
					&[group as u8, row as u8],
				);
			}
		}
		set_one::<JoinLeft>(&store, OperatorId(2), &left(1, 0), b"other");
		store
	}

	fn served(rows: Vec<(JoinLeftKey, Vec<u8>)>) -> Vec<(GroupId, u64)> {
		rows.into_iter().map(|(key, _)| (key.group.0, key.row.0.0)).collect()
	}

	fn expected<const N: usize>(pairs: [(u128, u64); N]) -> Vec<(GroupId, u64)> {
		pairs.into_iter().map(|(id, row)| (group(id), row)).collect()
	}

	#[test]
	fn a_bounded_range_serves_the_keys_between_its_bounds_in_key_order() {
		// the tier merges this page with its in memory runs assuming both are in Ord order, and Desc<GroupId>
		// means group 2 comes first; a range that walked the raw integer order would corrupt the merge
		let store = seeded();
		let all = served(range::<JoinLeft>(
			&store,
			OperatorId(1),
			&KeyRange::new(Bound::Unbounded, Bound::Unbounded),
			100,
		));
		assert_eq!(all, expected([(2, 0), (2, 1), (2, 2), (2, 3), (1, 0), (1, 1), (1, 2), (1, 3)]));
	}

	#[test]
	fn an_included_start_serves_its_own_key_and_an_excluded_one_skips_it() {
		// the two bounds differ by exactly one key, and getting them backwards either drops a live row or
		// re-serves the cursor the caller already consumed, which loops a paged scan forever
		let store = seeded();
		let included = served(range::<JoinLeft>(
			&store,
			OperatorId(1),
			&KeyRange::new(Bound::Included(left(2, 2)), Bound::Unbounded),
			100,
		));
		assert_eq!(included, expected([(2, 2), (2, 3), (1, 0), (1, 1), (1, 2), (1, 3)]));
		let excluded = served(range::<JoinLeft>(
			&store,
			OperatorId(1),
			&KeyRange::new(Bound::Excluded(left(2, 2)), Bound::Unbounded),
			100,
		));
		assert_eq!(excluded, expected([(2, 3), (1, 0), (1, 1), (1, 2), (1, 3)]));
	}

	#[test]
	fn an_end_bound_stops_the_range_and_excluded_drops_the_last_key() {
		let store = seeded();
		let included = served(range::<JoinLeft>(
			&store,
			OperatorId(1),
			&KeyRange::new(Bound::Unbounded, Bound::Included(left(2, 2))),
			100,
		));
		assert_eq!(included, expected([(2, 0), (2, 1), (2, 2)]));
		let excluded = served(range::<JoinLeft>(
			&store,
			OperatorId(1),
			&KeyRange::new(Bound::Unbounded, Bound::Excluded(left(2, 2))),
			100,
		));
		assert_eq!(excluded, expected([(2, 0), (2, 1)]));
	}

	#[test]
	fn a_range_bounded_on_both_sides_compares_the_whole_key_not_its_leading_column() {
		// a row value compare is the point of the direction encoding: bounds that land inside one group
		// must still cut on the trailing column, which a per column AND chain gets wrong
		let store = seeded();
		let inside = served(range::<JoinLeft>(
			&store,
			OperatorId(1),
			&KeyRange::new(Bound::Included(left(2, 2)), Bound::Excluded(left(1, 1))),
			100,
		));
		assert_eq!(inside, expected([(2, 2), (2, 3), (1, 0)]));
	}

	#[test]
	fn a_range_stops_at_its_limit_and_never_crosses_into_another_operator() {
		// without the limit a scan loads a whole table; a name without the operator reads another flow's state
		let store = seeded();
		let paged = served(range::<JoinLeft>(
			&store,
			OperatorId(1),
			&KeyRange::new(Bound::Unbounded, Bound::Unbounded),
			3,
		));
		assert_eq!(paged, expected([(2, 0), (2, 1), (2, 2)]));
		let other = served(range::<JoinLeft>(
			&store,
			OperatorId(2),
			&KeyRange::new(Bound::Unbounded, Bound::Unbounded),
			100,
		));
		assert_eq!(other, expected([(1, 0)]));
	}

	#[test]
	fn last_serves_the_tail_of_the_range_in_reverse_key_order() {
		// last_batch exists to answer the largest key without walking the table, so it must order by every
		// column descending; a DESC applied to the trailing column alone silently returns the wrong row
		let store = seeded();
		let tail = served(last::<JoinLeft>(
			&store,
			OperatorId(1),
			&KeyRange::new(Bound::Unbounded, Bound::Unbounded),
			3,
		));
		assert_eq!(tail, expected([(1, 3), (1, 2), (1, 1)]));
		let bounded = served(last::<JoinLeft>(
			&store,
			OperatorId(1),
			&KeyRange::new(Bound::Unbounded, Bound::Excluded(left(1, 1))),
			2,
		));
		assert_eq!(bounded, expected([(1, 0), (2, 3)]));
	}

	#[test]
	fn the_census_counts_each_operators_rows_and_payload_bytes() {
		// the census drives the memory budget; a table counted under the wrong operator bills the wrong flow
		let store = open();
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1), b"aaa");
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 2), b"bb");
		set_one::<JoinLeft>(&store, OperatorId(2), &left(1, 1), b"c");
		let mut counted = census::<JoinLeft>(&store);
		counted.sort_by_key(|(operator, _, _)| operator.0);
		assert_eq!(counted, vec![(OperatorId(1), 2, 5), (OperatorId(2), 1, 1)]);
	}

	#[test]
	fn a_removed_key_leaves_the_census() {
		// the census is what the budget spends against, so a delete that left its row counted would keep
		// charging for state that is gone and never let the budget recover
		let store = open();
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1), b"aaa");
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 2), b"bb");
		remove_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1));
		assert_eq!(census::<JoinLeft>(&store), vec![(OperatorId(1), 1, 2)]);
	}

	#[test]
	fn overwriting_a_key_moves_its_bytes_without_counting_it_twice() {
		// an upsert must replace, not append; a census that counted the key twice would report a table
		// twice its real size and the budget would evict state that was never there
		let store = open();
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1), b"aaaaa");
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1), b"b");
		assert_eq!(census::<JoinLeft>(&store), vec![(OperatorId(1), 1, 1)]);
	}

	#[test]
	fn an_empty_keyspace_reports_no_rows_rather_than_a_zero_row() {
		// callers sum the census to size a keyspace; a phantom zero row would name an operator that owns
		// nothing here and make an empty table look like a live one
		let store = open();
		assert_eq!(census::<JoinLeft>(&store), vec![]);
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1), b"a");
		remove_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1));
		assert_eq!(census::<JoinLeft>(&store), vec![]);
	}

	#[test]
	fn one_keyspaces_census_never_sees_another_keyspaces_rows() {
		// two keyspaces of one operator share the group and the key shape and differ only by table; a
		// census that read across them would charge each keyspace for the other's bytes
		let store = open();
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1), b"aaaa");
		set_one::<JoinRight>(&store, OperatorId(1), &right(1, 1), b"bb");
		assert_eq!(census::<JoinLeft>(&store), vec![(OperatorId(1), 1, 4)]);
		assert_eq!(census::<JoinRight>(&store), vec![(OperatorId(1), 1, 2)]);
	}

	#[test]
	fn a_group_id_binds_as_twenty_four_bytes_so_its_order_is_its_unsigned_order() {
		// R14: a u128 group split across a signed integer would order the top half before the bottom, and
		// the split is invisible until a group id happens to cross it
		let store = open();
		let low = left(0, 0);
		let high = left(u128::MAX, 0);
		set_one::<JoinLeft>(&store, OperatorId(1), &low, b"low");
		set_one::<JoinLeft>(&store, OperatorId(1), &high, b"high");
		let payloads: Vec<Vec<u8>> =
			scan::<JoinLeft>(&store, OperatorId(1)).into_iter().map(|(_, v)| v).collect();
		assert_eq!(
			payloads,
			vec![b"high".to_vec(), b"low".to_vec()],
			"Desc<GroupId> must serve the largest first"
		);
		let width: i64 = with_conn(&store, |conn| {
			conn.query_row(r#"SELECT LENGTH("group") FROM "operator_join_left_1" LIMIT 1"#, [], |row| {
				row.get(0)
			})
			.unwrap()
		});
		assert_eq!(width, GroupId::WIDTH as i64);
	}

	#[test]
	fn a_key_column_above_the_signed_range_still_sorts_after_every_smaller_one() {
		// sqlite has no unsigned integer, so a u64 past the signed maximum stored as INTEGER would go
		// negative and sort before every real key; eight big endian bytes make memcmp the u64 order
		let store = open();
		let rows = [0u64, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
		for row in rows {
			set_one::<JoinLeft>(&store, OperatorId(1), &left(1, row), &row.to_be_bytes());
		}
		let served: Vec<u64> =
			scan::<JoinLeft>(&store, OperatorId(1)).into_iter().map(|(key, _)| key.row.0.0).collect();
		assert_eq!(served, rows, "Asc<RowNumber> must serve the whole unsigned range in order");
	}

	#[test]
	fn a_u64_key_column_is_eight_bytes_wide() {
		// the column width is what makes memcmp agree with the integer order; a short or variable
		// encoding would order 0x0100 before 0xff and nothing downstream would notice
		let store = open();
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 7), b"x");
		let width: i64 = with_conn(&store, |conn| {
			conn.query_row(r#"SELECT LENGTH("row") FROM "operator_join_left_1" LIMIT 1"#, [], |row| {
				row.get(0)
			})
			.unwrap()
		});
		assert_eq!(width, 8);
	}

	#[test]
	fn a_keyspace_with_no_suffix_columns_still_addresses_one_row_per_group() {
		// several keyspaces carry only the group, so the generated statement has to stay valid with an
		// empty column list rather than emitting a dangling comma
		let ddl = create_table(
			KEYSPACES.iter().find(|spec| spec.name == TimerWheel::NAME).unwrap(),
			OperatorId(1),
		);
		let conn = Connection::open_in_memory().unwrap();
		conn.execute_batch(&ddl).unwrap();
		assert!(ddl.contains("PRIMARY KEY"), "{ddl}");
		assert!(conn
			.prepare(&format!(r#"SELECT * FROM "{}""#, <TimerWheel as SqlKey>::table(OperatorId(1))))
			.is_ok());
	}

	#[test]
	fn a_keyspace_with_no_key_columns_holds_one_row_per_operator() {
		// a keyspace with no key column still needs a primary key, or its first write stops the flush
		let store = open();
		let key = RingbufferTtlArmKey {};
		set_one::<RingbufferTtlArm>(&store, OperatorId(1), &key, b"first");
		set_one::<RingbufferTtlArm>(&store, OperatorId(1), &key, b"second");
		set_one::<RingbufferTtlArm>(&store, OperatorId(2), &key, b"other");
		assert_eq!(scan::<RingbufferTtlArm>(&store, OperatorId(1)), vec![(key, b"second".to_vec())]);
		assert_eq!(store.get_many(OperatorId(1), &[encode::<RingbufferTtlArm>(&key).into_encoded()]).len(), 1);
		remove_one::<RingbufferTtlArm>(&store, OperatorId(1), &key);
		assert!(scan::<RingbufferTtlArm>(&store, OperatorId(1)).is_empty());
		assert_eq!(get::<RingbufferTtlArm>(&store, OperatorId(2), &key).as_deref(), Some(b"other".as_slice()));
	}

	fn reprepares(conn: &Connection) -> i32 {
		let mut total = 0;
		// SAFETY: conn is borrowed for the whole walk, so its handle and every statement it owns stay alive
		unsafe {
			let db = conn.handle();
			let mut stmt = sqlite3_next_stmt(db, null_mut());
			while !stmt.is_null() {
				total += sqlite3_stmt_status(stmt, SQLITE_STMTSTATUS_REPREPARE, 0);
				stmt = sqlite3_next_stmt(db, stmt);
			}
		}
		total
	}

	#[test]
	fn a_range_read_runs_its_cached_statement_without_preparing_it_again() {
		// a bound limit expires the statement on every bind, so every read would parse and plan it again
		let store = seeded();
		with_conn(&store, |conn| {
			for row in 0u64..3 {
				let bounds =
					KeyRange::new(Bound::Included(left(2, row)), Bound::Excluded(left(1, row)));
				assert!(!conn_range::<JoinLeft>(conn, OperatorId(1), &bounds, 2).is_empty());
				assert!(!conn_last::<JoinLeft>(conn, OperatorId(1), &bounds, 2).is_empty());
				let suffix = KeyRange::new(Bound::Included(left(1, row)), Bound::Included(left(1, 3)));
				assert!(!range_in::<JoinLeft>(
					conn,
					OperatorId(1),
					&[group(1), group(2)],
					&suffix,
					2,
					"ASC"
				)
				.is_empty());
				assert!(!conn_keys_after::<JoinLeft>(conn, OperatorId(1), Some(&left(2, row)), 2)
					.is_empty());
			}
			assert_eq!(
				reprepares(conn),
				0,
				"every range shape must reuse its prepared statement on the next call"
			);
		});
	}

	#[test]
	fn a_flush_into_existing_tables_leaves_every_cached_read_prepared() {
		// a steady-state flush that still changed the schema would make every connection re-prepare its reads
		let store = open();
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 1), b"first");
		let reads = |store: &SqlitePersistent| {
			assert!(get::<JoinLeft>(store, OperatorId(1), &left(1, 1)).is_some());
			assert!(!scan::<JoinLeft>(store, OperatorId(1)).is_empty());
			assert!(!keys_after::<JoinLeft>(store, OperatorId(1), None, 2).is_empty());
		};
		reads(&store);
		set_one::<JoinLeft>(&store, OperatorId(1), &left(1, 2), b"second");
		reads(&store);
		assert_eq!(with_conn(&store, reprepares), 0);
	}
}
