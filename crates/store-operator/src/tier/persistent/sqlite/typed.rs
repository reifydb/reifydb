// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{keyspace::KeyspaceSpec, traits::Keyspace},
		typed::{
			direction::Direction,
			layout::{KeyColumn, KeyColumnType, KeyLayout, KeyValue},
			range::KeyRange,
		},
	},
};
use reifydb_sqlite::batch::values_placeholders;
use rusqlite::{Connection, Row, Transaction, params_from_iter, types::Value};

pub fn table_of(name: &str) -> String {
	format!("operator_{}", name.to_ascii_lowercase())
}

pub fn create_table(spec: &KeyspaceSpec) -> String {
	let mut sql =
		format!("CREATE TABLE IF NOT EXISTS \"{}\" (\n\t\"operator\" INTEGER NOT NULL,\n", table_of(spec.name));
	for column in spec.columns {
		sql.push_str(&format!("\t\"{}\" {} NOT NULL,\n", column.name, sql_type(column.ty)));
	}
	sql.push_str("\t\"bytes\" BLOB NOT NULL,\n\tPRIMARY KEY (\"operator\"");
	for column in spec.columns {
		sql.push_str(&format!(", \"{}\"", column.name));
	}
	sql.push_str(")\n) WITHOUT ROWID;");
	sql
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
	fn table() -> String;

	fn bind_key(key: &Self::GroupedKey) -> Vec<Value>;

	fn read_key(row: &Row<'_>, at: usize) -> Option<Self::GroupedKey>;

	fn columns() -> &'static [KeyColumn];

	fn column_list() -> String;

	fn placeholders(from: usize) -> String;

	fn key_predicate(from: usize) -> String;

	fn key_columns() -> String;

	fn key_values(from: usize) -> String;

	fn conflict_target() -> String;

	fn ordering(order: &str) -> String;
}

impl<K: Keyspace> SqlKey for K {
	fn table() -> String {
		table_of(K::NAME)
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

	fn conflict_target() -> String {
		match Self::columns().is_empty() {
			true => String::new(),
			false => format!(", {}", Self::column_list()),
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
}

pub fn set<K: Keyspace>(conn: &Connection, operator: OperatorId, key: &K::GroupedKey, bytes: &[u8]) {
	let sql = format!(
		"INSERT INTO \"{}\" (\"operator\", {}\"bytes\") VALUES (?1, {}?{})\n\
		 ON CONFLICT (\"operator\"{}) DO UPDATE SET \"bytes\" = excluded.\"bytes\"",
		K::table(),
		K::key_columns(),
		K::key_values(2),
		K::columns().len() + 2,
		K::conflict_target()
	);
	let mut params = vec![Value::Integer(operator.0 as i64)];
	params.extend(K::bind_key(key));
	params.push(Value::Blob(bytes.to_vec()));
	conn.execute(&sql, params_from_iter(params)).expect("operator state row could not be written");
}

pub const WRITE_CHUNK: usize = 100;

fn set_sql<K: Keyspace>(rows: usize) -> String {
	let cols = K::columns().len() + 2;
	format!(
		"INSERT INTO \"{}\" (\"operator\", {}\"bytes\") VALUES {}\n\
		 ON CONFLICT (\"operator\"{}) DO UPDATE SET \"bytes\" = excluded.\"bytes\"",
		K::table(),
		K::key_columns(),
		values_placeholders(rows, cols),
		K::conflict_target()
	)
}

fn remove_sql<K: Keyspace>(rows: usize) -> String {
	format!(
		"DELETE FROM \"{}\" WHERE (\"operator\"{}) IN (VALUES {})",
		K::table(),
		K::conflict_target(),
		values_placeholders(rows, K::columns().len() + 1)
	)
}

pub fn set_chunked<K: Keyspace>(txn: &Transaction, rows: &[(OperatorId, K::GroupedKey, Vec<u8>)]) {
	if rows.is_empty() {
		return;
	}
	let bind = |row: &(OperatorId, K::GroupedKey, Vec<u8>), params: &mut Vec<Value>| {
		params.push(Value::Integer(row.0.0 as i64));
		params.extend(K::bind_key(&row.1));
		params.push(Value::Blob(row.2.clone()));
	};
	let mut chunks = rows.chunks_exact(WRITE_CHUNK);
	let chunk_sql = set_sql::<K>(WRITE_CHUNK);
	for full in chunks.by_ref() {
		let mut params = Vec::with_capacity(WRITE_CHUNK * (K::columns().len() + 2));
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
	let rest_sql = set_sql::<K>(rest.len());
	let mut params = Vec::with_capacity(rest.len() * (K::columns().len() + 2));
	for row in rest {
		bind(row, &mut params);
	}
	txn.prepare_cached(&rest_sql)
		.expect("operator state write could not be prepared")
		.execute(params_from_iter(params))
		.expect("operator state write failed");
}

pub fn remove_chunked<K: Keyspace>(txn: &Transaction, keys: &[(OperatorId, K::GroupedKey)]) {
	if keys.is_empty() {
		return;
	}
	let bind = |row: &(OperatorId, K::GroupedKey), params: &mut Vec<Value>| {
		params.push(Value::Integer(row.0.0 as i64));
		params.extend(K::bind_key(&row.1));
	};
	let mut chunks = keys.chunks_exact(WRITE_CHUNK);
	let chunk_sql = remove_sql::<K>(WRITE_CHUNK);
	for full in chunks.by_ref() {
		let mut params = Vec::with_capacity(WRITE_CHUNK * (K::columns().len() + 1));
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
	let rest_sql = remove_sql::<K>(rest.len());
	let mut params = Vec::with_capacity(rest.len() * (K::columns().len() + 1));
	for row in rest {
		bind(row, &mut params);
	}
	txn.prepare_cached(&rest_sql)
		.expect("operator state delete could not be prepared")
		.execute(params_from_iter(params))
		.expect("operator state delete failed");
}

pub fn drop_operator_in<K: Keyspace>(txn: &Transaction, operator: OperatorId) {
	let sql = format!("DELETE FROM \"{}\" WHERE \"operator\" = ?1", K::table());
	txn.execute(&sql, [operator.0 as i64]).expect("operator state rows could not be dropped");
}

pub fn get<K: Keyspace>(conn: &Connection, operator: OperatorId, key: &K::GroupedKey) -> Option<Vec<u8>> {
	let sql = format!("SELECT \"bytes\" FROM \"{}\" WHERE \"operator\" = ?1{}", K::table(), K::key_predicate(2));
	let mut params = vec![Value::Integer(operator.0 as i64)];
	params.extend(K::bind_key(key));
	conn.query_row(&sql, params_from_iter(params), |row| row.get::<_, Vec<u8>>(0)).ok()
}

pub fn remove<K: Keyspace>(conn: &Connection, operator: OperatorId, key: &K::GroupedKey) {
	let sql = format!("DELETE FROM \"{}\" WHERE \"operator\" = ?1{}", K::table(), K::key_predicate(2));
	let mut params = vec![Value::Integer(operator.0 as i64)];
	params.extend(K::bind_key(key));
	conn.execute(&sql, params_from_iter(params)).expect("operator state row could not be removed");
}

pub fn drop_operator<K: Keyspace>(conn: &Connection, operator: OperatorId) {
	let sql = format!("DELETE FROM \"{}\" WHERE \"operator\" = ?1", K::table());
	conn.execute(&sql, [operator.0 as i64]).expect("operator state rows could not be dropped");
}

pub fn scan<K: Keyspace>(conn: &Connection, operator: OperatorId) -> Vec<(K::GroupedKey, Vec<u8>)> {
	let sql = format!(
		"SELECT {}\"bytes\" FROM \"{}\" WHERE \"operator\" = ?1{}",
		K::key_columns(),
		K::table(),
		K::ordering("ASC")
	);
	let mut stmt = conn.prepare_cached(&sql).expect("operator state scan could not be prepared");
	let mut rows = stmt.query([operator.0 as i64]).expect("operator state scan failed");
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

fn bounded<K: Keyspace>(
	conn: &Connection,
	operator: OperatorId,
	range: &KeyRange<K::GroupedKey>,
	limit: u64,
	order: &str,
) -> Vec<(K::GroupedKey, Vec<u8>)> {
	let width = K::columns().len();
	let mut at = 2;
	let start = bound_clause::<K>(range.start.as_ref(), ">=", ">", at);
	if bound_key::<K>(range.start.as_ref()).is_some() {
		at += width;
	}
	let end = bound_clause::<K>(range.end.as_ref(), "<=", "<", at);
	if bound_key::<K>(range.end.as_ref()).is_some() {
		at += width;
	}
	let sql = format!(
		"SELECT {}\"bytes\" FROM \"{}\" WHERE \"operator\" = ?1{}{}{} LIMIT ?{}",
		K::key_columns(),
		K::table(),
		start,
		end,
		K::ordering(order),
		at
	);
	let mut params = vec![Value::Integer(operator.0 as i64)];
	if !K::columns().is_empty() {
		for bound in [range.start.as_ref(), range.end.as_ref()] {
			if let Some(key) = bound_key::<K>(bound) {
				params.extend(K::bind_key(key));
			}
		}
	}
	params.push(Value::Integer(limit as i64));
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

pub fn census<K: Keyspace>(conn: &Connection) -> Vec<(OperatorId, u64, u64)> {
	let sql = format!(
		"SELECT \"operator\", COUNT(*), COALESCE(SUM(LENGTH(\"bytes\")), 0) FROM \"{}\" GROUP BY \"operator\"",
		K::table()
	);
	let mut stmt = conn.prepare_cached(&sql).expect("operator state census could not be prepared");
	let mut rows = stmt.query([]).expect("operator state census failed");
	let mut out = Vec::new();
	while let Some(row) = rows.next().expect("operator state census row failed") {
		let operator: i64 = row.get(0).expect("census row has no operator");
		let keys: i64 = row.get(1).expect("census row has no key count");
		let bytes: i64 = row.get(2).expect("census row has no byte count");
		out.push((OperatorId(operator as u64), keys as u64, bytes as u64));
	}
	out
}

#[cfg(test)]
mod tests {
	use std::{collections::HashSet, ops::Bound};

	use reifydb_core::{
		interface::catalog::flow::OperatorId,
		key::{
			operator::{
				keyspace::{
					KEYSPACES,
					join::{JoinLeft, JoinLeftKey, JoinRight, JoinRightKey},
					timer::TimerWheel,
				},
				state::GroupId,
				traits::Keyspace,
			},
			typed::{
				direction::{Asc, Desc},
				range::KeyRange,
			},
		},
	};
	use reifydb_value::{util::hash::Hash128, value::row_number::RowNumber};
	use rusqlite::Connection;

	use super::{SqlKey, census, create_table, drop_operator, get, last, range, remove, scan, set, table_of};
	use crate::tier::persistent::sqlite::schema::ensure_schema;

	fn db() -> Connection {
		let conn = Connection::open_in_memory().unwrap();
		ensure_schema(&conn);
		conn
	}

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
		let conn = db();
		let mut seen = HashSet::new();
		for spec in KEYSPACES {
			let table = table_of(spec.name);
			assert!(seen.insert(table.clone()), "{} reuses the table name {table}", spec.name);
			let found: i64 = conn
				.query_row(
					r#"SELECT COUNT(*) FROM "sqlite_master" WHERE "type" = 'table' AND "name" = ?1"#,
					[&table],
					|row| row.get(0),
				)
				.unwrap();
			assert_eq!(found, 1, "{} has no table", spec.name);
		}
		for reserved in ["operator_state", "operator_join_expiry", "operator_state_census", "flow_checkpoint"] {
			assert!(!seen.contains(reserved), "a keyspace table collides with {reserved}");
		}
	}

	#[test]
	fn the_primary_key_carries_every_column_in_order_and_never_a_direction_keyword() {
		// direction lives in the stored bytes now, so every column must compare ASC; a DESC left in the
		// clause would invert an already inverted column and a bounded row value compare could not be
		// written against this table at all
		let ddl = create_table(&KEYSPACES.iter().find(|spec| spec.name == "JOIN_LEFT").unwrap());
		assert!(ddl.contains(r#"PRIMARY KEY ("operator", "group", "row")"#), "{ddl}");
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
		let conn = db();
		let root = JoinLeftKey {
			group: Desc(GroupId::ROOT),
			row: Asc(RowNumber(0)),
		};
		set::<JoinLeft>(&conn, OperatorId(1), &root, b"x");
		let (group, row): (Vec<u8>, Vec<u8>) = conn
			.query_row(r#"SELECT "group", "row" FROM "operator_join_left""#, [], |r| {
				Ok((r.get(0).unwrap(), r.get(1).unwrap()))
			})
			.unwrap();
		assert_eq!(group, vec![0xFF; GroupId::WIDTH], "Desc<GroupId> of the minimum must store as all ones");
		assert_eq!(row, vec![0x00; 8], "Asc<RowNumber> of zero must store unchanged");
		assert_eq!(scan::<JoinLeft>(&conn, OperatorId(1))[0].0, root);
	}

	#[test]
	fn a_typed_key_survives_a_write_and_a_read_back() {
		let conn = db();
		let key = left(u128::MAX - 7, 41);
		set::<JoinLeft>(&conn, OperatorId(3), &key, b"payload");
		assert_eq!(get::<JoinLeft>(&conn, OperatorId(3), &key).as_deref(), Some(b"payload".as_slice()));
	}

	#[test]
	fn a_scan_returns_the_rows_in_the_order_the_key_type_orders_them() {
		// this is what the directions are for: the tier merges a sqlite page with its in memory runs by
		// assuming both are in Ord order, so a table that sorts differently corrupts the merge silently
		let conn = db();
		let mut keys = vec![left(5, 2), left(1, 9), left(5, 1), left(9, 3), left(1, 0)];
		for key in &keys {
			set::<JoinLeft>(&conn, OperatorId(1), key, b"x");
		}
		let scanned: Vec<JoinLeftKey> =
			scan::<JoinLeft>(&conn, OperatorId(1)).into_iter().map(|(k, _)| k).collect();
		keys.sort();
		assert_eq!(scanned, keys);
	}

	#[test]
	fn one_operator_never_reads_another_operators_rows() {
		// operator is the leading key column and the whole reason the table is shared; a predicate that
		// dropped it would hand one flow another flow's state
		let conn = db();
		let key = left(4, 4);
		set::<JoinLeft>(&conn, OperatorId(1), &key, b"mine");
		set::<JoinLeft>(&conn, OperatorId(2), &key, b"yours");
		assert_eq!(get::<JoinLeft>(&conn, OperatorId(1), &key).as_deref(), Some(b"mine".as_slice()));
		assert_eq!(scan::<JoinLeft>(&conn, OperatorId(2)).len(), 1);
		drop_operator::<JoinLeft>(&conn, OperatorId(1));
		assert_eq!(get::<JoinLeft>(&conn, OperatorId(1), &key), None);
		assert_eq!(get::<JoinLeft>(&conn, OperatorId(2), &key).as_deref(), Some(b"yours".as_slice()));
	}

	#[test]
	fn a_second_write_to_one_key_replaces_it_rather_than_duplicating_it() {
		// the primary key is the whole key, so a missing upsert would either fail the insert or leave two
		// rows the scan then serves as two distinct keys
		let conn = db();
		let key = left(2, 2);
		set::<JoinLeft>(&conn, OperatorId(1), &key, b"first");
		set::<JoinLeft>(&conn, OperatorId(1), &key, b"second");
		assert_eq!(scan::<JoinLeft>(&conn, OperatorId(1)).len(), 1);
		assert_eq!(get::<JoinLeft>(&conn, OperatorId(1), &key).as_deref(), Some(b"second".as_slice()));
	}

	#[test]
	fn a_removed_key_is_gone_and_its_neighbours_are_not() {
		let conn = db();
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 1), b"a");
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 2), b"b");
		remove::<JoinLeft>(&conn, OperatorId(1), &left(1, 1));
		assert_eq!(get::<JoinLeft>(&conn, OperatorId(1), &left(1, 1)), None);
		assert_eq!(get::<JoinLeft>(&conn, OperatorId(1), &left(1, 2)).as_deref(), Some(b"b".as_slice()));
	}

	fn seeded() -> Connection {
		let conn = db();
		for group in [1u128, 2] {
			for row in 0u64..4 {
				set::<JoinLeft>(&conn, OperatorId(1), &left(group, row), &[group as u8, row as u8]);
			}
		}
		set::<JoinLeft>(&conn, OperatorId(2), &left(1, 0), b"other");
		conn
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
		let conn = seeded();
		let all = served(range::<JoinLeft>(
			&conn,
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
		let conn = seeded();
		let included = served(range::<JoinLeft>(
			&conn,
			OperatorId(1),
			&KeyRange::new(Bound::Included(left(2, 2)), Bound::Unbounded),
			100,
		));
		assert_eq!(included, expected([(2, 2), (2, 3), (1, 0), (1, 1), (1, 2), (1, 3)]));
		let excluded = served(range::<JoinLeft>(
			&conn,
			OperatorId(1),
			&KeyRange::new(Bound::Excluded(left(2, 2)), Bound::Unbounded),
			100,
		));
		assert_eq!(excluded, expected([(2, 3), (1, 0), (1, 1), (1, 2), (1, 3)]));
	}

	#[test]
	fn an_end_bound_stops_the_range_and_excluded_drops_the_last_key() {
		let conn = seeded();
		let included = served(range::<JoinLeft>(
			&conn,
			OperatorId(1),
			&KeyRange::new(Bound::Unbounded, Bound::Included(left(2, 2))),
			100,
		));
		assert_eq!(included, expected([(2, 0), (2, 1), (2, 2)]));
		let excluded = served(range::<JoinLeft>(
			&conn,
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
		let conn = seeded();
		let inside = served(range::<JoinLeft>(
			&conn,
			OperatorId(1),
			&KeyRange::new(Bound::Included(left(2, 2)), Bound::Excluded(left(1, 1))),
			100,
		));
		assert_eq!(inside, expected([(2, 2), (2, 3), (1, 0)]));
	}

	#[test]
	fn a_range_stops_at_its_limit_and_never_crosses_into_another_operator() {
		// the limit is what makes a scan pageable, and operator is the leading column; a range that lost
		// either would either read a whole table into memory or hand one flow another flow's state
		let conn = seeded();
		let paged = served(range::<JoinLeft>(
			&conn,
			OperatorId(1),
			&KeyRange::new(Bound::Unbounded, Bound::Unbounded),
			3,
		));
		assert_eq!(paged, expected([(2, 0), (2, 1), (2, 2)]));
		let other = served(range::<JoinLeft>(
			&conn,
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
		let conn = seeded();
		let tail = served(last::<JoinLeft>(
			&conn,
			OperatorId(1),
			&KeyRange::new(Bound::Unbounded, Bound::Unbounded),
			3,
		));
		assert_eq!(tail, expected([(1, 3), (1, 2), (1, 1)]));
		let bounded = served(last::<JoinLeft>(
			&conn,
			OperatorId(1),
			&KeyRange::new(Bound::Unbounded, Bound::Excluded(left(1, 1))),
			2,
		));
		assert_eq!(bounded, expected([(1, 0), (2, 3)]));
	}

	#[test]
	fn the_census_counts_each_operators_rows_and_payload_bytes() {
		// the census drives the memory budget; counting a shared table without grouping by operator would
		// charge every flow for every other flow's state
		let conn = db();
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 1), b"aaa");
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 2), b"bb");
		set::<JoinLeft>(&conn, OperatorId(2), &left(1, 1), b"c");
		let mut counted = census::<JoinLeft>(&conn);
		counted.sort_by_key(|(operator, _, _)| operator.0);
		assert_eq!(counted, vec![(OperatorId(1), 2, 5), (OperatorId(2), 1, 1)]);
	}

	#[test]
	fn a_removed_key_leaves_the_census() {
		// the census is what the budget spends against, so a delete that left its row counted would keep
		// charging for state that is gone and never let the budget recover
		let conn = db();
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 1), b"aaa");
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 2), b"bb");
		remove::<JoinLeft>(&conn, OperatorId(1), &left(1, 1));
		assert_eq!(census::<JoinLeft>(&conn), vec![(OperatorId(1), 1, 2)]);
	}

	#[test]
	fn overwriting_a_key_moves_its_bytes_without_counting_it_twice() {
		// an upsert must replace, not append; a census that counted the key twice would report a table
		// twice its real size and the budget would evict state that was never there
		let conn = db();
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 1), b"aaaaa");
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 1), b"b");
		assert_eq!(census::<JoinLeft>(&conn), vec![(OperatorId(1), 1, 1)]);
	}

	#[test]
	fn dropping_an_operator_empties_its_census_entry_and_leaves_the_others() {
		// a dropped flow must stop being charged entirely; a lingering entry would hold budget forever
		// because nothing will ever write to that operator again to correct it
		let conn = db();
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 1), b"aa");
		set::<JoinLeft>(&conn, OperatorId(2), &left(1, 1), b"bbb");
		drop_operator::<JoinLeft>(&conn, OperatorId(1));
		assert_eq!(census::<JoinLeft>(&conn), vec![(OperatorId(2), 1, 3)]);
	}

	#[test]
	fn an_empty_keyspace_reports_no_rows_rather_than_a_zero_row() {
		// callers sum the census to size a keyspace; a phantom zero row would name an operator that owns
		// nothing here and make an empty table look like a live one
		let conn = db();
		assert_eq!(census::<JoinLeft>(&conn), vec![]);
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 1), b"a");
		remove::<JoinLeft>(&conn, OperatorId(1), &left(1, 1));
		assert_eq!(census::<JoinLeft>(&conn), vec![]);
	}

	#[test]
	fn one_keyspaces_census_never_sees_another_keyspaces_rows() {
		// two keyspaces of one operator share the group and the key shape and differ only by table; a
		// census that read across them would charge each keyspace for the other's bytes
		let conn = db();
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 1), b"aaaa");
		set::<JoinRight>(&conn, OperatorId(1), &right(1, 1), b"bb");
		assert_eq!(census::<JoinLeft>(&conn), vec![(OperatorId(1), 1, 4)]);
		assert_eq!(census::<JoinRight>(&conn), vec![(OperatorId(1), 1, 2)]);
	}

	#[test]
	fn a_group_id_binds_as_twenty_four_bytes_so_its_order_is_its_unsigned_order() {
		// R14: a u128 group split across a signed integer would order the top half before the bottom, and
		// the split is invisible until a group id happens to cross it
		let conn = db();
		let low = left(0, 0);
		let high = left(u128::MAX, 0);
		set::<JoinLeft>(&conn, OperatorId(1), &low, b"low");
		set::<JoinLeft>(&conn, OperatorId(1), &high, b"high");
		let payloads: Vec<Vec<u8>> =
			scan::<JoinLeft>(&conn, OperatorId(1)).into_iter().map(|(_, v)| v).collect();
		assert_eq!(
			payloads,
			vec![b"high".to_vec(), b"low".to_vec()],
			"Desc<GroupId> must serve the largest first"
		);
		let width: i64 = conn
			.query_row(r#"SELECT LENGTH("group") FROM "operator_join_left" LIMIT 1"#, [], |row| row.get(0))
			.unwrap();
		assert_eq!(width, GroupId::WIDTH as i64);
	}

	#[test]
	fn a_key_column_above_the_signed_range_still_sorts_after_every_smaller_one() {
		// sqlite has no unsigned integer, so a u64 past the signed maximum stored as INTEGER would go
		// negative and sort before every real key; eight big endian bytes make memcmp the u64 order
		let conn = db();
		let rows = [0u64, 1, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
		for row in rows {
			set::<JoinLeft>(&conn, OperatorId(1), &left(1, row), &row.to_be_bytes());
		}
		let served: Vec<u64> =
			scan::<JoinLeft>(&conn, OperatorId(1)).into_iter().map(|(key, _)| key.row.0.0).collect();
		assert_eq!(served, rows, "Asc<RowNumber> must serve the whole unsigned range in order");
	}

	#[test]
	fn a_u64_key_column_is_eight_bytes_wide() {
		// the column width is what makes memcmp agree with the integer order; a short or variable
		// encoding would order 0x0100 before 0xff and nothing downstream would notice
		let conn = db();
		set::<JoinLeft>(&conn, OperatorId(1), &left(1, 7), b"x");
		let width: i64 = conn
			.query_row(r#"SELECT LENGTH("row") FROM "operator_join_left" LIMIT 1"#, [], |row| row.get(0))
			.unwrap();
		assert_eq!(width, 8);
	}

	#[test]
	fn a_keyspace_with_no_suffix_columns_still_addresses_one_row_per_group() {
		// several keyspaces carry only the group, so the generated statement has to stay valid with an
		// empty column list rather than emitting a dangling comma
		let conn = db();
		let ddl = create_table(&KEYSPACES.iter().find(|spec| spec.name == TimerWheel::NAME).unwrap());
		assert!(ddl.contains("PRIMARY KEY"), "{ddl}");
		assert!(conn.prepare(&format!(r#"SELECT * FROM "{}""#, <TimerWheel as SqlKey>::table())).is_ok());
	}
}
