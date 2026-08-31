// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb_codec::key::encoded::EncodedKey;

pub(super) const STATE_GET_SQL: &str = r#"SELECT "bytes" FROM "operator_state" WHERE "operator" = ?1 AND "key" = ?2"#;

pub(super) const STATE_EXISTS_SQL: &str = r#"SELECT EXISTS(SELECT 1 FROM "operator_state")"#;

pub(super) const STATE_CONTAINS_SQL: &str =
	r#"SELECT 1 FROM "operator_state" WHERE "operator" = ?1 AND "key" = ?2 LIMIT 1"#;

pub(super) const STATE_SET_SQL: &str = r#"INSERT INTO "operator_state" ("operator", "key", "bytes") VALUES (?1, ?2, ?3)
	   ON CONFLICT ("operator", "key") DO UPDATE SET "bytes" = excluded."bytes""#;

pub(super) const STATE_REMOVE_SQL: &str = r#"DELETE FROM "operator_state" WHERE "operator" = ?1 AND "key" = ?2"#;

pub(super) const STATE_DROP_SQL: &str = r#"DELETE FROM "operator_state" WHERE "operator" = ?1"#;

#[cfg(reifydb_assertions)]
pub(super) const STATE_VALUE_LEN_SQL: &str =
	r#"SELECT LENGTH("bytes") FROM "operator_state" WHERE "operator" = ?1 AND "key" = ?2"#;

pub(super) const STATE_SIZE_CHUNK: usize = 512;

pub(super) const STATE_GET_CHUNK: usize = 512;

pub(super) fn state_gets_sql(count: usize) -> String {
	let mut sql =
		String::from(r#"SELECT "key", "bytes" FROM "operator_state" WHERE "operator" = ?1 AND "key" IN ("#);
	for index in 0..count {
		if index > 0 {
			sql.push(',');
		}
		sql.push('?');
		sql.push_str(&(index + 2).to_string());
	}
	sql.push(')');
	sql
}

pub(super) fn state_sizes_sql(count: usize) -> String {
	let mut sql = String::from(
		r#"SELECT "key", LENGTH("bytes") FROM "operator_state" WHERE "operator" = ?1 AND "key" IN ("#,
	);
	for index in 0..count {
		if index > 0 {
			sql.push(',');
		}
		sql.push('?');
		sql.push_str(&(index + 2).to_string());
	}
	sql.push(')');
	sql
}

pub(super) const STATE_BYTES_SQL: &str =
	r#"SELECT COALESCE(SUM(LENGTH("key") + LENGTH("bytes")), 0) FROM "operator_state" WHERE "operator" = ?1"#;

pub(super) const STATE_TOTAL_BYTES_SQL: &str =
	r#"SELECT COALESCE(SUM(LENGTH("key") + LENGTH("bytes")), 0) FROM "operator_state""#;

pub(super) const STATE_CENSUS_SQL: &str = r#"SELECT "operator", "keyspace", "keys", "key_bytes", "value_bytes"
	   FROM "operator_state_census"
	   WHERE "keys" > 0
	   ORDER BY "operator", "keyspace""#;

pub(super) const CENSUS_APPLY_SQL: &str = r#"INSERT INTO "operator_state_census"
		("operator", "keyspace", "keys", "key_bytes", "value_bytes")
	   VALUES (?1, ?2, ?3, ?4, ?5)
	   ON CONFLICT ("operator", "keyspace") DO UPDATE SET
		"keys" = "keys" + excluded."keys",
		"key_bytes" = "key_bytes" + excluded."key_bytes",
		"value_bytes" = "value_bytes" + excluded."value_bytes""#;

pub(super) const CENSUS_ZERO_OPERATOR_SQL: &str = r#"UPDATE "operator_state_census"
	   SET "keys" = 0, "key_bytes" = 0, "value_bytes" = 0
	   WHERE "operator" = ?1"#;

pub(crate) const JOIN_EXPIRIES_BY_TIME_SQL: &str = r#"SELECT "side", "row_number", "at" FROM "operator_join_expiry"
	   WHERE "operator" = ?1 AND "group" = ?2
	   ORDER BY "at" ASC LIMIT ?3"#;

pub(super) const JOIN_EXPIRIES_DUE_SQL: &str = r#"SELECT "side", "row_number", "at" FROM "operator_join_expiry"
	   WHERE "operator" = ?1 AND "group" = ?2 AND "at" <= ?3
	   ORDER BY "at" ASC LIMIT ?4"#;

pub(super) const JOIN_EXPIRY_GET_SQL: &str = r#"SELECT "at" FROM "operator_join_expiry"
	   WHERE "operator" = ?1 AND "group" = ?2 AND "side" = ?3 AND "row_number" = ?4"#;

pub(super) const JOIN_EXPIRY_SET_SQL: &str = r#"INSERT INTO "operator_join_expiry" ("operator", "group", "side", "row_number", "at")
	   VALUES (?1, ?2, ?3, ?4, ?5)
	   ON CONFLICT ("operator", "group", "side", "row_number")
	   DO UPDATE SET "at" = excluded."at""#;

pub(super) const JOIN_EXPIRY_REMOVE_SQL: &str = r#"DELETE FROM "operator_join_expiry"
	   WHERE "operator" = ?1 AND "group" = ?2 AND "side" = ?3 AND "row_number" = ?4"#;

pub(super) const JOIN_EXPIRIES_DROP_OPERATOR_SQL: &str = r#"DELETE FROM "operator_join_expiry" WHERE "operator" = ?1"#;

pub(super) const JOIN_EXPIRIES_DROP_GROUP_SQL: &str =
	r#"DELETE FROM "operator_join_expiry" WHERE "operator" = ?1 AND "group" = ?2"#;

pub(super) const JOIN_EXPIRY_EXISTS_SQL: &str = r#"SELECT EXISTS(SELECT 1 FROM "operator_join_expiry")"#;

pub(super) const JOIN_EXPIRY_COUNT_SQL: &str = r#"SELECT COUNT(*) FROM "operator_join_expiry" WHERE "operator" = ?1"#;

pub(super) const JOIN_EXPIRY_TOTAL_COUNT_SQL: &str = r#"SELECT COUNT(*) FROM "operator_join_expiry""#;

pub(super) const JOIN_EXPIRY_CENSUS_SQL: &str = r#"SELECT "operator", COUNT(*) FROM "operator_join_expiry"
	   GROUP BY "operator" ORDER BY "operator""#;

pub(super) const CHECKPOINT_GET_SQL: &str = r#"SELECT "version" FROM "flow_checkpoint" WHERE "flow" = ?1"#;

pub(super) const CHECKPOINT_SET_SQL: &str = r#"INSERT INTO "flow_checkpoint" ("flow", "version") VALUES (?1, ?2)
	   ON CONFLICT ("flow") DO UPDATE SET "version" = excluded."version""#;

pub(super) const CHECKPOINT_REMOVE_SQL: &str = r#"DELETE FROM "flow_checkpoint" WHERE "flow" = ?1"#;

pub(super) const CHECKPOINT_FLOOR_SQL: &str = r#"SELECT MIN("version") FROM "flow_checkpoint""#;

pub(super) const CHECKPOINT_LIST_SQL: &str = r#"SELECT "flow" FROM "flow_checkpoint" ORDER BY "flow" ASC"#;

macro_rules! range_sql_variant {
	($($clause:expr),*) => {
		concat!(
			r#"SELECT "key", "bytes" FROM "operator_state" WHERE "operator" = ?1"#,
			$($clause,)*
		)
	};
}

pub(super) fn last_sql(start: Bound<&EncodedKey>, end: Bound<&EncodedKey>) -> &'static str {
	match (start, end) {
		(Bound::Unbounded, Bound::Unbounded) => range_sql_variant!(r#" ORDER BY "key" DESC LIMIT ?2"#),
		(Bound::Unbounded, Bound::Included(_)) => {
			range_sql_variant!(r#" AND "key" <= ?2"#, r#" ORDER BY "key" DESC LIMIT ?3"#)
		}
		(Bound::Unbounded, Bound::Excluded(_)) => {
			range_sql_variant!(r#" AND "key" < ?2"#, r#" ORDER BY "key" DESC LIMIT ?3"#)
		}
		(Bound::Included(_), Bound::Unbounded) => {
			range_sql_variant!(r#" AND "key" >= ?2"#, r#" ORDER BY "key" DESC LIMIT ?3"#)
		}
		(Bound::Excluded(_), Bound::Unbounded) => {
			range_sql_variant!(r#" AND "key" > ?2"#, r#" ORDER BY "key" DESC LIMIT ?3"#)
		}
		(Bound::Included(_), Bound::Included(_)) => {
			range_sql_variant!(
				r#" AND "key" >= ?2"#,
				r#" AND "key" <= ?3"#,
				r#" ORDER BY "key" DESC LIMIT ?4"#
			)
		}
		(Bound::Included(_), Bound::Excluded(_)) => {
			range_sql_variant!(
				r#" AND "key" >= ?2"#,
				r#" AND "key" < ?3"#,
				r#" ORDER BY "key" DESC LIMIT ?4"#
			)
		}
		(Bound::Excluded(_), Bound::Included(_)) => {
			range_sql_variant!(
				r#" AND "key" > ?2"#,
				r#" AND "key" <= ?3"#,
				r#" ORDER BY "key" DESC LIMIT ?4"#
			)
		}
		(Bound::Excluded(_), Bound::Excluded(_)) => {
			range_sql_variant!(
				r#" AND "key" > ?2"#,
				r#" AND "key" < ?3"#,
				r#" ORDER BY "key" DESC LIMIT ?4"#
			)
		}
	}
}

pub(super) fn range_sql(start: Bound<&EncodedKey>, end: Bound<&EncodedKey>) -> &'static str {
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
