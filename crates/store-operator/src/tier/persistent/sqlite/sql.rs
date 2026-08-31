// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB





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

