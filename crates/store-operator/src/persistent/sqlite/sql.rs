// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(super) const CHECKPOINT_GET_SQL: &str = r#"SELECT "version", "lsn" FROM "flow_checkpoint" WHERE "flow" = ?1"#;

pub(super) const CHECKPOINT_SET_SQL: &str = r#"INSERT INTO "flow_checkpoint" ("flow", "version", "lsn") VALUES (?1, ?2, ?3)
	   ON CONFLICT ("flow") DO UPDATE SET "version" = excluded."version", "lsn" = excluded."lsn""#;

pub(super) const CHECKPOINT_REMOVE_SQL: &str = r#"DELETE FROM "flow_checkpoint" WHERE "flow" = ?1"#;

pub(super) const CHECKPOINT_FLOOR_SQL: &str = r#"SELECT MIN("version") FROM "flow_checkpoint""#;

pub(super) const CHECKPOINT_LSN_FLOOR_SQL: &str = r#"SELECT MIN("lsn") FROM "flow_checkpoint""#;

pub(super) const CHECKPOINT_LIST_SQL: &str = r#"SELECT "flow" FROM "flow_checkpoint" ORDER BY "flow" ASC"#;

pub(super) const CHECKPOINT_SCHEMA_SQL: &str = r#"CREATE TABLE IF NOT EXISTS "flow_checkpoint" (
	"flow" INTEGER NOT NULL PRIMARY KEY,
	"version" INTEGER NOT NULL,
	"lsn" INTEGER NOT NULL
) WITHOUT ROWID;"#;

pub(super) const SAVED_LSN_GET_SQL: &str = r#"SELECT "value" FROM "store_meta" WHERE "key" = 'saved_lsn'"#;

pub(super) const SAVED_LSN_SET_SQL: &str = r#"INSERT INTO "store_meta" ("key", "value") VALUES ('saved_lsn', ?1)
	   ON CONFLICT ("key") DO UPDATE SET "value" = excluded."value""#;

pub(super) const META_SCHEMA_SQL: &str = r#"CREATE TABLE IF NOT EXISTS "store_meta" (
	"key" TEXT NOT NULL PRIMARY KEY,
	"value" BLOB NOT NULL
) WITHOUT ROWID;"#;

pub(super) const TABLE_NAMES_SQL: &str = r#"SELECT "name" FROM "sqlite_master" WHERE "type" = 'table'"#;
