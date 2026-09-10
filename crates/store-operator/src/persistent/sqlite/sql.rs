// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

pub(super) const CHECKPOINT_GET_SQL: &str = r#"SELECT "version" FROM "flow_checkpoint" WHERE "flow" = ?1"#;

pub(super) const CHECKPOINT_SET_SQL: &str = r#"INSERT INTO "flow_checkpoint" ("flow", "version") VALUES (?1, ?2)
	   ON CONFLICT ("flow") DO UPDATE SET "version" = excluded."version""#;

pub(super) const CHECKPOINT_REMOVE_SQL: &str = r#"DELETE FROM "flow_checkpoint" WHERE "flow" = ?1"#;

pub(super) const CHECKPOINT_FLOOR_SQL: &str = r#"SELECT MIN("version") FROM "flow_checkpoint""#;

pub(super) const CHECKPOINT_LIST_SQL: &str = r#"SELECT "flow" FROM "flow_checkpoint" ORDER BY "flow" ASC"#;
