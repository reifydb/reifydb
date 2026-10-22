// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use napi::{Error as NapiError, Result as NapiResult};
use napi_derive::napi;
use reifydb::{WithSubsystem, embedded};
use reifydb_node::ReifydbNode;
use reifydb_place::{migration_path, routines};

#[napi]
pub fn create() -> NapiResult<ReifydbNode> {
	let db = embedded::memory()
		.with_flow(|flow| flow)
		.with_routines(routines)
		.with_migrations(migration_path())
		.build()
		.map_err(|e| NapiError::from_reason(format!("{e:?}")))?;
	ReifydbNode::from_database(db).map_err(|e| NapiError::from_reason(format!("{e:?}")))
}
